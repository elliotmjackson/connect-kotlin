// Copyright 2022-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.connectrpc.server.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.server.compression.ServerCompressionPool
import okio.Buffer

internal const val IDENTITY = "identity"

internal class Compressions(pools: List<ServerCompressionPool>) {
    private val byName = pools.associateBy { it.name() }

    /** Comma-separated names for `Accept-Encoding` / `Connect-Accept-Encoding` / `grpc-accept-encoding`. */
    val advertised: String? = byName.keys.joinToString(",").ifEmpty { null }

    /**
     * Picks the request and response compression (null = identity).
     *
     * An unknown request encoding is `unimplemented` with the supported list
     * (protocol.md "Content-Encoding"; grpc `doc/compression.md`).
     * When [accept] is present, the response takes its first supported
     * entry, an ordered list (protocol.md "Accept-Encoding"), or identity if
     * none is supported; that entry may be the request's encoding. Entries
     * with `q=0` are skipped: "not acceptable" (RFC 9110 §12.5.3). A server
     * asked to use an algorithm the client's last `grpc-accept-encoding`
     * excludes "SHALL send the message uncompressed" (grpc `doc/compression.md`
     * "Compression Method Asymmetry Between Peers"); when no listed coding is
     * available the origin server "SHOULD send a response without any content
     * coding" (RFC 9110 §12.5.3). This deliberately diverges from connect-go,
     * which reuses the request's compression whatever the accept list says
     * (`connecthttp/protocol.go:334-349`).
     * When [accept] is absent, the response reuses the request's compression:
     * servers "must assume that the client accepts the Content-Encoding used
     * for the request" (protocol.md "Accept-Encoding"), as connect-go does.
     */
    fun negotiate(sent: String?, accept: String?): Pair<ServerCompressionPool?, ServerCompressionPool?> {
        val sentName = sent?.trim()?.lowercase()
        val request = if (sentName.isNullOrEmpty() || sentName == IDENTITY) {
            null
        } else {
            byName[sentName] ?: throw ConnectException(
                Code.UNIMPLEMENTED,
                "unknown compression \"${sanitizeValue(sentName).take(64)}\": supported encodings are ${advertised ?: IDENTITY}",
            )
        }
        return request to if (accept == null) request else firstAccepted(accept)
    }

    private fun firstAccepted(accept: String): ServerCompressionPool? {
        for (entry in accept.split(',')) {
            val parts = entry.split(';')
            val name = parts[0].trim().lowercase()
            val compression = byName[name] ?: continue
            val zeroQ = parts.drop(1).any { param ->
                val (key, value) = param.split('=', limit = 2).let { it[0].trim() to it.getOrElse(1) { "" }.trim() }
                key.equals("q", ignoreCase = true) && value.toDoubleOrNull() == 0.0
            }
            if (!zeroQ) return compression
        }
        return null
    }
}

/**
 * Decompresses [compressed], failing with `resource_exhausted` as soon as the
 * output exceeds [readMaxBytes] (0 = unlimited) instead of inflating the
 * rest (connect-go `connecthttp/compression.go:39-65`).
 */
internal fun decompressBounded(compression: ServerCompressionPool, compressed: Buffer, readMaxBytes: Long): Buffer {
    val out = Buffer()
    try {
        compression.decompress(compressed).use { source ->
            while (true) {
                if (source.read(out, DECOMPRESS_CHUNK) == -1L) break
                if (readMaxBytes > 0 && out.size > readMaxBytes) {
                    throw ConnectException(Code.RESOURCE_EXHAUSTED, "message is larger than configured max $readMaxBytes")
                }
            }
        }
    } catch (e: ConnectException) {
        throw e
    } catch (e: Exception) {
        throw ConnectException(Code.INVALID_ARGUMENT, "could not decompress ${compression.name()} message", e)
    }
    return out
}

private const val DECOMPRESS_CHUNK = 8192L
