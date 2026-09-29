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
import com.connectrpc.server.http.HttpExchange
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import okio.Buffer
import java.io.IOException

internal const val FLAG_COMPRESSED = 0x01
internal const val FLAG_CONNECT_END_STREAM = 0x02
internal const val FLAG_GRPC_WEB_TRAILER = 0x80

/**
 * Bounded reader over a request body. Holds at most one message plus one read
 * chunk; the transport is only asked for more when that is needed.
 */
internal class RequestReader(
    private val exchange: HttpExchange,
    private val readMaxBytes: Long,
) {
    private val buffer = Buffer()
    private var eof = false

    /**
     * True once a read failed because the transport did: the adapter threw
     * [IOException], which [HttpExchange.readRequestBody] reserves for a client
     * that has gone away, and the read failed with `canceled`.
     */
    var transportFailed = false
        private set

    /**
     * Reads a whole unary body. Stops at `readMaxBytes + 1` wire bytes with
     * `resource_exhausted` (connect-go `connecthttp/protocol_connect.go:1186-1206`).
     */
    suspend fun readAll(): Buffer {
        while (fill(CHUNK)) {
            if (readMaxBytes > 0 && buffer.size > readMaxBytes) {
                discard(DISCARD_LIMIT)
                throw tooLarge()
            }
        }
        return buffer
    }

    /** True if the body has at least one byte; used to reject GET requests with a body. */
    suspend fun hasBody(): Boolean = require(1)

    /**
     * Reads the next enveloped message (protocol.md "Streaming-Request";
     * PROTOCOL-HTTP2.md "Length-Prefixed-Message") and returns its decompressed
     * payload, or null at a clean end of body.
     *
     * The length prefix is checked against the limit before any payload is
     * read (connect-go `connecthttp/envelope.go:340-348`). Request envelopes may
     * only carry the compressed flag: end-stream and reserved bits are
     * `internal` errors (protocol.md "Streaming-Request": the end-stream bit
     * "MUST" be unset on requests, the other bits are reserved; connect-go
     * `connecthttp/protocol_connect.go:940-941`).
     */
    suspend fun nextEnvelope(compression: ServerCompressionPool?): Buffer? {
        if (!require(1)) return null
        if (!require(PREFIX)) throw ConnectException(Code.INVALID_ARGUMENT, "protocol error: incomplete envelope")
        val flags = buffer[0].toInt() and 0xff
        val size = (buffer.readInt(1)).toLong() and 0xffffffffL
        if (flags and FLAG_COMPRESSED.inv() != 0) {
            throw ConnectException(Code.INTERNAL_ERROR, "protocol error: invalid envelope flags $flags")
        }
        if (readMaxBytes > 0 && size > readMaxBytes) {
            discard(minOf(PREFIX + size, DISCARD_LIMIT))
            throw ConnectException(Code.RESOURCE_EXHAUSTED, "message size $size is larger than configured max $readMaxBytes")
        }
        if (!require(PREFIX + size)) {
            throw ConnectException(
                Code.INVALID_ARGUMENT,
                "protocol error: promised $size bytes in enveloped message, got ${buffer.size - PREFIX} bytes",
            )
        }
        buffer.skip(PREFIX)
        val payload = Buffer()
        payload.write(buffer, size)
        if (flags and FLAG_COMPRESSED == 0) return payload
        if (compression == null) {
            throw ConnectException(Code.INTERNAL_ERROR, "protocol error: sent compressed message without compression support")
        }
        // Zero-length content is never decompressed (protocol.md "Content-Encoding").
        return if (payload.size == 0L) payload else decompressBounded(compression, payload, readMaxBytes)
    }

    private suspend fun require(bytes: Long): Boolean {
        while (buffer.size < bytes) {
            if (!fill(bytes - buffer.size)) return false
        }
        return true
    }

    /**
     * Reads and drops up to [max] bytes so the client can finish sending and
     * read the error, as connect-go does before reporting an oversized message
     * (`connecthttp/envelope.go:340-348`, `protocol_connect.go:1199-1203`),
     * capped like its `discardLimit` (`connecthttp/protocol.go:38`).
     */
    private suspend fun discard(max: Long) {
        var dropped = buffer.size
        buffer.clear()
        try {
            while (dropped < max && fill(minOf(CHUNK, max - dropped))) {
                dropped += buffer.size
                buffer.clear()
            }
        } catch (e: ConnectException) {
            // The transport failed while draining; the size error is still the one to report.
        }
    }

    /** Appends at least one byte; false at end of body. */
    private suspend fun fill(wanted: Long): Boolean {
        if (eof) return false
        val max = maxOf(wanted, CHUNK).let { if (readMaxBytes > 0) minOf(it, readMaxBytes + PREFIX + CHUNK) else it }
        val read = try {
            exchange.readRequestBody(buffer, max)
        } catch (e: IOException) {
            currentCoroutineContext().ensureActive()
            transportFailed = true
            throw ConnectException(Code.CANCELED, "request body read failed", e)
        }
        if (read == -1L) {
            eof = true
            return false
        }
        return true
    }

    private fun tooLarge() = ConnectException(Code.RESOURCE_EXHAUSTED, "message is larger than configured max $readMaxBytes")

    private fun Buffer.readInt(offset: Long): Int = ((this[offset].toInt() and 0xff) shl 24) or
        ((this[offset + 1].toInt() and 0xff) shl 16) or
        ((this[offset + 2].toInt() and 0xff) shl 8) or
        (this[offset + 3].toInt() and 0xff)

    private companion object {
        const val PREFIX = 5L
        const val CHUNK = 16L * 1024
        const val DISCARD_LIMIT = 4L * 1024 * 1024
    }
}

/** Frames [payload] as one envelope: flags, 4-byte big-endian length, bytes (protocol.md "Enveloped-Message"). */
internal fun envelope(flags: Int, payload: Buffer): Buffer {
    val out = Buffer()
    out.writeByte(flags)
    out.writeInt(payload.size.toInt())
    out.writeAll(payload)
    return out
}
