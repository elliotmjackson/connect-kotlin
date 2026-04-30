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

package com.connectrpc.server.protocol

import com.connectrpc.Code
import com.connectrpc.ConnectException
import okio.Buffer

/** Content-type for Connect streaming, binary protobuf payload. */
const val CONNECT_STREAM_CONTENT_TYPE_PROTO = "application/connect+proto"

/** Content-type for Connect streaming, JSON payload. */
const val CONNECT_STREAM_CONTENT_TYPE_JSON = "application/connect+json"

/** Envelope flag indicating a compressed payload. */
const val ENVELOPE_FLAG_COMPRESSED = 0x01

/** Envelope flag indicating the EndStream envelope (last in a stream). */
const val ENVELOPE_FLAG_END_STREAM = 0x02

/**
 * A single Connect-protocol stream envelope: 1-byte flags + 4-byte length + payload.
 */
data class ConnectEnvelope(val flags: Int, val payload: ByteArray) {
    val isEndStream: Boolean get() = (flags and ENVELOPE_FLAG_END_STREAM) != 0
    val isCompressed: Boolean get() = (flags and ENVELOPE_FLAG_COMPRESSED) != 0

    override fun equals(other: Any?): Boolean =
        other is ConnectEnvelope && flags == other.flags && payload.contentEquals(other.payload)

    override fun hashCode(): Int = 31 * flags + payload.contentHashCode()
}

/** Encodes one envelope as bytes ready to write to the wire. */
fun encodeEnvelope(flags: Int, payload: ByteArray): ByteArray {
    val out = ByteArray(5 + payload.size)
    out[0] = flags.toByte()
    out[1] = (payload.size ushr 24).toByte()
    out[2] = (payload.size ushr 16).toByte()
    out[3] = (payload.size ushr 8).toByte()
    out[4] = payload.size.toByte()
    System.arraycopy(payload, 0, out, 5, payload.size)
    return out
}

/**
 * Reads one envelope from [buffer], advancing it. Returns null if the buffer
 * is exhausted before any header byte. Throws [ConnectException] if a partial
 * envelope is found (header read but payload truncated).
 */
fun decodeNextEnvelope(buffer: Buffer): ConnectEnvelope? {
    if (buffer.exhausted()) return null
    if (buffer.size < 5) {
        throw ConnectException(
            Code.INVALID_ARGUMENT,
            "envelope header truncated: expected 5 bytes, got ${buffer.size}",
        )
    }
    val flags = buffer.readByte().toInt() and 0xff
    val length = buffer.readInt().toLong() and 0xffffffffL
    if (buffer.size < length) {
        throw ConnectException(
            Code.INVALID_ARGUMENT,
            "envelope payload truncated: expected $length bytes, got ${buffer.size}",
        )
    }
    val payload = ByteArray(length.toInt())
    buffer.readFully(payload)
    return ConnectEnvelope(flags, payload)
}

/**
 * Builds the JSON payload for an EndStream envelope.
 *
 * The body is `{"error": {...}, "metadata": {...}}` where both fields are
 * optional. `metadata` is a JSON object with each key mapping to an array
 * of strings (multi-value headers).
 */
fun endStreamJsonPayload(
    exception: ConnectException?,
    trailers: Map<String, List<String>>,
): ByteArray {
    val sb = StringBuilder()
    sb.append('{')
    var first = true
    if (exception != null) {
        sb.append("\"error\":")
        sb.append(connectErrorJsonBody(exception).toString(Charsets.UTF_8))
        first = false
    }
    if (trailers.isNotEmpty()) {
        if (!first) sb.append(',')
        sb.append("\"metadata\":{")
        var firstKey = true
        for ((key, values) in trailers) {
            if (!firstKey) sb.append(',')
            firstKey = false
            appendJsonStringRaw(sb, key)
            sb.append(":[")
            for ((i, v) in values.withIndex()) {
                if (i > 0) sb.append(',')
                appendJsonStringRaw(sb, v)
            }
            sb.append(']')
        }
        sb.append('}')
    }
    sb.append('}')
    return sb.toString().toByteArray(Charsets.UTF_8)
}

private fun appendJsonStringRaw(sb: StringBuilder, value: String) {
    sb.append('"')
    for (c in value) {
        when {
            c == '"' -> sb.append("\\\"")
            c == '\\' -> sb.append("\\\\")
            c == '\b' -> sb.append("\\b")
            c == '\n' -> sb.append("\\n")
            c == '\r' -> sb.append("\\r")
            c == '\t' -> sb.append("\\t")
            c.code < 0x20 -> sb.append("\\u").append("%04x".format(c.code))
            else -> sb.append(c)
        }
    }
    sb.append('"')
}
