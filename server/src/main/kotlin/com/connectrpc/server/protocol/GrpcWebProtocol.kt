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

import com.connectrpc.ConnectException
import okio.Buffer

/** Content-type for gRPC-Web binary protobuf payload. */
const val GRPC_WEB_CONTENT_TYPE_PROTO = "application/grpc-web+proto"

/** Content-type for gRPC-Web JSON payload. */
const val GRPC_WEB_CONTENT_TYPE_JSON = "application/grpc-web+json"

/** Bare gRPC-Web content-type alias for proto (per spec). */
const val GRPC_WEB_CONTENT_TYPE_BARE = "application/grpc-web"

/** Envelope flag bit indicating the gRPC-Web trailer envelope. */
const val ENVELOPE_FLAG_GRPC_WEB_TRAILER = 0x80

/** gRPC trailer key for status code (integer). */
const val GRPC_HEADER_STATUS = "grpc-status"

/** gRPC trailer key for the (percent-encoded) error message. */
const val GRPC_HEADER_MESSAGE = "grpc-message"

/** gRPC trailer key for base64 google.rpc.Status proto with error details. */
const val GRPC_HEADER_STATUS_DETAILS_BIN = "grpc-status-details-bin"

/**
 * Builds the body of a gRPC-Web trailer envelope: HTTP-headers-style
 * key:value\r\n pairs (lowercased keys) plus the standard grpc-* trailers.
 *
 * For success, [exception] should be null and the function emits
 * `grpc-status: 0` plus any user-set trailers. For error, emits
 * `grpc-status`, `grpc-message`, and (when there are details)
 * `grpc-status-details-bin`.
 */
fun grpcWebTrailerPayload(
    exception: ConnectException?,
    userTrailers: Map<String, List<String>>,
): ByteArray {
    val sb = StringBuilder()
    if (exception == null) {
        sb.append(GRPC_HEADER_STATUS).append(": 0\r\n")
    } else {
        sb.append(GRPC_HEADER_STATUS).append(": ").append(exception.code.value).append("\r\n")
        val message = exception.message
        if (!message.isNullOrEmpty()) {
            sb.append(GRPC_HEADER_MESSAGE).append(": ")
                .append(grpcPercentEncode(message))
                .append("\r\n")
        }
        if (exception.details.isNotEmpty()) {
            val statusBytes = encodeRpcStatusInternal(exception)
            // gRPC-Web spec: -bin headers are base64 without padding.
            val b64 = okio.ByteString.of(*statusBytes).base64().trimEnd('=')
            sb.append(GRPC_HEADER_STATUS_DETAILS_BIN).append(": ").append(b64).append("\r\n")
        }
    }
    for ((name, values) in userTrailers) {
        val lower = name.lowercase()
        for (v in values) sb.append(lower).append(": ").append(v).append("\r\n")
    }
    return sb.toString().toByteArray(Charsets.UTF_8)
}

/**
 * Percent-encodes the gRPC-message header value per the gRPC spec:
 * any byte outside 0x20..0x7e (or '%' itself) is encoded as %XX.
 * Non-ASCII characters are encoded byte-by-byte from their UTF-8 form.
 */
internal fun grpcPercentEncode(value: String): String {
    val bytes = value.toByteArray(Charsets.UTF_8)
    val sb = StringBuilder(bytes.size)
    for (b in bytes) {
        val u = b.toInt() and 0xff
        if (u < 0x20 || u > 0x7e || u == '%'.code) {
            sb.append('%')
            sb.append("%02X".format(u))
        } else {
            sb.append(u.toChar())
        }
    }
    return sb.toString()
}

/**
 * Encodes the [google.rpc.Status][https://cloud.google.com/apis/design/errors]
 * proto for the given exception:
 *   message Status { int32 code = 1; string message = 2; repeated Any details = 3; }
 * Hand-rolled to avoid pulling the proto runtime into :server.
 */
internal fun encodeRpcStatusInternal(exception: ConnectException): ByteArray {
    val out = Buffer()
    // field 1 (code), varint
    if (exception.code.value != 0) {
        writeProtoTag(out, 1, WIRE_TYPE_VARINT)
        writeVarint(out, exception.code.value.toLong())
    }
    // field 2 (message), length-delimited
    val msg = exception.message
    if (!msg.isNullOrEmpty()) {
        val msgBytes = msg.toByteArray(Charsets.UTF_8)
        writeProtoTag(out, 2, WIRE_TYPE_LENGTH_DELIMITED)
        writeVarint(out, msgBytes.size.toLong())
        out.write(msgBytes)
    }
    // field 3 (details, repeated), each is an embedded Any message
    for (detail in exception.details) {
        val anyBytes = encodeAny(detail.type, detail.payload.toByteArray())
        writeProtoTag(out, 3, WIRE_TYPE_LENGTH_DELIMITED)
        writeVarint(out, anyBytes.size.toLong())
        out.write(anyBytes)
    }
    return out.readByteArray()
}

private fun encodeAny(typeUrl: String, value: ByteArray): ByteArray {
    val out = Buffer()
    val typeBytes = typeUrl.toByteArray(Charsets.UTF_8)
    writeProtoTag(out, 1, WIRE_TYPE_LENGTH_DELIMITED)
    writeVarint(out, typeBytes.size.toLong())
    out.write(typeBytes)
    writeProtoTag(out, 2, WIRE_TYPE_LENGTH_DELIMITED)
    writeVarint(out, value.size.toLong())
    out.write(value)
    return out.readByteArray()
}

private const val WIRE_TYPE_VARINT = 0
private const val WIRE_TYPE_LENGTH_DELIMITED = 2

private fun writeProtoTag(out: Buffer, fieldNumber: Int, wireType: Int) {
    writeVarint(out, ((fieldNumber shl 3) or wireType).toLong())
}

private fun writeVarint(out: Buffer, value: Long) {
    var v = value
    while ((v and 0x7fL.inv()) != 0L) {
        out.writeByte(((v and 0x7fL) or 0x80L).toInt())
        v = v ushr 7
    }
    out.writeByte(v.toInt() and 0x7f)
}
