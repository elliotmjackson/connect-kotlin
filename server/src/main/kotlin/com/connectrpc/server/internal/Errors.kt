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
import com.connectrpc.server.encodeBinaryHeader
import okio.Buffer

/** HTTP status for a Connect unary error (protocol.md "Error Codes"). */
internal fun Code.connectHttpStatus(): Int = when (this) {
    Code.CANCELED -> 499
    Code.UNKNOWN -> 500
    Code.INVALID_ARGUMENT -> 400
    Code.DEADLINE_EXCEEDED -> 504
    Code.NOT_FOUND -> 404
    Code.ALREADY_EXISTS -> 409
    Code.PERMISSION_DENIED -> 403
    Code.RESOURCE_EXHAUSTED -> 429
    Code.FAILED_PRECONDITION -> 400
    Code.ABORTED -> 409
    Code.OUT_OF_RANGE -> 400
    Code.UNIMPLEMENTED -> 501
    Code.INTERNAL_ERROR -> 500
    Code.UNAVAILABLE -> 503
    Code.DATA_LOSS -> 500
    Code.UNAUTHENTICATED -> 401
}

/**
 * Connect error JSON (protocol.md "Error"): `code`, optional `message`,
 * optional `details`. A detail's `type` is the fully-qualified message name,
 * so any type-URL prefix is stripped through the last `/` (protocol.md
 * "Error Details"); `value` is unpadded standard base64. Written by hand
 * because `server` has no JSON or protobuf dependency of its own: `library`
 * keeps moshi as an `implementation` dependency and its error models internal.
 */
internal fun appendConnectErrorJson(sb: StringBuilder, error: ConnectException) {
    sb.append("{\"code\":\"").append(error.code.codeName).append('"')
    val message = error.message
    if (!message.isNullOrEmpty()) {
        sb.append(",\"message\":")
        appendJsonString(sb, message)
    }
    if (error.details.isNotEmpty()) {
        sb.append(",\"details\":[")
        for ((i, detail) in error.details.withIndex()) {
            if (i > 0) sb.append(',')
            sb.append("{\"type\":")
            appendJsonString(sb, detail.type.substringAfterLast('/'))
            sb.append(",\"value\":\"").append(encodeBinaryHeader(detail.payload.toByteArray())).append("\"}")
        }
        sb.append(']')
    }
    sb.append('}')
}

internal fun connectErrorJson(error: ConnectException): ByteArray = StringBuilder().also { appendConnectErrorJson(it, error) }.toString().toByteArray(Charsets.UTF_8)

/**
 * Connect EndStreamResponse JSON (protocol.md "Error"/"End-Of-Stream"):
 * `error` only on failure, `metadata` only when there are trailers.
 */
internal fun endStreamJson(error: ConnectException?, trailers: Map<String, List<String>>): Buffer {
    val sb = StringBuilder("{")
    if (error != null) {
        sb.append("\"error\":")
        appendConnectErrorJson(sb, error)
    }
    if (trailers.isNotEmpty()) {
        if (error != null) sb.append(',')
        sb.append("\"metadata\":{")
        var first = true
        for ((name, values) in trailers) {
            if (!first) sb.append(',')
            first = false
            appendJsonString(sb, name)
            sb.append(":[")
            for ((i, v) in values.withIndex()) {
                if (i > 0) sb.append(',')
                appendJsonString(sb, v)
            }
            sb.append(']')
        }
        sb.append('}')
    }
    sb.append('}')
    return Buffer().writeUtf8(sb.toString())
}

private fun appendJsonString(sb: StringBuilder, value: String) {
    sb.append('"')
    for (c in value) {
        when {
            c == '"' -> sb.append("\\\"")
            c == '\\' -> sb.append("\\\\")
            c == '\n' -> sb.append("\\n")
            c == '\r' -> sb.append("\\r")
            c == '\t' -> sb.append("\\t")
            c < ' ' -> sb.append("\\u").append(c.code.toString(16).padStart(4, '0'))
            else -> sb.append(c)
        }
    }
    sb.append('"')
}

/**
 * gRPC status fields for [error] (PROTOCOL-HTTP2.md "Responses"):
 * `grpc-status`, percent-encoded `grpc-message`, and for failures with details
 * `grpc-status-details-bin` (unpadded base64 `google.rpc.Status`).
 */
internal fun grpcStatusFields(error: ConnectException?, out: MetadataBuilder) {
    if (error == null) {
        out.set("grpc-status", "0")
        return
    }
    out.set("grpc-status", error.code.value.toString())
    val message = error.message
    if (!message.isNullOrEmpty()) out.set("grpc-message", grpcPercentEncode(message))
    if (error.details.isNotEmpty()) {
        out.set("grpc-status-details-bin", encodeBinaryHeader(encodeRpcStatus(error)))
    }
}

/**
 * Percent-encodes `grpc-message`: bytes of the UTF-8 form outside
 * `%x20-24 / %x26-7E` become `%XX` (PROTOCOL-HTTP2.md "Status-Message").
 */
internal fun grpcPercentEncode(value: String): String {
    val bytes = value.toByteArray(Charsets.UTF_8)
    val sb = StringBuilder(bytes.size)
    for (b in bytes) {
        val u = b.toInt() and 0xff
        if (u < 0x20 || u > 0x7e || u == '%'.code) {
            sb.append('%').append(HEX[u shr 4]).append(HEX[u and 0xf])
        } else {
            sb.append(u.toChar())
        }
    }
    return sb.toString()
}

private const val HEX = "0123456789ABCDEF"

/**
 * `google.rpc.Status { int32 code = 1; string message = 2; repeated Any details = 3; }`.
 * `Any.type_url` needs a `/`; bare message names get `type.googleapis.com/`
 * (protobuf `any.proto`).
 */
internal fun encodeRpcStatus(error: ConnectException): ByteArray {
    val out = Buffer()
    writeTag(out, 1, WIRE_VARINT)
    writeVarint(out, error.code.value.toLong())
    val message = error.message
    if (!message.isNullOrEmpty()) writeBytes(out, 2, message.toByteArray(Charsets.UTF_8))
    for (detail in error.details) {
        val typeUrl = if ('/' in detail.type) detail.type else "type.googleapis.com/${detail.type}"
        val any = Buffer()
        writeBytes(any, 1, typeUrl.toByteArray(Charsets.UTF_8))
        writeBytes(any, 2, detail.payload.toByteArray())
        writeBytes(out, 3, any.readByteArray())
    }
    return out.readByteArray()
}

private const val WIRE_VARINT = 0
private const val WIRE_LENGTH_DELIMITED = 2

private fun writeBytes(out: Buffer, field: Int, bytes: ByteArray) {
    writeTag(out, field, WIRE_LENGTH_DELIMITED)
    writeVarint(out, bytes.size.toLong())
    out.write(bytes)
}

private fun writeTag(out: Buffer, field: Int, wireType: Int) = writeVarint(out, ((field shl 3) or wireType).toLong())

private fun writeVarint(out: Buffer, value: Long) {
    var v = value
    while (v and 0x7fL.inv() != 0L) {
        out.writeByte(((v and 0x7f) or 0x80).toInt())
        v = v ushr 7
    }
    out.writeByte(v.toInt())
}
