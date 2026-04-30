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

/** Content-type for Connect unary requests/responses, binary protobuf payload. */
const val CONNECT_UNARY_CONTENT_TYPE_PROTO = "application/proto"

/** Content-type for Connect unary requests/responses, JSON payload. */
const val CONNECT_UNARY_CONTENT_TYPE_JSON = "application/json"

/** Content-type for Connect error responses (always JSON, regardless of request codec). */
const val CONNECT_ERROR_CONTENT_TYPE = "application/json"

/**
 * Maps a Connect [Code] to the HTTP status code defined in the Connect protocol.
 * https://connectrpc.com/docs/protocol#error-codes
 */
fun Code.connectHttpStatus(): Int = when (this) {
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
 * Renders a [ConnectException] as a Connect-protocol error response body
 * (JSON object with `code`, optional `message`, and optional `details`).
 *
 * Per the Connect spec, detail `type` is the fully-qualified message name
 * without the `type.googleapis.com/` prefix; `value` is base64-encoded
 * protobuf bytes.
 */
fun connectErrorJsonBody(exception: ConnectException): ByteArray {
    val sb = StringBuilder()
    sb.append("{\"code\":\"").append(exception.code.codeName).append('"')
    val message = exception.message
    if (!message.isNullOrEmpty()) {
        sb.append(",\"message\":")
        appendJsonString(sb, message)
    }
    if (exception.details.isNotEmpty()) {
        sb.append(",\"details\":[")
        for ((i, detail) in exception.details.withIndex()) {
            if (i > 0) sb.append(',')
            sb.append("{\"type\":")
            appendJsonString(sb, detail.type.removePrefix("type.googleapis.com/"))
            // Connect protocol: detail value is unpadded base64.
            sb.append(",\"value\":\"")
                .append(detail.payload.base64().trimEnd('='))
                .append("\"}")
        }
        sb.append(']')
    }
    sb.append('}')
    return sb.toString().toByteArray(Charsets.UTF_8)
}

private fun appendJsonString(sb: StringBuilder, value: String) {
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
