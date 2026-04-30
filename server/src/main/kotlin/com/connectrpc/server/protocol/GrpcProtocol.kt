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

/** Bare gRPC content-type (proto by default). */
const val GRPC_CONTENT_TYPE_BARE = "application/grpc"

/** gRPC binary protobuf content-type. */
const val GRPC_CONTENT_TYPE_PROTO = "application/grpc+proto"

/** gRPC JSON content-type. */
const val GRPC_CONTENT_TYPE_JSON = "application/grpc+json"

/**
 * Builds the standard gRPC trailer set: grpc-status, optional grpc-message,
 * optional grpc-status-details-bin. Returned as a flat list of (name, value)
 * pairs ready to be assembled into HTTP/2 trailers.
 *
 * User-supplied trailers are appended verbatim (lowercased keys).
 */
fun grpcTrailerPairs(
    exception: ConnectException?,
    userTrailers: Map<String, List<String>>,
): List<Pair<String, String>> {
    val out = mutableListOf<Pair<String, String>>()
    if (exception == null) {
        out += GRPC_HEADER_STATUS to "0"
    } else {
        out += GRPC_HEADER_STATUS to exception.code.value.toString()
        val message = exception.message
        if (!message.isNullOrEmpty()) {
            out += GRPC_HEADER_MESSAGE to grpcPercentEncode(message)
        }
        if (exception.details.isNotEmpty()) {
            val statusBytes = encodeRpcStatusForGrpc(exception)
            // grpc spec: -bin headers are unpadded base64.
            out += GRPC_HEADER_STATUS_DETAILS_BIN to
                okio.ByteString.of(*statusBytes).base64().trimEnd('=')
        }
    }
    for ((name, values) in userTrailers) {
        val lower = name.lowercase()
        for (v in values) out += lower to v
    }
    return out
}

/**
 * Encodes a google.rpc.Status proto for the gRPC trailer details. Identical to
 * the gRPC-Web code path, just exposed under a name that doesn't read as
 * Web-specific.
 */
internal fun encodeRpcStatusForGrpc(exception: ConnectException): ByteArray =
    encodeRpcStatusInternal(exception)
