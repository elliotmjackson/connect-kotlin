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

package com.connectrpc.server

import com.connectrpc.Headers

/**
 * Per-request context exposed to a handler. Carries the inbound request
 * metadata and provides mutable maps for response headers and trailers
 * that the handler populates.
 *
 * Headers vs. trailers — when the framework commits each:
 * - Unary: response headers and trailers go into HTTP headers (trailers
 *   prefixed with `trailer-`), committed when the handler returns.
 * - Server-stream / bidi: response headers go into HTTP headers committed
 *   either on first [ServerMessageStream.send] or when the handler completes
 *   without sending — populate them before either point. Response trailers
 *   are written into the trailing envelope (Connect / gRPC-Web) or HTTP/2
 *   trailers (gRPC) at the end of the response, so they may be populated
 *   any time before the handler returns.
 */
class HandlerContext(
    /** Fully-qualified procedure path, e.g. `connectrpc.eliza.v1.ElizaService/Say`. */
    val procedure: String,
    /** Headers received with the request. Lookups are case-insensitive per HTTP convention. */
    val requestHeaders: Headers,
    /** HTTP method used by the client — typically `POST`, but `GET` for Connect-GET. */
    val httpMethod: String,
    /**
     * Effective per-request deadline in milliseconds, parsed from
     * `Connect-Timeout-Ms` (Connect) or `Grpc-Timeout` (gRPC / gRPC-Web).
     * Null if no deadline was advertised. The framework enforces this
     * automatically via `withTimeout` around the handler — the field is
     * exposed here for handlers that want to budget their own work.
     */
    val timeoutMs: Long?,
    /**
     * Query parameters from the request URL. Populated for Connect-GET requests
     * (idempotent unary calls); null otherwise.
     */
    val queryParams: Map<String, List<String>>? = null,
    /**
     * Response headers to send to the client. Mutate before the response is
     * committed — see the class-level KDoc for when each protocol commits.
     */
    val responseHeaders: MutableMap<String, MutableList<String>> = mutableMapOf(),
    /**
     * Response trailers to send to the client. Mutate any time before the
     * handler returns — they are emitted at end-of-response.
     */
    val responseTrailers: MutableMap<String, MutableList<String>> = mutableMapOf(),
)
