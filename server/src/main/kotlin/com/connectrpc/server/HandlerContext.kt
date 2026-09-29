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
import com.connectrpc.server.internal.CaseInsensitiveHeaders
import kotlin.time.Duration
import kotlin.time.TimeMark

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
 *   are written into the trailing envelope (Connect / gRPC-Web) or HTTP
 *   trailers (gRPC) at the end of the response, so they may be populated
 *   any time before the handler returns.
 *
 * Names reserved by the protocols (`content-type`, `content-encoding`,
 * `accept-encoding`, `connect-*`, `grpc-*`, ...) are dropped from response
 * headers and trailers. Values have CR, LF, NUL and other control
 * characters replaced by spaces (RFC 9110 §5.5); names that are not RFC 9110
 * tokens are dropped. Binary values go in `-bin` names, encoded with
 * [encodeBinaryHeader]; `-bin` request headers arrive still base64-encoded and
 * are read with [decodeBinaryHeaders].
 */
class HandlerContext(
    /** Fully-qualified procedure path, e.g. `connectrpc.eliza.v1.ElizaService/Say`. */
    val procedure: String,
    requestHeaders: Headers,
    /** HTTP method used by the client — typically `POST`, but `GET` for Connect-GET. */
    val httpMethod: String,
    /**
     * When the call times out, or null if it has no deadline. Set when the
     * call starts from the timeout the client sent (`Connect-Timeout-Ms`,
     * `grpc-timeout`), limited by [ServerConfig.maxTimeout]. The server
     * enforces it over request reads and the handler: the handler is
     * cancelled and the call fails with `deadline_exceeded` when it passes.
     * A handler blocked in code that does not suspend is not interrupted;
     * the call fails when it returns, discarding its result.
     * Response writes are bounded by the HTTP server, not by the deadline
     * (see [ServerConfig.maxTimeout]).
     */
    val deadline: TimeMark?,
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
) {
    /**
     * Headers received with the request, with lower-case names. Lookups
     * ignore case (RFC 9110 §5.1); repeated fields keep every value.
     */
    val requestHeaders: Headers = CaseInsensitiveHeaders.of(requestHeaders)

    /**
     * Returns the time left until [deadline], negative once it has passed, or
     * null if the call has no deadline; like connect-es `timeoutMs()`
     * (`implementation.ts:161-167`). Pass it on as the timeout of calls the
     * handler makes to other services.
     */
    fun timeRemaining(): Duration? = deadline?.let { -it.elapsedNow() }
}
