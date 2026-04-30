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
 * Per-request context exposed to a handler. Carries the request headers,
 * procedure metadata, and provides mutable maps for response headers and
 * trailers that the handler populates.
 *
 * For streaming handlers, response headers/trailers are accessed via the
 * [ServerStream] or [BidiStream] instead, because their visibility across
 * the wire is sequenced (headers before first message, trailers at close).
 */
class HandlerContext(
    val procedure: String,
    val requestHeaders: Headers,
    val httpMethod: String,
    val timeoutMs: Long?,
    val responseHeaders: MutableMap<String, MutableList<String>> = mutableMapOf(),
    val responseTrailers: MutableMap<String, MutableList<String>> = mutableMapOf(),
)
