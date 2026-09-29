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

import com.connectrpc.Code
import com.connectrpc.Headers

/**
 * Observes every request a [ConnectServer] serves, including those that fail
 * before a handler runs (unknown path, unsupported content type or
 * compression, undecodable message), which interceptors never see. Set it
 * with [ServerConfig.observer].
 *
 * This is enough for OpenTelemetry's RPC conventions as connect-go's
 * otelconnect applies them: `rpc.system` from the protocol (`grpc` for gRPC
 * and gRPC-Web, `connect_rpc` for Connect, `interceptor.go:363-376`),
 * `rpc.service` and `rpc.method` from the procedure, and the status code
 * (`attributes.go:50-110`). Requests are served concurrently, so
 * implementations must be thread-safe.
 */
fun interface ServerObserver {
    /**
     * Called for each request once its path, method and content type have
     * been checked, before its body is read.
     *
     * @param procedure The procedure requested, e.g.
     *     `connectrpc.eliza.v1.ElizaService/Say`, or null when the path names
     *     none.
     * @param protocol `connect`, `grpc` or `grpcweb`, as connect-go names them
     *     (v1.20.0 `protocol.go:32-34`), or null when the server answered with
     *     an HTTP status alone before choosing one: 404 for an unknown path,
     *     405 for a method not allowed, 415 for an unsupported content type or
     *     GET encoding.
     * @param requestHeaders The request headers, with lower-case names.
     * @return The observer of this request's end.
     */
    fun onStart(procedure: String?, protocol: String?, requestHeaders: Headers): Call

    /** Observes the end of one request. */
    fun interface Call {
        /**
         * Called once, when the response has been handed to the transport or
         * the transport failed. Adapters may still be writing the bytes: Ktor
         * on Netty queues a unary body before it reaches the socket.
         *
         * @param code The code sent to the client, or null on success. For an
         *     HTTP status alone it is the code clients infer from the status
         *     (protocol.md "HTTP to Error Code"): `unimplemented` for 404,
         *     `unknown` for 405 and 415. A transport failure or a client that
         *     went away before the response was handed to the transport is
         *     `canceled`, even when the bytes already written held the whole
         *     response, which the client may then have read.
         */
        fun onEnd(code: Code?)
    }
}
