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

/**
 * CORS settings that browsers need to call Connect and gRPC-Web procedures
 * from another origin, for the HTTP framework's CORS support (Ktor's `CORS`
 * plugin, Spring's `CorsConfiguration`). Add any headers your application
 * sends or reads as well.
 *
 * The lists are those of https://connectrpc.com/docs/cors and connect-go's
 * `connectrpc.com/cors` package (v0.1.0 `cors.go`).
 */
object ConnectCors {
    /** Methods for `Access-Control-Allow-Methods`: GET for Connect, POST for all protocols. */
    val allowedMethods: List<String> = listOf("GET", "POST")

    /** Request headers for `Access-Control-Allow-Headers`. */
    val allowedHeaders: List<String> = listOf(
        // All protocols.
        "Content-Type",
        // Connect.
        "Connect-Protocol-Version",
        "Connect-Timeout-Ms",
        // gRPC-Web.
        "Grpc-Timeout",
        "X-Grpc-Web",
        // All protocols.
        "X-User-Agent",
    )

    /**
     * Response headers for `Access-Control-Expose-Headers`. A gRPC-Web
     * trailers-only response carries the status in these headers
     * (PROTOCOL-WEB.md, "Trailers-only responses").
     */
    val exposedHeaders: List<String> = listOf("Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin")
}
