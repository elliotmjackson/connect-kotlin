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

package com.connectrpc.server.ktor

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.ConnectCors
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import io.ktor.http.HttpMethod
import io.ktor.server.application.install
import io.ktor.server.plugins.cors.routing.CORS
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

/** The Ktor `CORS` plugin configured from [ConnectCors], as the docs show it. */
class CorsTest {
    private val origin = "https://app.example.com"

    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec("test.v1.TestService/Unary", TestMessage::class, TestMessage::class, StreamType.UNARY)
                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = throw ConnectException(Code.NOT_FOUND, "missing")
            },
        )
        .build()

    private fun server(block: (TestServer) -> Unit) = TestServer.start(registry) {
        install(CORS) {
            allowHost("app.example.com", schemes = listOf("https"))
            ConnectCors.allowedMethods.forEach { allowMethod(HttpMethod.parse(it)) }
            // Ktor's allowHeader("Content-Type") permits non-simple content types (CORSConfig.kt:289-293).
            ConnectCors.allowedHeaders.forEach(::allowHeader)
            ConnectCors.exposedHeaders.forEach(::exposeHeader)
        }
    }.use(block)

    @Test
    fun preflightAllowsTheHeadersEachProtocolSends() = server { server ->
        val client = newTestClient()
        val requests = mapOf(
            "Connect POST" to ("POST" to "content-type,connect-protocol-version,connect-timeout-ms"),
            "Connect GET" to ("GET" to "connect-protocol-version"),
            "gRPC-Web" to ("POST" to "content-type,x-grpc-web,x-user-agent,grpc-timeout"),
        )
        for ((name, request) in requests) {
            val (method, headers) = request
            val preflight = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .method("OPTIONS", null)
                .header("Origin", origin)
                .header("Access-Control-Request-Method", method)
                .header("Access-Control-Request-Headers", headers)
                .build()
            client.newCall(preflight).execute().use { response ->
                assertThat(response.code).`as`(name).isEqualTo(200)
                assertThat(response.header("Access-Control-Allow-Origin")).`as`(name).isEqualTo(origin)
            }
        }
    }

    @Test
    fun grpcWebResponseExposesTheStatusHeaders() = server { server ->
        val request = Request.Builder()
            .url("${server.baseUrl}/test.v1.TestService/Unary")
            .header("Origin", origin)
            .post(envelope(0, "x".toByteArray()).toRequestBody("application/grpc-web+proto".toMediaType()))
            .build()
        newTestClient().newCall(request).execute().use { response ->
            assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(origin)
            val exposed = response.header("Access-Control-Expose-Headers").orEmpty().split(",").map { it.trim().lowercase() }
            assertThat(exposed).contains("grpc-status", "grpc-message", "grpc-status-details-bin")
        }
    }
}
