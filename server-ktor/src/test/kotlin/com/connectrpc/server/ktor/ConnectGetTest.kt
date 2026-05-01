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

import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.Base64

class ConnectGetTest {

    @Test
    fun connectGetRoundTripsIdempotentUnary() {
        val handler = idempotentUnary("test.v1.TestService/Idempotent") { req, _ -> req }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()
        TestServer.start(registry).use { server ->
            val url = "${server.baseUrl}/test.v1.TestService/Idempotent".toHttpUrl()
                .newBuilder()
                .addQueryParameter("connect", "v1")
                .addQueryParameter("encoding", "proto")
                .addQueryParameter("message", "hello")
                .build()
            val req = Request.Builder().url(url).get().build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
                assertThat(response.body!!.string()).isEqualTo("hello")
            }
        }
    }

    @Test
    fun connectGetWithBase64UrlSafeMessage() {
        val handler = idempotentUnary("test.v1.TestService/Idempotent") { req, _ -> req }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()
        TestServer.start(registry).use { server ->
            val raw = "binary bytes".toByteArray()
            val urlSafe = Base64.getUrlEncoder().withoutPadding().encodeToString(raw)
            val url = "${server.baseUrl}/test.v1.TestService/Idempotent".toHttpUrl()
                .newBuilder()
                .addQueryParameter("connect", "v1")
                .addQueryParameter("encoding", "proto")
                .addQueryParameter("base64", "1")
                .addQueryParameter("message", urlSafe)
                .build()
            val req = Request.Builder().url(url).get().build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
                assertThat(response.body!!.bytes()).isEqualTo(raw)
            }
        }
    }

    @Test
    fun nonIdempotentUnaryNotRoutedAsGet() {
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/SideEffects",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
                // default Idempotency.UNKNOWN
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = request
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()
        TestServer.start(registry).use { server ->
            val url = "${server.baseUrl}/test.v1.TestService/SideEffects".toHttpUrl()
                .newBuilder()
                .addQueryParameter("connect", "v1")
                .addQueryParameter("encoding", "proto")
                .addQueryParameter("message", "x")
                .build()
            val req = Request.Builder().url(url).get().build()
            newTestClient().newCall(req).execute().use { response ->
                // Ktor only registers the GET route for idempotent procedures,
                // so non-idempotent ones return 405 Method Not Allowed.
                assertThat(response.code).isIn(404, 405)
            }
        }
    }

    @Test
    fun missingConnectParamRejects() {
        val handler = idempotentUnary("test.v1.TestService/Idempotent") { req, _ -> req }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()
        TestServer.start(registry).use { server ->
            val url = "${server.baseUrl}/test.v1.TestService/Idempotent".toHttpUrl()
                .newBuilder()
                .addQueryParameter("encoding", "proto")
                .addQueryParameter("message", "hello")
                .build()
            val req = Request.Builder().url(url).get().build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isFalse()
                assertThat(response.body!!.string()).contains("invalid_argument")
            }
        }
    }
}

private fun idempotentUnary(
    path: String,
    body: suspend (TestMessage, HandlerContext) -> TestMessage,
): UnaryHandler<TestMessage, TestMessage> = object : UnaryHandler<TestMessage, TestMessage> {
    override val methodSpec = MethodSpec(
        path,
        TestMessage::class,
        TestMessage::class,
        StreamType.UNARY,
        idempotency = Idempotency.NO_SIDE_EFFECTS,
    )

    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
        body(request, ctx)
}
