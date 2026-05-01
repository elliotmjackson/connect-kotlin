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

package com.connectrpc.server.springboot

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.TimeUnit

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [TrailerTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class TrailerTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun unaryConnectTrailersGoToTrailerPrefixedHeaders() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/UnaryWithTrailers")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            // Connect unary surfaces ctx.responseTrailers as `trailer-<name>` HTTP headers.
            assertThat(response.header("trailer-x-custom-trailer")).isEqualTo("custom-value")
        }
    }

    @Test
    fun grpcMultiValueTrailersJoinedNotCollapsed() {
        // Servlet's setTrailerFields is single-value-per-name. We comma-join
        // multi-value entries; the gRPC reference client splits them. The
        // test client (OkHttp) sees the raw comma-joined value; assert that.
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/MultiValue")
            .header("Content-Type", "application/grpc+proto")
            .header("TE", "trailers")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            val trailerValue = response.trailers()["x-multi"]
                ?: response.header("x-multi")
            assertThat(trailerValue)
                .describedAs("multi-value trailer should be comma-joined, not collapsed to last")
                .contains("first")
                .contains("second")
        }
    }

    @Test
    fun grpcErrorBeforeAnyResponseSetsGrpcStatus() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/FailEarly")
            .header("Content-Type", "application/grpc+proto")
            .header("TE", "trailers")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            // Code 8 = RESOURCE_EXHAUSTED
            val status = response.trailers()["grpc-status"] ?: response.header("grpc-status")
            assertThat(status).isEqualTo("8")
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val unaryWithTrailers = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/UnaryWithTrailers",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                    ctx.responseTrailers.getOrPut("x-custom-trailer") { mutableListOf() }
                        .add("custom-value")
                    return request
                }
            }
            val multiValue = object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/MultiValue",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.SERVER,
                )

                override suspend fun handle(
                    request: TestMessage,
                    ctx: HandlerContext,
                    stream: ServerMessageStream<TestMessage>,
                ) {
                    ctx.responseTrailers["x-multi"] = mutableListOf("first", "second")
                    stream.send(TestMessage("ok"))
                }
            }
            val failEarly = object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/FailEarly",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.SERVER,
                )

                override suspend fun handle(
                    request: TestMessage,
                    ctx: HandlerContext,
                    stream: ServerMessageStream<TestMessage>,
                ) {
                    throw ConnectException(Code.RESOURCE_EXHAUSTED, "too big")
                }
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(unaryWithTrailers)
                .register(multiValue)
                .register(failEarly)
                .build()
        }
    }
}
