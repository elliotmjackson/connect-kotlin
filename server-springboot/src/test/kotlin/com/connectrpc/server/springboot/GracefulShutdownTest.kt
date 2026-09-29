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

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Closing the application context shuts the ConnectServer down before
 * Tomcat's graceful shutdown: calls in flight get
 * `connectrpc.shutdown-grace-period`, then end with `unavailable` in their
 * protocol's error shape, and calls that start meanwhile fail with
 * `unavailable`.
 */
class GracefulShutdownTest {
    private val context = SpringApplicationBuilder(TestApp::class.java)
        .web(WebApplicationType.SERVLET)
        .properties("server.port=0", "server.address=127.0.0.1", "server.http2.enabled=true", "connectrpc.shutdown-grace-period=1s")
        .run()
    private val baseUrl = "http://127.0.0.1:${checkNotNull((context as WebServerApplicationContext).webServer).port}/test.v1.TestService"
    private var closed: CompletableFuture<Void>? = null

    init {
        entered = CountDownLatch(1)
    }

    @After
    fun tearDown() {
        (closed ?: CompletableFuture.runAsync(context::close)).get(15, TimeUnit.SECONDS)
    }

    @Test
    fun unaryInFlightCompletes() {
        val call = callAsync(http1, connectUnary("Slow"))
        closeWhenEntered()

        val response = call.get(10, TimeUnit.SECONDS)
        assertThat(response.status).isEqualTo(200)
        assertThat(String(response.body)).isEqualTo("done")
    }

    @Test
    fun connectStreamEndsWithUnavailable() {
        val call = callAsync(http1, enveloped("application/connect+proto"))
        closeWhenEntered()

        val response = call.get(10, TimeUnit.SECONDS)
        assertThat(response.status).isEqualTo(200)
        val body = Buffer().write(response.body)
        assertThat(readEnvelope(body)?.payload?.let(::String)).isEqualTo("first")
        val end = checkNotNull(readEnvelope(body))
        assertThat(end.flags).isEqualTo(0x02)
        assertThat(String(end.payload)).isEqualTo("""{"error":{"code":"unavailable","message":"server is shutting down"}}""")
        assertThat(body.exhausted()).isTrue()
    }

    @Test
    fun grpcStreamEndsWithUnavailableTrailers() {
        val client = http1.newBuilder().protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)).build()
        val call = callAsync(client, enveloped("application/grpc+proto").newBuilder().header("TE", "trailers").build())
        closeWhenEntered()

        val response = call.get(10, TimeUnit.SECONDS)
        assertThat(response.status).isEqualTo(200)
        val body = Buffer().write(response.body)
        assertThat(readEnvelope(body)?.payload?.let(::String)).isEqualTo("first")
        assertThat(body.exhausted()).isTrue()
        assertThat(response.trailers).containsEntry("grpc-status", listOf("14"))
        assertThat(response.trailers).containsEntry("grpc-message", listOf("server is shutting down"))
    }

    @Test
    fun grpcWebStreamEndsWithUnavailableTrailers() {
        val call = callAsync(http1, enveloped("application/grpc-web+proto"))
        closeWhenEntered()

        val response = call.get(10, TimeUnit.SECONDS)
        assertThat(response.status).isEqualTo(200)
        val body = Buffer().write(response.body)
        assertThat(readEnvelope(body)?.payload?.let(::String)).isEqualTo("first")
        val trailers = checkNotNull(readEnvelope(body))
        assertThat(trailers.flags).isEqualTo(0x80)
        assertThat(String(trailers.payload).split("\r\n").filter { it.isNotEmpty() })
            .containsExactlyInAnyOrder("grpc-status: 14", "grpc-message: server is shutting down")
        assertThat(body.exhausted()).isTrue()
    }

    @Test
    fun callsStartingDuringTheGracePeriodFailWithUnavailable() {
        val stream = callAsync(http1, enveloped("application/connect+proto"))
        closeWhenEntered()

        // The stream holds the context in the grace period, before Tomcat's
        // graceful shutdown closes the connectors.
        var response = http1.call(connectUnary("Ping"))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
        while (response.status == 200 && System.nanoTime() < deadline) response = http1.call(connectUnary("Ping"))
        assertThat(response.status).isEqualTo(503)
        assertThat(String(response.body)).isEqualTo("""{"code":"unavailable","message":"server is shutting down"}""")
        assertThat(stream.get(10, TimeUnit.SECONDS).status).isEqualTo(200)
    }

    /** Waits until the handler has started, then closes the context on another thread; close blocks for the grace period. */
    private fun closeWhenEntered() {
        assertThat(entered.await(5, TimeUnit.SECONDS)).describedAs("handler started").isTrue()
        closed = CompletableFuture.runAsync(context::close)
    }

    private fun connectUnary(procedure: String): Request = Request.Builder()
        .url("$baseUrl/$procedure")
        .post("a".toByteArray().toRequestBody("application/proto".toMediaType()))
        .build()

    private fun enveloped(contentType: String): Request = Request.Builder()
        .url("$baseUrl/Stream")
        .post(envelope(0, "a".toByteArray()).toRequestBody(contentType.toMediaType()))
        .build()

    private class Response(val status: Int, val body: ByteArray, val trailers: Map<String, List<String>>)

    private val http1 = OkHttpClient.Builder().callTimeout(10, TimeUnit.SECONDS).build()

    private fun OkHttpClient.call(request: Request): Response = newCall(request).execute().use { response ->
        val body = response.body!!.bytes()
        Response(response.code, body, response.trailers().toMultimap())
    }

    private fun callAsync(client: OkHttpClient, request: Request): CompletableFuture<Response> = CompletableFuture.supplyAsync { client.call(request) }

    companion object {
        /** Counted down once the handler of the call under test is running. */
        @Volatile
        private var entered = CountDownLatch(1)
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Slow", TestMessage::class, TestMessage::class, StreamType.UNARY)

                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        entered.countDown()
                        delay(300)
                        return TestMessage("done")
                    }
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Ping", TestMessage::class, TestMessage::class, StreamType.UNARY)

                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = TestMessage("pong")
                },
            )
            .register(
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Stream", TestMessage::class, TestMessage::class, StreamType.SERVER)

                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        stream.send(TestMessage("first"))
                        entered.countDown()
                        awaitCancellation()
                    }
                },
            )
            .build()
    }
}
