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
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Test
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.net.SocketFactory

/**
 * Stopping the engine runs ConnectServer.shutdown before the engine closes
 * connections: calls in flight get the grace period, then end with
 * `unavailable` in their protocol's error shape, and calls that start during
 * the grace period fail with `unavailable`.
 */
class GracefulShutdownTest {
    private val entered = CountDownLatch(1)

    private val registry = HandlerRegistry.builder()
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
        .register(
            object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec("test.v1.TestService/Big", TestMessage::class, TestMessage::class, StreamType.SERVER)

                override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                    // Fits in Ktor's 1 MiB body channel (ktor-io 3.6.0
                    // `ByteChannel.kt:22, 51`), so send returns while most of
                    // it waits for a client that is not reading yet.
                    stream.send(TestMessage(ByteArray(BIG_MESSAGE_SIZE)))
                    entered.countDown()
                    awaitCancellation()
                }
            },
        )
        .build()

    private val server = TestServer.start(registry, withH2c = true)
    private var stopped: CompletableFuture<Void>? = null

    @After
    fun tearDown() {
        (stopped ?: CompletableFuture.runAsync(server::close)).get(15, TimeUnit.SECONDS)
    }

    @Test
    fun unaryInFlightCompletes() {
        val call = callAsync(newTestClient(), connectUnary("Slow"))
        stopWhenEntered()

        val response = call.get(10, TimeUnit.SECONDS)
        assertThat(response.status).isEqualTo(200)
        assertThat(String(response.body)).isEqualTo("done")
    }

    @Test
    fun connectStreamEndsWithUnavailable() {
        val call = callAsync(newTestClient(), enveloped("application/connect+proto"))
        stopWhenEntered()

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
        val request = enveloped("application/grpc+proto").newBuilder().header("TE", "trailers").build()
        val call = callAsync(newTestClient(h2cPriorKnowledge = true), request)
        stopWhenEntered()

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
        val call = callAsync(newTestClient(), enveloped("application/grpc-web+proto"))
        stopWhenEntered()

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
        val client = newTestClient()
        val stream = callAsync(client, enveloped("application/connect+proto"))
        stopWhenEntered()

        // The stream keeps the server in its grace period, so the connectors
        // are still open and only the Connect server refuses calls.
        var response = client.call(connectUnary("Ping"))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
        while (response.status == 200 && System.nanoTime() < deadline) response = client.call(connectUnary("Ping"))
        assertThat(response.status).isEqualTo(503)
        assertThat(String(response.body)).isEqualTo("""{"code":"unavailable","message":"server is shutting down"}""")
        assertThat(stream.get(10, TimeUnit.SECONDS).status).isEqualTo(200)
    }

    /**
     * A client reading slowly still gets all of the response: stop waits for
     * it to be written after the grace period, rather than letting the engine
     * close the connection with the message still in Ktor's body channel. The
     * client's small receive buffer keeps most of the message on the server.
     */
    @Test
    fun slowReaderGetsTheWholeStream() {
        val client = newTestClient().newBuilder().socketFactory(SmallReceiveBufferSocketFactory).build()
        val request = enveloped("application/connect+proto").newBuilder().url("${server.baseUrl}/test.v1.TestService/Big").build()
        val call = CompletableFuture.supplyAsync {
            client.newCall(request).execute().use { response ->
                Thread.sleep(1_500)
                response.body!!.bytes()
            }
        }
        stopWhenEntered()

        val body = Buffer().write(call.get(15, TimeUnit.SECONDS))
        assertThat(readEnvelope(body)?.payload?.size).isEqualTo(BIG_MESSAGE_SIZE)
        val end = checkNotNull(readEnvelope(body))
        assertThat(String(end.payload)).isEqualTo("""{"error":{"code":"unavailable","message":"server is shutting down"}}""")
    }

    /** Waits until the handler has started, then stops the server on another thread; stop blocks for the grace period. */
    private fun stopWhenEntered() {
        assertThat(entered.await(5, TimeUnit.SECONDS)).describedAs("handler started").isTrue()
        stopped = CompletableFuture.runAsync(server::stopGracefully)
    }

    private fun connectUnary(procedure: String): Request = Request.Builder()
        .url("${server.baseUrl}/test.v1.TestService/$procedure")
        .post("a".toByteArray().toRequestBody("application/proto".toMediaType()))
        .build()

    private fun enveloped(contentType: String): Request = Request.Builder()
        .url("${server.baseUrl}/test.v1.TestService/Stream")
        .post(envelope(0, "a".toByteArray()).toRequestBody(contentType.toMediaType()))
        .build()

    private class Response(val status: Int, val body: ByteArray, val trailers: Map<String, List<String>>)

    private fun OkHttpClient.call(request: Request): Response = newCall(request).execute().use { response ->
        val body = response.body!!.bytes()
        Response(response.code, body, response.trailers().toMultimap())
    }

    private fun callAsync(client: OkHttpClient, request: Request): CompletableFuture<Response> = CompletableFuture.supplyAsync { client.call(request) }
}

private const val BIG_MESSAGE_SIZE = 768 * 1024

private object SmallReceiveBufferSocketFactory : SocketFactory() {
    private fun configure(socket: Socket) = socket.apply { receiveBufferSize = 16 * 1024 }

    override fun createSocket(): Socket = configure(Socket())

    override fun createSocket(host: String, port: Int): Socket = configure(Socket()).apply { connect(InetSocketAddress(host, port)) }

    override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int): Socket = throw UnsupportedOperationException()

    override fun createSocket(host: InetAddress, port: Int): Socket = configure(Socket()).apply { connect(InetSocketAddress(host, port)) }

    override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket = throw UnsupportedOperationException()
}
