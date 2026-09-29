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
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import com.connectrpc.server.encodeBinaryHeader
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import java.io.InputStream
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Behaviour owned by the servlet bridge rather than the protocol engine. */
class ServletBridgeTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java, "server.http2.enabled=true")

        @Volatile
        private var probe = Probe()

        @Volatile
        private var blockingRendezvous = CountDownLatch(0)
    }

    private class Probe {
        val sent = AtomicInteger()
        val firstSent = CompletableFuture<Unit>()
        val cancelled = CompletableFuture<Throwable>()
    }

    private val port: Int get() = server.port

    @Before
    fun resetProbe() {
        probe = Probe()
    }

    /** Tomcat sends trailers on HTTP/1.1 through chunked encoding, so gRPC works there too. */
    @Test
    fun grpcOverHttp1CarriesStatusInChunkedTrailers() {
        val client = OkHttpClient.Builder().protocols(listOf(Protocol.HTTP_1_1)).callTimeout(5, TimeUnit.SECONDS).build()
        client.newCall(grpcUnary()).execute().use { response ->
            assertThat(response.protocol).isEqualTo(Protocol.HTTP_1_1)
            assertThat(response.header("Transfer-Encoding")).isEqualTo("chunked")
            val message = readEnvelope(Buffer().write(response.body!!.bytes()))!!
            assertThat(String(message.payload)).isEqualTo("pong:ping")
            assertThat(response.trailers()["grpc-status"]).isEqualTo("0")
        }
    }

    @Test
    fun grpcOverH2cCarriesStatusInTrailers() {
        val client = OkHttpClient.Builder().protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)).callTimeout(5, TimeUnit.SECONDS).build()
        client.newCall(grpcUnary()).execute().use { response ->
            assertThat(response.protocol).isEqualTo(Protocol.H2_PRIOR_KNOWLEDGE)
            val message = readEnvelope(Buffer().write(response.body!!.bytes()))!!
            assertThat(String(message.payload)).isEqualTo("pong:ping")
            assertThat(response.trailers()["grpc-status"]).isEqualTo("0")
        }
    }

    /** A client that stops reading stalls the handler's sends instead of growing server memory. */
    @Test
    fun slowClientBackPressuresServerStream() {
        Socket("127.0.0.1", port).use { socket ->
            socket.getOutputStream().write(post("/test.v1.TestService/Flood", envelope(0, "65536".toByteArray())))
            probe.firstSent.get(5, TimeUnit.SECONDS)
            Thread.sleep(2_000)
            val stalledAt = probe.sent.get()
            Thread.sleep(1_000)
            // 64 KiB messages: socket buffers plus Tomcat's 8 KiB response buffer hold a few MiB at most.
            assertThat(stalledAt).isLessThan(128)
            assertThat(probe.sent.get()).isEqualTo(stalledAt)
        }
        assertThat(probe.cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
    }

    @Test
    fun http2StreamResetCancelsHandler() {
        val client = OkHttpClient.Builder().protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)).build()
        val call = client.newCall(
            Request.Builder()
                .url("http://127.0.0.1:$port/test.v1.TestService/Flood")
                .post(envelope(0, "0".toByteArray()).toRequestBody("application/connect+proto".toMediaType()))
                .build(),
        )
        call.execute().use { response ->
            assertThat(response.protocol).isEqualTo(Protocol.H2_PRIOR_KNOWLEDGE)
            probe.firstSent.get(5, TimeUnit.SECONDS)
            call.cancel()
        }
        assertThat(probe.cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
    }

    /**
     * Calls that end without reading their request body give its HTTP/2 flow-control
     * credit back. Tomcat's connection window is 65,535 bytes, so otherwise the fifth
     * 16,000-byte body could not be sent on the connection.
     */
    @Test
    fun unreadRequestBodiesDoNotExhaustHttp2ConnectionWindow() {
        val client = OkHttpClient.Builder().protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)).callTimeout(5, TimeUnit.SECONDS).build()
        val body = ByteArray(16_000).toRequestBody("application/proto".toMediaType())
        repeat(8) {
            // An unknown method is answered without reading the request.
            client.newCall(Request.Builder().url("http://127.0.0.1:$port/test.v1.TestService/Missing").post(body).build()).execute().use { response ->
                assertThat(response.protocol).isEqualTo(Protocol.H2_PRIOR_KNOWLEDGE)
                assertThat(response.code).isEqualTo(404)
            }
        }
    }

    /**
     * The Servlet API reports an HTTP/1.1 disconnect only when I/O fails, so a
     * handler that keeps sending is cancelled at its next write (Spring
     * Framework reference, "Asynchronous Requests" > "Disconnects").
     */
    @Test
    fun http1DisconnectCancelsHandlerAtNextWrite() {
        Socket("127.0.0.1", port).use { socket ->
            socket.getOutputStream().write(post("/test.v1.TestService/Flood", envelope(0, "tick".toByteArray())))
            probe.firstSent.get(5, TimeUnit.SECONDS)
        }
        assertThat(probe.cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
    }

    /** Open request bodies hold no threads: more stalled streams than any pool size still leave room. */
    @Test
    fun stalledClientStreamsDoNotBlockOtherCalls() {
        val stalled = (1..256).map {
            Socket("127.0.0.1", port).also { socket ->
                socket.getOutputStream().write(
                    (
                        "POST /test.v1.TestService/Sum HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/connect+proto\r\nTransfer-Encoding: chunked\r\n\r\n"
                        ).toByteArray(),
                )
                socket.getOutputStream().flush()
            }
        }
        try {
            val client = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()
            val unary = Request.Builder()
                .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
                .post("ping".toByteArray().toRequestBody("application/proto".toMediaType()))
                .build()
            client.newCall(unary).execute().use { response ->
                assertThat(response.code).isEqualTo(200)
                assertThat(response.body!!.string()).isEqualTo("pong:ping")
            }
        } finally {
            stalled.forEach(Socket::close)
        }
    }

    /** The deadline fires while the handler waits for a request message that never comes. */
    @Test
    fun bidiDeadlineFiresWhileWaitingForRequestBody() {
        Socket("127.0.0.1", port).use { socket ->
            socket.soTimeout = 5_000
            socket.getOutputStream().write(
                (
                    "POST /test.v1.TestService/Echo HTTP/1.1\r\nHost: localhost\r\n" +
                        "Content-Type: application/connect+proto\r\nConnect-Timeout-Ms: 500\r\n" +
                        "Transfer-Encoding: chunked\r\n\r\n"
                    ).toByteArray(),
            )
            socket.getOutputStream().flush()
            val started = System.nanoTime()
            val response = socket.getInputStream().readUntil("deadline_exceeded")
            val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            assertThat(response).startsWith("HTTP/1.1 200")
            assertThat(elapsedMs).isBetween(400L, 3_000L)
        }
    }

    /**
     * Handlers that block their thread must not stall each other: more of them than
     * there are CPU cores (the size of Dispatchers.Default) all get to run at once.
     */
    @Test
    fun blockingHandlersBeyondCoreCountRunConcurrently() {
        val calls = Runtime.getRuntime().availableProcessors() * 2 + 2
        blockingRendezvous = CountDownLatch(calls)
        val client = OkHttpClient.Builder()
            .dispatcher(
                okhttp3.Dispatcher().apply {
                    maxRequests = calls
                    maxRequestsPerHost = calls
                },
            )
            .callTimeout(10, TimeUnit.SECONDS)
            .build()
        val pool = Executors.newFixedThreadPool(calls)
        try {
            val results = (1..calls).map {
                pool.submit<String> {
                    client.newCall(
                        Request.Builder()
                            .url("http://127.0.0.1:$port/test.v1.TestService/Block")
                            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                            .build(),
                    ).execute().use { it.body!!.string() }
                }
            }
            assertThat(results.map { it.get(15, TimeUnit.SECONDS) }).containsOnly("all running")
        } finally {
            pool.shutdownNow()
        }
    }

    /** Tomcat lists a header name once per spelling; the handler must see each value once (RFC 9110 §5.1). */
    @Test
    fun headerSpelledTwoWaysKeepsEachValueOnce() {
        Socket("127.0.0.1", port).use { socket ->
            socket.soTimeout = 5_000
            socket.getOutputStream().write(
                (
                    "POST /test.v1.TestService/Headers HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/proto\r\n" +
                        "X-Foo: a\r\nx-foo: b\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    ).toByteArray(),
            )
            val response = socket.getInputStream().readBytes().toString(Charsets.ISO_8859_1)
            assertThat(response).startsWith("HTTP/1.1 200").endsWith("\r\n\r\n[a, b]")
        }
    }

    /** Repeated trailer values are joined with "," so gRPC's split on "," restores them exactly. */
    @Test
    fun repeatedGrpcTrailerValuesJoinWithBareComma() {
        val client = OkHttpClient.Builder().protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE)).callTimeout(5, TimeUnit.SECONDS).build()
        client.newCall(
            Request.Builder()
                .url("http://127.0.0.1:$port/test.v1.TestService/Trailers")
                .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc".toMediaType()))
                .build(),
        ).execute().use { response ->
            response.body!!.bytes()
            assertThat(response.trailers()["grpc-status"]).isEqualTo("0")
            assertThat(response.trailers()["x-multi-bin"]).isEqualTo("AQ,Ag")
        }
    }

    private fun grpcUnary() = Request.Builder()
        .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
        .post(envelope(0, "ping".toByteArray()).toRequestBody("application/grpc".toMediaType()))
        .build()

    private fun post(path: String, body: ByteArray): ByteArray = (
        "POST $path HTTP/1.1\r\nHost: localhost\r\n" +
            "Content-Type: application/connect+proto\r\nContent-Length: ${body.size}\r\n\r\n"
        ).toByteArray() +
        body

    /** Reads until [marker] appears; returns everything read, as Latin-1. */
    private fun InputStream.readUntil(marker: String): String {
        val out = StringBuilder()
        val chunk = ByteArray(4096)
        while (!out.contains(marker)) {
            val n = read(chunk)
            if (n < 0) break
            out.append(String(chunk, 0, n, Charsets.ISO_8859_1))
        }
        return out.toString()
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Unary", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = TestMessage("pong:${request.text()}")
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Block", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        val rendezvous = blockingRendezvous
                        rendezvous.countDown()
                        // Blocks the thread, as JDBC or a blocking HTTP client would.
                        return TestMessage(if (rendezvous.await(5, TimeUnit.SECONDS)) "all running" else "stalled")
                    }
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Headers", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = TestMessage(ctx.requestHeaders["x-foo"].toString())
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Trailers", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        ctx.responseTrailers["x-multi-bin"] = mutableListOf(encodeBinaryHeader(byteArrayOf(1)), encodeBinaryHeader(byteArrayOf(2)))
                        return TestMessage("ok")
                    }
                },
            )
            .register(
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Flood", TestMessage::class, TestMessage::class, StreamType.SERVER)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        val probe = probe
                        try {
                            // "tick": an empty message every 100 ms; otherwise messages of that many bytes, back to back.
                            val tick = request.text() == "tick"
                            val chunk = if (tick) ByteArray(0) else ByteArray(request.text().toInt())
                            while (true) {
                                stream.send(TestMessage(chunk))
                                probe.sent.incrementAndGet()
                                probe.firstSent.complete(Unit)
                                if (tick) {
                                    delay(100)
                                } else if (chunk.isEmpty()) {
                                    awaitCancellation()
                                }
                            }
                        } catch (e: CancellationException) {
                            probe.cancelled.complete(e)
                            throw e
                        }
                    }
                },
            )
            .register(
                object : ClientStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Sum", TestMessage::class, TestMessage::class, StreamType.CLIENT)
                    override suspend fun handle(stream: ClientMessageStream<TestMessage>, ctx: HandlerContext): TestMessage {
                        var count = 0
                        while (stream.receive() != null) count++
                        return TestMessage("$count")
                    }
                },
            )
            .register(
                object : BidiStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Echo", TestMessage::class, TestMessage::class, StreamType.BIDI)
                    override suspend fun handle(stream: BidiStream<TestMessage, TestMessage>, ctx: HandlerContext) {
                        while (true) stream.send(stream.receive() ?: return)
                    }
                },
            )
            .build()
    }
}
