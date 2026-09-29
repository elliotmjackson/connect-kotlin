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
import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * How the servlet adapter maps a Tomcat request onto `HttpExchange` and the
 * exchange's responses back onto HTTP/1.x, per the contract in
 * `server/src/main/kotlin/com/connectrpc/server/http/HttpExchange.kt`.
 * Services are mounted under a context path and a path prefix, which the
 * exchange's path excludes.
 */
class RequestResponseMappingTest {
    companion object {
        @Volatile
        private var countOutcome = CompletableFuture<String>()

        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java, "server.servlet.context-path=/api", "connectrpc.path-prefix=/rpc")

        fun registry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(ECHO, TestMessage::class, TestMessage::class, StreamType.UNARY, Idempotency.NO_SIDE_EFFECTS)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        ctx.responseHeaders["x-multi"] = mutableListOf("a", "b")
                        return request
                    }
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(HEADERS, TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = TestMessage(ctx.requestHeaders["x-foo"].orEmpty().joinToString("|"))
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(FAIL, TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = throw ConnectException(Code.NOT_FOUND, message = "no such thing")
                },
            )
            .register(
                object : ClientStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(COUNT, TestMessage::class, TestMessage::class, StreamType.CLIENT)
                    override suspend fun handle(stream: ClientMessageStream<TestMessage>, ctx: HandlerContext): TestMessage {
                        val outcome = countOutcome
                        var count = 0
                        try {
                            while (stream.receive() != null) count++
                        } catch (e: Throwable) {
                            outcome.complete("failed after $count")
                            throw e
                        }
                        outcome.complete("completed after $count")
                        return TestMessage("$count")
                    }
                },
            )
            .register(
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(STREAM, TestMessage::class, TestMessage::class, StreamType.SERVER)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        stream.send(request)
                    }
                },
            )
            .build()

        private const val BASE = "/api/rpc"
        private const val ECHO = "test.v1.TestService/Echo"
        private const val HEADERS = "test.v1.TestService/Headers"
        private const val FAIL = "test.v1.TestService/Fail"
        private const val COUNT = "test.v1.TestService/Count"
        private const val STREAM = "test.v1.TestService/Stream"
    }

    /**
     * The query reaches the engine still percent-encoded, so escapes decode
     * exactly once and `%2B` stays distinct from `+`, which form-decodes to a
     * space (protocol.md "Unary-Get-Request"; Go `url.ParseQuery`).
     */
    @Test
    fun connectGetQueryIsDecodedExactlyOnce() {
        val cases = mapOf(
            "a%2Bb" to "a+b",
            "a+b" to "a b",
            "a%26b%3Dc" to "a&b=c",
            "%252B" to "%2B",
            "caf%C3%A9" to "café",
        )
        for ((encoded, decoded) in cases) {
            val response = rawExchange(server.port, "GET $BASE/$ECHO?encoding=proto&message=$encoded HTTP/1.1")
            assertThat(response.status).describedAs(encoded).isEqualTo(200)
            assertThat(String(response.body, Charsets.UTF_8)).describedAs(encoded).isEqualTo(decoded)
        }
    }

    /** Header names are matched without regard to case and every field line's value is kept (RFC 9110 §5.1, §5.3). */
    @Test
    fun requestHeaderFieldsKeepEveryValue() {
        val cases = listOf(
            listOf("X-Foo: a") to "a",
            listOf("X-FOO: a") to "a",
            listOf("X-Foo: a", "X-Foo: b") to "a|b",
            listOf("X-Foo: a, b") to "a, b",
        )
        for ((lines, expected) in cases) {
            val response = rawExchange(server.port, "POST $BASE/$HEADERS HTTP/1.1", listOf("Content-Type: application/proto") + lines, ByteArray(0))
            assertThat(response.status).describedAs(lines.toString()).isEqualTo(200)
            assertThat(String(response.body)).describedAs(lines.toString()).isEqualTo(expected)
        }
    }

    /** The body reaches the engine whole whatever its size and framing; -1 comes only at its end. */
    @Test
    fun requestBodyIsReadWholeForAnyFraming() {
        val large = Random(1).nextBytes(1024 * 1024 + 7)
        val cases = listOf(
            ByteArray(0) to Framing.Length,
            ByteArray(0) to Framing.Chunked(1),
            "x".toByteArray() to Framing.Length,
            "hello".toByteArray() to Framing.Chunked(1),
            large to Framing.Length,
            large to Framing.Chunked(1000),
        )
        for ((body, framing) in cases) {
            val label = "${body.size} bytes, $framing"
            val response = rawExchange(server.port, "POST $BASE/$ECHO HTTP/1.1", listOf("Content-Type: application/proto"), body, framing)
            assertThat(response.status).describedAs(label).isEqualTo(200)
            assertThat(response.body).describedAs(label).isEqualTo(body)
        }
    }

    /** Envelope prefixes split across reads still frame the stream. */
    @Test
    fun clientStreamEnvelopesSplitAcrossChunks() {
        countOutcome = CompletableFuture()
        val body = envelope(0, "one".toByteArray()) + envelope(0, ByteArray(0)) + envelope(0, "three".toByteArray())
        val response = rawExchange(server.port, "POST $BASE/$COUNT HTTP/1.1", listOf("Content-Type: application/connect+proto"), body, Framing.Chunked(1))
        assertThat(response.status).isEqualTo(200)
        assertThat(String(readEnvelope(Buffer().write(response.body))!!.payload)).isEqualTo("3")
    }

    /**
     * A request body cut off by a closed connection must not read as the end of
     * the stream: the handler would see a shorter stream as complete.
     */
    @Test
    fun connectionClosedMidBodyIsNotEndOfStream() {
        countOutcome = CompletableFuture()
        Socket("127.0.0.1", server.port).use { socket ->
            val envelopes = envelope(0, "one".toByteArray()) + envelope(0, "two".toByteArray())
            socket.getOutputStream().write(
                (
                    "POST $BASE/$COUNT HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/connect+proto\r\n" +
                        "Transfer-Encoding: chunked\r\n\r\n${Integer.toHexString(envelopes.size)}\r\n"
                    ).toByteArray() +
                    envelopes +
                    "\r\n".toByteArray(),
            )
            socket.getOutputStream().flush()
            Thread.sleep(200)
        }
        assertThat(countOutcome.get(5, TimeUnit.SECONDS)).startsWith("failed")
    }

    /**
     * Every method reaches the engine, and complete responses carry
     * `Content-Length` rather than chunked framing, with the engine's headers
     * (Content-Type included) as given. Streamed responses are chunked.
     */
    @Test
    fun responsesKeepEngineHeadersAndFraming() {
        data class Case(
            val requestLine: String,
            val contentType: String?,
            val body: ByteArray?,
            val status: Int,
            val headers: Map<String, List<String>>,
            val chunked: Boolean = false,
        )
        val cases = listOf(
            Case("POST $BASE/$ECHO HTTP/1.1", "application/proto", "ping".toByteArray(), 200, mapOf("Content-Type" to listOf("application/proto"), "x-multi" to listOf("a", "b"))),
            Case("GET $BASE/$ECHO?encoding=proto&message=hi HTTP/1.1", null, null, 200, mapOf("Content-Type" to listOf("application/proto"))),
            Case("POST $BASE/$FAIL HTTP/1.1", "application/proto", ByteArray(0), 404, mapOf("Content-Type" to listOf("application/json"))),
            Case("PUT $BASE/$ECHO HTTP/1.1", "application/proto", ByteArray(0), 405, mapOf("Allow" to listOf("GET, POST"))),
            Case("DELETE $BASE/$ECHO HTTP/1.1", null, null, 405, mapOf("Allow" to listOf("GET, POST"))),
            Case("POST $BASE/$STREAM HTTP/1.1", "application/connect+proto", envelope(0, "x".toByteArray()), 200, mapOf("Content-Type" to listOf("application/connect+proto")), chunked = true),
        )
        for (case in cases) {
            val response = rawExchange(server.port, case.requestLine, listOfNotNull(case.contentType?.let { "Content-Type: $it" }), case.body)
            val label = "${case.requestLine} ${case.contentType}"
            assertThat(response.status).describedAs(label).isEqualTo(case.status)
            for ((name, values) in case.headers) assertThat(response.values(name)).describedAs("$label $name").isEqualTo(values)
            if (case.chunked) {
                assertThat(response.header("Transfer-Encoding")).describedAs(label).isEqualTo("chunked")
                assertThat(response.values("Content-Length")).describedAs(label).isEmpty()
            } else {
                assertThat(response.header("Content-Length")).describedAs(label).isEqualTo(response.body.size.toString())
                assertThat(response.values("Transfer-Encoding")).describedAs(label).isEmpty()
            }
        }
    }

    /**
     * Tomcat maps servlets on the decoded, normalized path while the exchange
     * gets the raw one, so only the canonical spelling of the context path,
     * prefix and procedure is served (protocol.md "Path": case-sensitive).
     */
    @Test
    fun onlyTheCanonicalPathIsServed() {
        val cases = mapOf(
            "$BASE/$ECHO" to 200,
            "/api/rpc/test.v1.TestService/%45cho" to 404,
            "/api/r%70c/$ECHO" to 404,
            "/api/rpc//$ECHO" to 404,
            "/api/rpc/./$ECHO" to 404,
            "/api/x/../rpc/$ECHO" to 404,
            "/api/$ECHO" to 404,
            "/rpc/$ECHO" to 404,
        )
        for ((path, status) in cases) {
            val response = rawExchange(server.port, "POST $path HTTP/1.1", listOf("Content-Type: application/proto"), "hi".toByteArray())
            assertThat(response.status).describedAs(path).isEqualTo(status)
        }
    }

    /**
     * HTTP/1.0 has no trailers, so gRPC is refused Trailers-Only with
     * `unimplemented` rather than answered without `grpc-status`
     * (PROTOCOL-HTTP2.md "Responses").
     */
    @Test
    fun grpcOverHttp10IsRefusedTrailersOnly() {
        val response = rawExchange(server.port, "POST $BASE/$ECHO HTTP/1.0", listOf("Content-Type: application/grpc"), envelope(0, "ping".toByteArray()))
        assertThat(response.status).isEqualTo(200)
        assertThat(response.header("grpc-status")).isEqualTo(Code.UNIMPLEMENTED.value.toString())
        assertThat(response.body).isEmpty()
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = registry()
    }
}
