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
import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import io.ktor.http.Headers
import io.ktor.http.content.OutgoingContent
import io.ktor.server.application.Application
import io.ktor.server.application.PipelineCall
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.server.response.ApplicationSendPipeline
import io.ktor.server.routing.RoutingPipelineCall
import io.ktor.utils.io.ByteWriteChannel
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import okio.ByteString.Companion.toByteString
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * gRPC over HTTP/2 against a real Netty engine: every response must end with
 * a trailer block carrying `grpc-status` (and, on error, `grpc-message` and
 * `grpc-status-details-bin`) plus the handler's trailers
 * (https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#responses).
 *
 * Ktor's Netty engine ends the HTTP/2 stream on its event loop once the body
 * channel closes, while the call thread copies `OutgoingContent.trailers()`
 * into the stream only after that close (ktor-server-netty 3.6.0
 * `http2/NettyHttp2ApplicationResponse.kt:50-61`,
 * `cio/NettyHttpResponsePipeline.kt:348-381`). [endStreamBeforeTrailersCopy]
 * holds the call thread in that gap until the stream has closed, so every
 * call takes the interleaving that ends the stream without trailers.
 */
class GrpcHttp2TrailersTest {

    @Test
    fun unarySuccessAlwaysEndsWithStatusTrailer() {
        val results = callConcurrently(UNARY_OK_PATH)

        assertThat(results.filterNot { it.trailers == expectedTrailers(status = "0") })
            .describedAs("responses without the expected trailer block, of ${results.size}")
            .isEmpty()
        assertThat(results.map { it.messages }).allMatch { it == listOf("pong") }
    }

    @Test
    fun unaryErrorAlwaysEndsWithStatusMessageAndDetailsTrailers() {
        val results = callConcurrently(UNARY_ERROR_PATH)

        assertThat(results.filterNot { it.trailers == expectedErrorTrailers() })
            .describedAs("responses without the expected trailer block, of ${results.size}")
            .isEmpty()
        assertThat(results.map { it.messages }).allMatch { it.isEmpty() }
    }

    @Test
    fun serverStreamErrorAfterMessagesAlwaysEndsWithTrailers() {
        val results = callConcurrently(SERVER_STREAM_ERROR_PATH)

        assertThat(results.filterNot { it.trailers == expectedErrorTrailers() })
            .describedAs("responses without the expected trailer block, of ${results.size}")
            .isEmpty()
        assertThat(results.map { it.messages }).allMatch { it == listOf("a", "b", "c") }
    }

    @Test
    fun bidiStreamErrorAfterEchoAlwaysEndsWithTrailers() {
        val results = callConcurrently(BIDI_ERROR_PATH)

        assertThat(results.filterNot { it.trailers == expectedErrorTrailers() })
            .describedAs("responses without the expected trailer block, of ${results.size}")
            .isEmpty()
        assertThat(results.map { it.messages }).allMatch { it == listOf("ping") }
    }

    /**
     * Errors raised before the handler runs are sent Trailers-Only: a single
     * HEADERS frame with `grpc-status` and no trailer block.
     */
    @Test
    fun rejectedRequestIsTrailersOnly() {
        TestServer.start(registry(), withH2c = true).use { server ->
            val request = grpcRequest(server.baseUrl, UNARY_OK_PATH)
                .newBuilder()
                .header("Grpc-Encoding", "snappy")
                .build()
            newTestClient(h2cPriorKnowledge = true).newCall(request).execute().use { response ->
                assertThat(response.body!!.bytes()).isEmpty()
                assertThat(response.header("grpc-status")).isEqualTo(Code.UNIMPLEMENTED.value.toString())
                assertThat(response.header("grpc-message")).isEqualTo("unknown compression \"snappy\": supported encodings are gzip,deflate")
                assertThat(response.trailers().size).isZero()
            }
        }
    }

    /**
     * Ktor sends HTTP trailers only on Netty HTTP/2, so gRPC over HTTP/1.1
     * cannot carry its status in trailers (PROTOCOL-HTTP2.md "Responses"). It is
     * refused Trailers-Only instead of answering 200 without a status.
     */
    @Test
    fun grpcOverHttp1IsRefusedTrailersOnly() {
        TestServer.start(registry()).use { server ->
            newTestClient().newCall(grpcRequest(server.baseUrl, UNARY_OK_PATH)).execute().use { response ->
                assertThat(response.protocol).isEqualTo(okhttp3.Protocol.HTTP_1_1)
                assertThat(response.code).isEqualTo(200)
                assertThat(response.header("grpc-status")).isEqualTo(Code.UNIMPLEMENTED.value.toString())
                assertThat(response.body!!.bytes()).isEmpty()
            }
        }
    }

    private fun callConcurrently(path: String): List<GrpcResult> = TestServer.start(registry(), withH2c = true, module = { endStreamBeforeTrailersCopy() }).use { server ->
        val client = newTestClient(h2cPriorKnowledge = true)
        val pool = Executors.newFixedThreadPool(CONCURRENCY)
        try {
            pool.invokeAll(List(CALLS) { Callable { execute(client, grpcRequest(server.baseUrl, path)) } })
                .map { it.get() }
        } finally {
            pool.shutdownNow()
            pool.awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    /**
     * Wraps each streamed response so that the engine's read of
     * [OutgoingContent.trailers], which happens after the body channel is
     * closed, blocks until the HTTP/2 stream channel has closed.
     */
    private fun Application.endStreamBeforeTrailersCopy() {
        sendPipeline.intercept(ApplicationSendPipeline.After) { message ->
            if (message !is OutgoingContent.WriteChannelContent) return@intercept
            val engineCall = generateSequence<PipelineCall>(context) { (it as? RoutingPipelineCall)?.engineCall }
                .filterIsInstance<NettyApplicationCall>()
                .first()
            val streamClosed = engineCall.context.channel().closeFuture()
            proceedWith(
                object : OutgoingContent.WriteChannelContent() {
                    override val contentType get() = message.contentType
                    override val contentLength get() = message.contentLength
                    override val status get() = message.status
                    override val headers get() = message.headers
                    override suspend fun writeTo(channel: ByteWriteChannel) = message.writeTo(channel)
                    override fun trailers(): Headers? {
                        streamClosed.awaitUninterruptibly(STREAM_CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)
                        return message.trailers()
                    }
                },
            )
        }
    }

    private data class GrpcResult(
        val messages: List<String>,
        val trailers: Map<String, List<String>>,
    )

    private fun execute(client: OkHttpClient, request: Request): GrpcResult = client.newCall(request).execute().use { response ->
        val body = Buffer().write(response.body!!.bytes())
        val messages = generateSequence { readEnvelope(body) }
            .map { String(it.payload, Charsets.UTF_8) }
            .toList()
        GrpcResult(messages, response.trailers().toMultimap())
    }

    private fun grpcRequest(baseUrl: String, path: String): Request = Request.Builder()
        .url("$baseUrl/$path")
        .header("TE", "trailers")
        .post(envelope(0, "ping".toByteArray()).toRequestBody(GRPC_PROTO.toMediaType()))
        .build()

    private fun expectedTrailers(status: String): Map<String, List<String>> = mapOf(
        "grpc-status" to listOf(status),
        "x-custom-trailer" to listOf("bing"),
    )

    private fun expectedErrorTrailers(): Map<String, List<String>> = expectedTrailers(
        status = Code.FAILED_PRECONDITION.value.toString(),
    ) +
        mapOf(
            // PROTOCOL-HTTP2.md: grpc-message is percent-encoded UTF-8; space stays literal.
            "grpc-message" to listOf("precondition failed: %C3%BC"),
            "grpc-status-details-bin" to listOf(DETAILS_BIN),
        )

    private fun registry(): HandlerRegistry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            unary(UNARY_OK_PATH) { ctx ->
                ctx.addTrailer()
                TestMessage("pong")
            },
        )
        .register(
            unary(UNARY_ERROR_PATH) { ctx ->
                ctx.addTrailer()
                throw failure()
            },
        )
        .register(
            object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    SERVER_STREAM_ERROR_PATH,
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.SERVER,
                )

                override suspend fun handle(
                    request: TestMessage,
                    ctx: HandlerContext,
                    stream: ServerMessageStream<TestMessage>,
                ) {
                    for (text in listOf("a", "b", "c")) stream.send(TestMessage(text))
                    ctx.addTrailer()
                    throw failure()
                }
            },
        )
        .register(
            object : BidiStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    BIDI_ERROR_PATH,
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.BIDI,
                )

                override suspend fun handle(stream: BidiStream<TestMessage, TestMessage>, ctx: HandlerContext) {
                    while (true) stream.send(stream.receive() ?: break)
                    ctx.addTrailer()
                    throw failure()
                }
            },
        )
        .build()

    private fun unary(
        path: String,
        body: (HandlerContext) -> TestMessage,
    ): UnaryHandler<TestMessage, TestMessage> = object : UnaryHandler<TestMessage, TestMessage> {
        override val methodSpec = MethodSpec(path, TestMessage::class, TestMessage::class, StreamType.UNARY)

        override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = body(ctx)
    }

    private fun HandlerContext.addTrailer() {
        responseTrailers["x-custom-trailer"] = mutableListOf("bing")
    }

    private fun failure(): ConnectException = ConnectException(Code.FAILED_PRECONDITION, message = "precondition failed: ü")
        .withErrorDetails(TestSerializationStrategy.errorDetailParser(), listOf(DETAIL))

    private companion object {
        const val CALLS = 64
        const val CONCURRENCY = 8
        const val STREAM_CLOSE_WAIT_SECONDS = 5L
        const val GRPC_PROTO = "application/grpc+proto"
        const val UNARY_OK_PATH = "test.v1.TestService/Unary"
        const val UNARY_ERROR_PATH = "test.v1.TestService/UnaryError"
        const val SERVER_STREAM_ERROR_PATH = "test.v1.TestService/ServerStreamError"
        const val BIDI_ERROR_PATH = "test.v1.TestService/BidiError"

        val DETAIL = ConnectErrorDetail(
            "type.googleapis.com/test.v1.Detail",
            byteArrayOf(1, 2, 3).toByteString(),
        )

        // google.rpc.Status{code: 9, message: "precondition failed: ü",
        // details: [Any{type_url: "type.googleapis.com/test.v1.Detail", value: 0x010203}]},
        // unpadded base64 per PROTOCOL-HTTP2.md ("-bin" values).
        val DETAILS_BIN: String = run {
            val message = "precondition failed: ü".toByteArray(Charsets.UTF_8)
            val typeUrl = "type.googleapis.com/test.v1.Detail".toByteArray(Charsets.UTF_8)
            val any = Buffer()
                .writeByte(0x0a).writeByte(typeUrl.size).write(typeUrl)
                .writeByte(0x12).writeByte(3).write(byteArrayOf(1, 2, 3))
                .readByteArray()
            Buffer()
                .writeByte(0x08).writeByte(Code.FAILED_PRECONDITION.value)
                .writeByte(0x12).writeByte(message.size).write(message)
                .writeByte(0x1a).writeByte(any.size).write(any)
                .readByteString()
                .base64()
                .trimEnd('=')
        }
    }
}
