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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Behaviour owned by the Ktor bridge rather than the protocol engine. */
class BridgeTest {
    private val sent = AtomicInteger()
    private val firstSent = CompletableFuture<Unit>()
    private val cancelled = CompletableFuture<Throwable>()

    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec("test.v1.TestService/Unary", TestMessage::class, TestMessage::class, StreamType.UNARY)
                override suspend fun handle(request: TestMessage, ctx: HandlerContext) = TestMessage("pong:${request.text()}")
            },
        )
        .register(
            object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec("test.v1.TestService/Flood", TestMessage::class, TestMessage::class, StreamType.SERVER)
                override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                    try {
                        val chunk = ByteArray(request.text().toInt())
                        while (true) {
                            stream.send(TestMessage(chunk))
                            sent.incrementAndGet()
                            firstSent.complete(Unit)
                            if (chunk.isEmpty()) awaitCancellation()
                        }
                    } catch (e: CancellationException) {
                        cancelled.complete(e)
                        throw e
                    }
                }
            },
        )
        .build()

    @Test
    fun grpcAndConnectOverTlsHttp2() {
        TestServer.start(registry, withTls = true).use { server ->
            val client = newTlsTestClient()
            val grpc = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("TE", "trailers")
                .post(envelope(0, "ping".toByteArray()).toRequestBody("application/grpc".toMediaType()))
                .build()
            client.newCall(grpc).execute().use { response ->
                assertThat(response.protocol).isEqualTo(Protocol.HTTP_2)
                val message = readEnvelope(Buffer().write(response.body!!.bytes()))!!
                assertThat(String(message.payload)).isEqualTo("pong:ping")
                assertThat(response.trailers()["grpc-status"]).isEqualTo("0")
            }
            val connect = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .post("ping".toByteArray().toRequestBody("application/proto".toMediaType()))
                .build()
            client.newCall(connect).execute().use { response ->
                assertThat(response.protocol).isEqualTo(Protocol.HTTP_2)
                assertThat(response.body!!.string()).isEqualTo("pong:ping")
            }
        }
    }

    /** A client that stops reading stalls the handler's sends instead of growing server memory. */
    @Test
    fun slowClientBackPressuresServerStream() {
        TestServer.start(registry).use { server ->
            Socket("127.0.0.1", server.port).use { socket ->
                val body = envelope(0, "65536".toByteArray())
                socket.getOutputStream().write(
                    (
                        "POST /test.v1.TestService/Flood HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/connect+proto\r\nContent-Length: ${body.size}\r\n\r\n"
                        ).toByteArray() +
                        body,
                )
                socket.getOutputStream().flush()
                firstSent.get(5, TimeUnit.SECONDS)
                Thread.sleep(2_000)
                val stalledAt = sent.get()
                Thread.sleep(1_000)
                // 64 KiB messages: the socket buffers plus Ktor's channel hold a few MiB at most.
                assertThat(stalledAt).isLessThan(128)
                assertThat(sent.get()).isEqualTo(stalledAt)
            }
            assertThat(cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
        }
    }

    @Test
    fun http2StreamResetCancelsHandler() {
        TestServer.start(registry, withH2c = true).use { server ->
            val call = newTestClient(h2cPriorKnowledge = true).newCall(
                Request.Builder()
                    .url("${server.baseUrl}/test.v1.TestService/Flood")
                    .post(envelope(0, "0".toByteArray()).toRequestBody("application/connect+proto".toMediaType()))
                    .build(),
            )
            call.execute().use { response ->
                assertThat(response.protocol).isEqualTo(Protocol.H2_PRIOR_KNOWLEDGE)
                firstSent.get(5, TimeUnit.SECONDS)
                call.cancel()
            }
            assertThat(cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
        }
    }

    @Test
    fun http1DisconnectCancelsHandler() {
        TestServer.start(registry).use { server ->
            Socket("127.0.0.1", server.port).use { socket ->
                val body = envelope(0, "0".toByteArray())
                socket.getOutputStream().write(
                    (
                        "POST /test.v1.TestService/Flood HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/connect+proto\r\nContent-Length: ${body.size}\r\n\r\n"
                        ).toByteArray() +
                        body,
                )
                firstSent.get(5, TimeUnit.SECONDS)
            }
            assertThat(cancelled.get(5, TimeUnit.SECONDS)).isInstanceOf(CancellationException::class.java)
        }
    }
}
