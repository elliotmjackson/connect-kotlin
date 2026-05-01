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
import com.connectrpc.server.UnaryHandler
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean

/**
 * TDD targets for timeout enforcement and request cancellation.
 *
 * The server currently parses the timeout header into HandlerContext.timeoutMs
 * but does not actually cancel the handler coroutine, and a client disconnect
 * is not propagated either. Both are required for a production-grade server.
 */
class TimeoutCancellationTest {

    /**
     * `Connect-Timeout-Ms: 100` should cause the server to abort a 2-second
     * handler with `code: deadline_exceeded` shortly after the deadline,
     * rather than waiting for the handler to complete naturally.
     *
     * Currently fails: the handler runs to completion and we return its
     * result regardless of the elapsed time.
     */
    @Test
    fun connectTimeoutCancelsHandler() {
        val cancelled = AtomicBoolean(false)
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Unary",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                try {
                    delay(2_000)
                    return TestMessage("late")
                } catch (e: kotlinx.coroutines.CancellationException) {
                    cancelled.set(true)
                    throw e
                }
            }
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .header("Connect-Timeout-Ms", "100")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            val started = System.currentTimeMillis()
            val response = newTestClient().newCall(req).execute()
            val elapsed = System.currentTimeMillis() - started
            response.use {
                // We expect a Connect error response with code "deadline_exceeded"
                // and HTTP status 504, well before the 2s handler delay.
                assertThat(elapsed)
                    .describedAs("response time should be near the 100ms deadline")
                    .isLessThan(500)
                assertThat(it.code).isEqualTo(504)
                val body = it.body!!.string()
                assertThat(body).contains("\"code\":\"deadline_exceeded\"")
            }
            // Make sure the handler's coroutine actually cancelled, not just
            // that we returned a deadline-exceeded response and let it run on.
            assertThat(cancelled.get())
                .describedAs("handler must observe cancellation via CancellationException")
                .isTrue()
        }
    }

    /**
     * When the client closes the underlying connection mid-handler, the
     * handler's coroutine should be cancelled.
     *
     * Verified empirically that even with a real raw-Socket close (not
     * OkHttp's pooled cancel), Ktor Netty does not propagate
     * `channelInactive` to the application coroutine — the handler keeps
     * running until it tries to write a response. To make this test pass,
     * the adapter needs a Netty channel-inactive bridge installed via
     * `channelPipelineConfig` that maps back to the [ApplicationCall]'s
     * Job and cancels it. The mapping isn't exposed by Ktor's public API,
     * so this needs reflection or a custom call attributes channel.
     */
    @Test
    @Ignore("TDD target: Ktor Netty doesn't propagate channelInactive; needs custom Netty handler bridging to ApplicationCall.coroutineContext")
    fun clientDisconnectCancelsHandler() {
        val cancelled = AtomicBoolean(false)
        val cancelledLatch = java.util.concurrent.CountDownLatch(1)
        val handlerEnteredLatch = java.util.concurrent.CountDownLatch(1)

        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Unary",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                handlerEnteredLatch.countDown()
                try {
                    delay(30_000)
                    return TestMessage("never")
                } catch (e: kotlinx.coroutines.CancellationException) {
                    cancelled.set(true)
                    cancelledLatch.countDown()
                    throw e
                }
            }
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            // Open a raw socket and write a minimal HTTP/1.1 POST.
            val socket = java.net.Socket("127.0.0.1", server.port)
            socket.use {
                val out = it.getOutputStream()
                val request = (
                    "POST /test.v1.TestService/Unary HTTP/1.1\r\n" +
                        "Host: 127.0.0.1\r\n" +
                        "Content-Type: application/proto\r\n" +
                        "Content-Length: 0\r\n" +
                        "Connection: close\r\n" +
                        "\r\n"
                    ).toByteArray()
                out.write(request)
                out.flush()

                // Wait for the handler to actually start before pulling the rug.
                assertThat(
                    handlerEnteredLatch.await(2, java.util.concurrent.TimeUnit.SECONDS),
                ).describedAs("handler must be reached before we close the socket").isTrue()

                // Closing the socket on `use` block exit (next line) sends FIN/RST.
            }
            // Closed; wait for the server to observe the cancellation.
            assertThat(
                cancelledLatch.await(2, java.util.concurrent.TimeUnit.SECONDS),
            ).describedAs("handler must observe cancellation within 2s of client close").isTrue()
            assertThat(cancelled.get()).isTrue()
        }
    }
}
