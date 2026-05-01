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
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * When the Ktor application is stopped while RPCs are in flight, in-flight
 * handlers must observe cancellation cleanly rather than running to
 * completion against a torn-down engine. The wiring relies on
 * [withCancellationOnDisconnect] catching the channel close that Netty
 * fires during shutdown.
 */
class GracefulShutdownTest {

    /**
     * A long-running unary handler is interrupted when the server stops
     * before its delay elapses.
     */
    @Test
    fun unaryHandlerCancelledOnServerStop() {
        val cancelled = AtomicBoolean(false)
        val cancelledLatch = CountDownLatch(1)
        val handlerEnteredLatch = CountDownLatch(1)

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

        val server = TestServer.start(registry)
        val client = newTestClient(callTimeoutMs = 30_000)
        val callThread = Thread {
            try {
                val req = Request.Builder()
                    .url("${server.baseUrl}/test.v1.TestService/Unary")
                    .header("Content-Type", "application/proto")
                    .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                    .build()
                client.newCall(req).execute().use { it.body?.bytes() }
            } catch (_: Exception) {
                // expected — connection torn down by server stop
            }
        }
        callThread.start()

        assertThat(
            handlerEnteredLatch.await(5, TimeUnit.SECONDS),
        ).describedAs("handler must enter before we stop the server").isTrue()

        server.close()

        assertThat(
            cancelledLatch.await(5, TimeUnit.SECONDS),
        ).describedAs("in-flight unary handler must observe cancellation when server stops").isTrue()
        assertThat(cancelled.get()).isTrue()
        callThread.join(5_000)
    }

    /**
     * A server-stream handler that has already committed headers and is
     * blocked between sends gets cancelled when the engine shuts down.
     */
    @Test
    fun serverStreamHandlerCancelledOnServerStop() {
        val cancelled = AtomicBoolean(false)
        val cancelledLatch = CountDownLatch(1)
        val firstSendLatch = CountDownLatch(1)

        val handler = object : ServerStreamHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Stream",
                TestMessage::class,
                TestMessage::class,
                StreamType.SERVER,
            )

            override suspend fun handle(
                request: TestMessage,
                ctx: HandlerContext,
                stream: ServerMessageStream<TestMessage>,
            ) {
                stream.send(TestMessage("first"))
                firstSendLatch.countDown()
                try {
                    delay(30_000)
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

        val server = TestServer.start(registry)
        val client = newTestClient(callTimeoutMs = 30_000)
        val callThread = Thread {
            try {
                val req = Request.Builder()
                    .url("${server.baseUrl}/test.v1.TestService/Stream")
                    .header("Content-Type", "application/connect+proto")
                    .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
                    .build()
                client.newCall(req).execute().use { it.body?.bytes() }
            } catch (_: Exception) {
                // expected — connection torn down
            }
        }
        callThread.start()

        assertThat(
            firstSendLatch.await(5, TimeUnit.SECONDS),
        ).describedAs("handler must reach its first send before we stop the server").isTrue()

        server.close()

        assertThat(
            cancelledLatch.await(5, TimeUnit.SECONDS),
        ).describedAs("in-flight server-stream handler must observe cancellation when server stops").isTrue()
        assertThat(cancelled.get()).isTrue()
        callThread.join(5_000)
    }
}
