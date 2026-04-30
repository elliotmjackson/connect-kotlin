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
    @Ignore("TDD target: Connect-Timeout-Ms is parsed but not enforced; need handler cancellation at deadline")
    fun connectTimeoutCancelsHandler() {
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Unary",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                delay(2_000)
                return TestMessage("late")
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
        }
    }

    /**
     * When the client closes the underlying connection mid-stream, the
     * handler's coroutine should be cancelled. We assert this via a flag
     * the handler sets in its `finally` block.
     *
     * Currently fails: nothing wires the request lifecycle to handler
     * cancellation, so the handler runs to completion regardless.
     */
    @Test
    @Ignore("TDD target: client disconnect is not propagated to handler cancellation")
    fun clientDisconnectCancelsHandler() {
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
                    delay(5_000)
                    return TestMessage("never")
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
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()
            val call = newTestClient(callTimeoutMs = 30_000).newCall(req)
            // Start the call asynchronously and cancel it after a short delay.
            val responseLatch = java.util.concurrent.CountDownLatch(1)
            val callback = object : okhttp3.Callback {
                override fun onFailure(call: okhttp3.Call, e: java.io.IOException) {
                    responseLatch.countDown()
                }
                override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                    response.close()
                    responseLatch.countDown()
                }
            }
            call.enqueue(callback)
            Thread.sleep(200) // let the request reach the handler
            call.cancel()

            // Give the server a moment to observe the cancellation.
            Thread.sleep(500)
            assertThat(cancelled.get())
                .describedAs("handler should observe cancellation when client disconnects")
                .isTrue()
        }
    }
}
