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

package com.connectrpc.server

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class CallLifecycleTest {
    private fun streamExchange(method: String, contentType: String = "application/connect+proto", gate: Channel<Unit>? = null) = FakeExchange(path = "/$SVC/$method", requestHeaders = mapOf("content-type" to listOf(contentType)), writeGate = gate)

    @Test
    fun slowClientStallsTheHandler(): Unit = runBlocking {
        val sent = AtomicInteger()
        val server = server(
            serverStream { _, _, s ->
                repeat(1000) {
                    s.send(Msg("m$it"))
                    sent.incrementAndGet()
                }
            },
        )
        val gate = Channel<Unit>(Channel.UNLIMITED)
        val exchange = streamExchange("ServerStream", gate = gate)
        exchange.bodyChunks.trySend(env(0, "go"))
        exchange.bodyChunks.close()
        val call = launch { server.serve(exchange) }
        delay(200)
        // No write has been accepted: at most one message is serialised ahead of the stalled writer.
        assertThat(exchange.writes.get()).isZero()
        assertThat(sent.get()).isLessThanOrEqualTo(1)
        repeat(1001) { gate.send(Unit) }
        call.join()
        assertThat(sent.get()).isEqualTo(1000)
        assertThat(frames(exchange.bodyBytes)).hasSize(1001)
    }

    @Test
    fun cancellingTheCallCancelsTheHandler(): Unit = runBlocking {
        val started = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Throwable>()
        val server = server(
            serverStream { _, _, s ->
                s.send(Msg("first"))
                started.complete(Unit)
                try {
                    delay(60_000)
                } catch (e: CancellationException) {
                    cancelled.complete(e)
                    throw e
                }
            },
        )
        val exchange = streamExchange("ServerStream")
        exchange.bodyChunks.trySend(env(0, "go"))
        exchange.bodyChunks.close()
        val call = launch { server.serve(exchange) }
        withTimeout(5_000) { started.await() }
        call.cancel()
        assertThat(withTimeout(5_000) { cancelled.await() }).isInstanceOf(CancellationException::class.java)
        call.join()
        // Nothing is written after the call is gone: no end-stream frame.
        assertThat(frames(exchange.bodyBytes).map { it.flags }).containsExactly(0)
    }

    @Test
    fun cancelledServeRethrows() {
        val server = server(
            unary { _, _ ->
                delay(60_000)
                Msg("never")
            },
        )
        val exchange = FakeExchange(path = "/$SVC/Unary", requestHeaders = mapOf("content-type" to listOf("application/proto")))
        exchange.bodyChunks.trySend("a".toByteArray())
        exchange.bodyChunks.close()
        assertThatThrownBy { runBlocking { withTimeout(100) { server.serve(exchange) } } }
            .isInstanceOf(CancellationException::class.java)
        assertThat(exchange.status).isNull()
    }

    @Test
    fun fullDuplexBidi(): Unit = runBlocking {
        val server = server(
            bidi { stream, _ ->
                while (true) {
                    val msg = stream.receive() ?: break
                    stream.send(Msg("echo:${msg.text}"))
                }
            },
        )
        val exchange = streamExchange("Bidi", "application/grpc")
        val call = launch { server.serve(exchange) }
        exchange.bodyChunks.send(env(0, "1"))
        // The response to the first message arrives while the request is still open.
        withTimeout(5_000) { while (exchange.writes.get() < 1) delay(5) }
        assertThat(frames(exchange.bodyBytes).single().text).isEqualTo("echo:1")
        exchange.bodyChunks.send(env(0, "2"))
        exchange.bodyChunks.close()
        call.join()
        assertThat(frames(exchange.bodyBytes).map { it.text }).containsExactly("echo:1", "echo:2")
        assertThat(exchange.trailers["grpc-status"]).containsExactly("0")
    }

    @Test
    fun headersCommitAtFirstSend() {
        val server = server(
            serverStream { _, ctx, s ->
                ctx.responseHeaders["x-before"] = mutableListOf("1")
                s.send(Msg("m"))
                ctx.responseHeaders["x-after"] = mutableListOf("2")
            },
        )
        val ex = server.call("ServerStream", "application/connect+proto", env(0, "a"))
        assertThat(ex.headers).containsKey("x-before").doesNotContainKey("x-after")
    }
}
