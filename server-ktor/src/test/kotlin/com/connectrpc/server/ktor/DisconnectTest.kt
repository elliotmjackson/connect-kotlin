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
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerObserver
import com.connectrpc.server.ServerStreamHandler
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.netty.NettyApplicationCall
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.Socket
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A call whose client has gone away ends, and so does everything the engine
 * keeps for its connection. On HTTP/2 the engine runs each connection in a
 * child of the application's job, which lives until every stream's request
 * body has ended or failed (ktor-server-netty 3.6.0
 * `http2/NettyHttp2Handler.kt:40, 285-287`,
 * `http2/NettyHttp2ApplicationRequest.kt:47-53`), so a connection is released
 * once the application's job is back to the children it had before.
 */
class DisconnectTest {
    private val started = AtomicInteger()

    // Connect code names, "ok" for success.
    private val ended = ConcurrentLinkedQueue<String>()
    private val completed = AtomicInteger()
    private val held = Semaphore(0)

    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(HOLD, TestMessage::class, TestMessage::class, StreamType.SERVER)
                override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) = awaitCancellation()
            },
        )
        .register(
            object : ClientStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(COUNT, TestMessage::class, TestMessage::class, StreamType.CLIENT)
                override suspend fun handle(stream: ClientMessageStream<TestMessage>, ctx: HandlerContext): TestMessage {
                    var count = 0
                    while (stream.receive() != null) count++
                    completed.incrementAndGet()
                    return TestMessage("$count")
                }
            },
        )
        .build()

    private val config = ServerConfig(
        observer = { _, _, _ ->
            started.incrementAndGet()
            ServerObserver.Call { code -> ended.add(code?.codeName ?: "ok") }
        },
    )

    /** Calls reading their request bodies when the connection closes. */
    @Test
    fun http2ConnectionCloseReleasesTheConnection() {
        lateinit var application: Application
        TestServer.start(registry, config, withH2c = true, module = { application = this }).use { server ->
            val baseline = application.jobChildren()
            RawHttp2Connection(server.port).use { connection ->
                repeat(CALLS) { connection.openStream("/$HOLD", "application/grpc") }
                awaitUntil { started.get() == CALLS }
            }
            assertAllEndCanceled()
            awaitUntil { (application.jobChildren() - baseline).isEmpty() }
            assertThat(application.jobChildren() - baseline).isEmpty()
        }
    }

    /**
     * Streams the server resets, here for malformed requests, on a connection
     * that stays open: nothing of them may stay in the connection's scope.
     */
    @Test
    fun http2StreamResetByTheServerIsReleased() {
        lateinit var application: Application
        TestServer.start(registry, config, withH2c = true, module = { application = this }).use { server ->
            val before = application.coroutineContext[Job]!!.children.toSet()
            RawHttp2Connection(server.port).use { connection ->
                val streams = List(CALLS) { connection.openStream("/$HOLD", "application/grpc") }
                awaitUntil { started.get() == CALLS }
                streams.forEach(connection::sendTrailersWithoutEndStream)
                assertAllEndCanceled()
                val scope = (application.coroutineContext[Job]!!.children.toSet() - before).single()
                awaitUntil { scope.children.none() }
                assertThat(scope.children.toList()).isEmpty()
            }
        }
    }

    /** Streams whose connection closed before routing reached their calls. */
    @Test
    fun http2ConnectionClosedBeforeRoutingReleasesTheConnection() {
        lateinit var application: Application
        TestServer.start(
            registry,
            config,
            withH2c = true,
            module = {
                application = this
                holdUntilClosed()
            },
        ).use { server ->
            val baseline = application.jobChildren()
            RawHttp2Connection(server.port).use { connection ->
                repeat(CALLS) { connection.openStream("/$HOLD", "application/grpc") }
                assertThat(held.tryAcquire(CALLS, 5, TimeUnit.SECONDS)).isTrue()
            }
            awaitUntil { (application.jobChildren() - baseline).isEmpty() && ended.size == started.get() }
            assertThat(application.jobChildren() - baseline).isEmpty()
            assertThat(ended).hasSize(started.get()).containsOnly(Code.CANCELED.codeName)
        }
    }

    /**
     * A client stream cut off with its HTTP/1.1 connection before routing
     * reached the call: the handler must not take the messages that arrived
     * for the whole stream.
     */
    @Test
    fun http1ConnectionClosedBeforeRoutingCancelsTheCall() {
        TestServer.start(registry, config, module = { holdUntilClosed() }).use { server ->
            repeat(CALLS) {
                Socket("127.0.0.1", server.port).use { socket ->
                    val body = envelope(0, "one".toByteArray())
                    socket.getOutputStream().write(
                        (
                            "POST /$COUNT HTTP/1.1\r\nHost: localhost\r\n" +
                                "Content-Type: application/connect+proto\r\nContent-Length: ${body.size + 100}\r\n\r\n"
                            ).toByteArray() +
                            body,
                    )
                    socket.getOutputStream().flush()
                    assertThat(held.tryAcquire(5, TimeUnit.SECONDS)).isTrue()
                }
            }
            assertAllEndCanceled()
            assertThat(completed.get()).isZero()
        }
    }

    /** Holds each call before routing until its HTTP/2 stream, or its HTTP/1.1 connection, has closed. */
    private fun Application.holdUntilClosed() {
        intercept(ApplicationCallPipeline.Setup) {
            val closed = CompletableDeferred<Unit>()
            (call as NettyApplicationCall).context.channel().closeFuture().addListener { closed.complete(Unit) }
            held.release()
            closed.await()
        }
    }

    private fun Application.jobChildren(): Set<Job> = coroutineContext[Job]!!.children.toSet()

    private fun assertAllEndCanceled() {
        awaitUntil { ended.size == CALLS }
        assertThat(started.get()).isEqualTo(CALLS)
        assertThat(ended).hasSize(CALLS).containsOnly(Code.CANCELED.codeName)
    }

    private fun awaitUntil(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition() && System.nanoTime() < deadline) Thread.sleep(10)
    }

    private companion object {
        const val HOLD = "test.v1.TestService/Hold"
        const val COUNT = "test.v1.TestService/Count"
        const val CALLS = 8
    }
}
