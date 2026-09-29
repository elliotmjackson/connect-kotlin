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
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.Socket
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

class MultipleMountShutdownTest {
    @Test
    fun separateMountsShareOneGracePeriod() {
        val entered = CountDownLatch(2)
        val cancelledAt = ConcurrentLinkedQueue<Long>()
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : ClientStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Hold", TestMessage::class, TestMessage::class, StreamType.CLIENT)

                    override suspend fun handle(stream: ClientMessageStream<TestMessage>, ctx: HandlerContext): TestMessage {
                        entered.countDown()
                        try {
                            awaitCancellation()
                        } finally {
                            cancelledAt.add(System.nanoTime())
                        }
                    }
                },
            )
            .build()
        val app = embeddedServer(
            factory = Netty,
            environment = applicationEnvironment { },
            configure = {
                connector {
                    host = "127.0.0.1"
                    port = 0
                }
            },
            module = {
                routing {
                    route("/a") { connectRpc(ConnectServer(registry), GRACE.milliseconds) }
                    route("/b") { connectRpc(ConnectServer(registry), GRACE.milliseconds) }
                }
            },
        ).start(wait = false)
        val port = runBlocking { app.engine.resolvedConnectors().first().port }
        val sockets = listOf("a", "b").map { prefix ->
            Socket("127.0.0.1", port).also { socket ->
                val payload = envelope(0, "hello".toByteArray())
                socket.getOutputStream().write(
                    (
                        "POST /$prefix/test.v1.TestService/Hold HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/connect+proto\r\nContent-Length: ${payload.size}\r\n\r\n"
                        ).toByteArray() +
                        payload,
                )
                socket.getOutputStream().flush()
            }
        }
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
            val stopStarted = System.nanoTime()
            app.stop(0, 3_000, TimeUnit.MILLISECONDS)
            // Each handler is cancelled only once its grace period has run.
            // Were the grace periods consecutive, the later cancellation
            // would come at least GRACE after the earlier; a shared period
            // cancels both at its end.
            val offsetsMs = cancelledAt.map { TimeUnit.NANOSECONDS.toMillis(it - stopStarted) }
            assertThat(offsetsMs).hasSize(2).allSatisfy { assertThat(it).isGreaterThanOrEqualTo(GRACE) }
            assertThat(offsetsMs.max() - offsetsMs.min()).isLessThan(GRACE)
        } finally {
            sockets.forEach(Socket::close)
        }
    }

    private companion object {
        const val GRACE = 250L
    }
}
