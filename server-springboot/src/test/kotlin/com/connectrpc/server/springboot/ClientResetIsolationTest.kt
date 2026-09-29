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
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import kotlinx.coroutines.delay
import org.apache.catalina.connector.Connector
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.tomcat.TomcatConnectorCustomizer
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import java.io.InputStream
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * A handler write that fails because its client reset the connection must not
 * fail a later request that Tomcat serves with the same pooled objects.
 */
class ClientResetIsolationTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java)

        /** A connector added with `addAdditionalConnectors`, which connector customizers never reach. */
        private val additionalConnector = Connector().apply {
            port = 0
            setProperty("address", "127.0.0.1")
            protocolHandler.executor = HandlerThreadWaitsExecutor()
        }

        /** Completed when a handler thread returns from work it handed to Tomcat. */
        @Volatile
        private var handlerThreadResumed = CompletableFuture<Unit>()
    }

    @Test
    fun http1ResetDuringHandlerWriteDoesNotFailTheNextRequest() {
        assertResetDoesNotFailTheNextRequest(server.port)
    }

    @Test
    fun http1ResetOnAnAdditionalConnectorDoesNotFailTheNextRequest() {
        assertResetDoesNotFailTheNextRequest(additionalConnector.localPort)
    }

    private fun assertResetDoesNotFailTheNextRequest(port: Int) {
        repeat(5) {
            handlerThreadResumed = CompletableFuture()
            Socket("127.0.0.1", port).use { socket ->
                socket.setSoLinger(true, 0)
                socket.soTimeout = 5_000
                val body = envelope(0, ByteArray(0))
                socket.getOutputStream().write(
                    (
                        "POST /test.v1.TestService/Tick HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/connect+proto\r\nContent-Length: ${body.size}\r\n\r\n"
                        ).toByteArray() +
                        body,
                )
                // Response headers: the stream is running.
                socket.getInputStream().readUntil("\r\n\r\n")
            }
            // The handler's next write fails; Tomcat handles that failure on a container thread.
            handlerThreadResumed.get(5, TimeUnit.SECONDS)

            Socket("127.0.0.1", port).use { socket ->
                socket.soTimeout = 5_000
                socket.getOutputStream().write(
                    (
                        "POST /test.v1.TestService/Unary HTTP/1.1\r\nHost: localhost\r\n" +
                            "Content-Type: application/proto\r\nContent-Length: 4\r\nConnection: close\r\n\r\nping"
                        ).toByteArray(),
                )
                val response = socket.getInputStream().readBytes().toString(Charsets.ISO_8859_1)
                assertThat(response).startsWith("HTTP/1.1 200").endsWith("pong:ping")
            }
        }
    }

    /** Reads until [marker] appears; returns everything read, as Latin-1. */
    private fun InputStream.readUntil(marker: String): String {
        val out = StringBuilder()
        while (!out.contains(marker)) {
            val b = read()
            if (b < 0) break
            out.append(b.toChar())
        }
        return out.toString()
    }

    /**
     * Runs Tomcat's work on its own threads, but makes a handler thread that hands
     * work to Tomcat wait until that work is done: the schedule in which the handler
     * thread is descheduled right after Tomcat dispatches its write failure to a
     * container thread (Tomcat 11.0.22 `AbstractProcessor.java:404-411`).
     */
    private class HandlerThreadWaitsExecutor : Executor {
        private val pool = Executors.newCachedThreadPool { task -> Thread(task, "test-tomcat-exec").apply { isDaemon = true } }

        override fun execute(task: Runnable) {
            val done = pool.submit(task)
            if (Thread.currentThread().name.startsWith("DefaultDispatcher-worker")) {
                done.get()
                handlerThreadResumed.complete(Unit)
            }
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun handlerThreadWaitsExecutor() = TomcatConnectorCustomizer { connector -> connector.protocolHandler.executor = HandlerThreadWaitsExecutor() }

        @Bean
        open fun additionalConnector() = WebServerFactoryCustomizer<TomcatServletWebServerFactory> { it.addAdditionalConnectors(additionalConnector) }

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
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Tick", TestMessage::class, TestMessage::class, StreamType.SERVER)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        while (true) {
                            stream.send(TestMessage(ByteArray(0)))
                            delay(10)
                        }
                    }
                },
            )
            .build()
    }
}
