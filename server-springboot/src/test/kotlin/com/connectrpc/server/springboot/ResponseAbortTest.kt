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
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerObserver
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.catalina.core.StandardContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.tomcat.TomcatWebServer
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * A response that fails part way to a client that has stopped reading, here
 * because its handler throws an [Error] after the response started, is
 * aborted: Tomcat resets the HTTP/2 stream or closes the HTTP/1.1 connection
 * although it could not write what it holds, and the async request is
 * released.
 */
class ResponseAbortTest {
    companion object {
        private const val FLOOD = "test.v1.TestService/Flood"
        private const val FAIL_AFTER_MS = 300L

        // RFC 9113 §7.
        private const val INTERNAL_ERROR = 0x2

        private val ended = LinkedBlockingQueue<Code?>()

        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java, "server.http2.enabled=true")
    }

    @Before
    fun clear() = ended.clear()

    @Test
    fun http1ConnectionOfAClientThatStoppedReadingIsClosed() {
        Socket().use { socket ->
            // A small window, so the server's writes back up soon.
            socket.receiveBufferSize = 4096
            socket.connect(InetSocketAddress("127.0.0.1", server.port))
            socket.getOutputStream().write(
                "POST /$FLOOD HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/connect+proto\r\nContent-Length: 0\r\n\r\n".toByteArray(),
            )
            socket.getOutputStream().flush()
            awaitEnd()
            assertAsyncRequestsReleased()
            assertThat(readsToTheEnd(socket)).describedAs("the server closed the connection").isTrue()
        }
    }

    /**
     * The request stays open, so Tomcat would reset the stream on an ordinary
     * completion too, with NO_ERROR (Tomcat 11.0.22
     * `StreamProcessor.java:108-116`); the forced abort resets it with
     * INTERNAL_ERROR (`StreamProcessor.java:123-131`, `ErrorState.java:39`).
     */
    @Test
    fun http2StreamOfAClientThatStoppedReadingIsReset() {
        RawHttp2Connection(server.port).use { connection ->
            val stream = connection.openStream("/$FLOOD", "application/connect+proto")
            awaitEnd()
            assertAsyncRequestsReleased()
            assertThat(connection.awaitReset(stream, timeoutMs = 5_000)).describedAs("RST_STREAM error code").isEqualTo(INTERNAL_ERROR)
        }
    }

    private fun awaitEnd() {
        assertThat(ended.poll(5, TimeUnit.SECONDS)).isEqualTo(Code.CANCELED)
    }

    /** Tomcat counts each async request until it has completed (StandardContext.getInProgressAsyncCount). */
    private fun assertAsyncRequestsReleased() {
        val webServer = (server.context as WebServerApplicationContext).webServer as TomcatWebServer
        val context = webServer.tomcat.host.findChildren().single() as StandardContext
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
        while (context.inProgressAsyncCount != 0L && System.nanoTime() < deadline) Thread.sleep(10)
        assertThat(context.inProgressAsyncCount).isZero()
    }

    /** Reads until the connection ends; false if it is still open after five quiet seconds. */
    private fun readsToTheEnd(socket: Socket): Boolean {
        socket.soTimeout = 5_000
        val buffer = ByteArray(64 * 1024)
        return try {
            while (socket.getInputStream().read(buffer) != -1) continue
            true
        } catch (e: SocketTimeoutException) {
            false
        } catch (e: SocketException) {
            // Reset: the server closed with bytes the client had not read.
            true
        }
    }

    private class HandlerFailure : Error("handler failed")

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                // Sends until the client stops reading, and fails once it has.
                object : BidiStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec(FLOOD, TestMessage::class, TestMessage::class, StreamType.BIDI)

                    override suspend fun handle(stream: BidiStream<TestMessage, TestMessage>, ctx: HandlerContext) = coroutineScope {
                        launch {
                            delay(FAIL_AFTER_MS)
                            throw HandlerFailure()
                        }
                        val message = TestMessage(ByteArray(16 * 1024))
                        while (true) stream.send(message)
                    }
                },
            )
            .build()

        @Bean
        open fun connectRpcObserver() = ServerObserver { _, _, _ -> ServerObserver.Call { code -> ended.add(code) } }
    }
}
