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
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ServerObserver
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.netty.NettyApplicationCall
import io.netty.channel.Channel
import io.netty.handler.codec.http2.Http2StreamChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * A response that fails part way, here because its handler throws an
 * [Error] after the response started, is aborted: the adapter resets the
 * HTTP/2 stream or closes the HTTP/1.1 connection, so the client does not
 * wait for the rest and Netty frees what it could not send.
 */
class ResponseAbortTest {
    // Each observed call end, wrapped because success ends with a null code.
    private val ended = ArrayBlockingQueue<Ended>(1)

    // The Netty channel of the call: its HTTP/2 stream or HTTP/1.1 connection.
    private val callChannel = AtomicReference<Channel>()

    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            // Sends until the client stops reading, and fails once it has.
            bidi(FLOOD) { stream ->
                coroutineScope {
                    launch {
                        delay(FAIL_AFTER_MS)
                        throw HandlerFailure()
                    }
                    val message = TestMessage(ByteArray(16 * 1024))
                    while (true) stream.send(message)
                }
            },
        )
        .register(
            bidi(SEND_ONE) { stream ->
                stream.send(TestMessage("a"))
                delay(FAIL_AFTER_MS)
                throw HandlerFailure()
            },
        )
        .register(bidi(SEND_ONE_OK) { stream -> stream.send(TestMessage("a")) })
        .build()

    private val config = ServerConfig(observer = { _, _, _ -> ServerObserver.Call { code -> ended.add(Ended(code)) } })

    @Test
    fun http1ConnectionOfAClientThatStoppedReadingIsClosed() {
        TestServer.start(registry, config, module = { captureCallChannel() }).use { server ->
            Socket().use { socket ->
                // A small window, so the server's writes back up soon.
                socket.receiveBufferSize = 4096
                socket.connect(InetSocketAddress("127.0.0.1", server.port))
                socket.getOutputStream().write(
                    "POST /$FLOOD HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/connect+proto\r\nContent-Length: 0\r\n\r\n".toByteArray(),
                )
                socket.getOutputStream().flush()
                awaitEnd()
                assertThat(callChannel.get().closeFuture().await(2, TimeUnit.SECONDS)).isTrue()
            }
        }
    }

    /**
     * The reset is queued behind a connection socket the client has let fill
     * up, and the stream still closes without closing its connection: once
     * the client drains the socket, the connection serves another stream.
     */
    @Test
    fun http2StreamIsClosedWhileTheConnectionSocketIsFull() {
        TestServer.start(registry, config, withH2c = true, module = { captureCallChannel() }).use { server ->
            RawHttp2Connection(server.port, receiveBufferSize = 4096, window = 16 * 1024 * 1024).use { connection ->
                connection.openStream("/$FLOOD", "application/connect+proto")
                awaitEnd()
                val stream = callChannel.get()
                assertThat(stream).isInstanceOf(Http2StreamChannel::class.java)
                assertThat(stream.closeFuture().await(2, TimeUnit.SECONDS)).isTrue()
                assertThat(stream.parent().isActive).isTrue()
                val next = connection.openStream("/$SEND_ONE_OK", "application/connect+proto", endStream = true)
                val response = connection.awaitResponse(next, timeoutMs = 10_000)
                assertThat(response).isNotNull()
                // HPACK static table index 8 is `:status: 200` (RFC 7541 Appendix A).
                assertThat(response!!.headerBlock.first()).isEqualTo(0x88.toByte())
                val envelopes = envelopes(response.data)
                assertThat(envelopes.map { it.first }).containsExactly(0, FLAG_END_STREAM)
                assertThat(envelopes.last().second.decodeToString()).doesNotContain("\"error\"")
                assertThat(ended.poll(5, TimeUnit.SECONDS)).isEqualTo(Ended(null))
            }
        }
    }

    /**
     * A stream whose request END_STREAM arrived before the failure: the
     * client sees it reset, promptly, and the connection carries on.
     *
     * This does not isolate the adapter's explicit RST_STREAM: the response
     * has not ended, so closing the stream channel resets it too
     * (netty-codec-http2 4.2.17 `AbstractHttp2StreamChannel.java:733-741`).
     * Closing sends no reset once Ktor has also handed Netty the response's
     * END_STREAM. For a failed body channel Ktor does so only when its body
     * loop sees the channel closed at its `isClosedForRead` check and falls
     * through to the end of stream (ktor-server-netty 3.6.0
     * `cio/NettyHttpResponsePipeline.kt:348, 380-381`; ktor-io
     * `ByteChannel.kt:88-89`). When the failure instead reaches the loop
     * suspended in `awaitContent`, which rethrows the cause, Ktor closes the
     * stream without END_STREAM (`NettyHttpResponsePipeline.kt:277`,
     * `respondWithFailure` at :123-141). In the first case only the explicit
     * reset keeps a response end still queued from reaching the client;
     * which case occurs is a race on the event loop that the adapter gives a
     * test no way to force.
     */
    @Test
    fun http2StreamWhoseRequestEndedIsReset() {
        TestServer.start(registry, config, withH2c = true, module = { captureCallChannel() }).use { server ->
            RawHttp2Connection(server.port).use { connection ->
                val streamId = connection.openStream("/$SEND_ONE", "application/connect+proto", endStream = true)
                assertThat(connection.awaitFrame(RST_STREAM, streamId, timeoutMs = 5_000)).isTrue()
                awaitEnd()
                val stream = callChannel.get()
                assertThat(stream.closeFuture().await(2, TimeUnit.SECONDS)).isTrue()
                assertThat(stream.parent().isActive).isTrue()
            }
        }
    }

    private fun Application.captureCallChannel() {
        intercept(ApplicationCallPipeline.Setup) {
            callChannel.set((call as NettyApplicationCall).context.channel())
        }
    }

    private fun awaitEnd() {
        assertThat(ended.poll(5, TimeUnit.SECONDS)).isEqualTo(Ended(Code.CANCELED))
    }

    /** Splits Connect streaming envelopes into their flags and payloads. */
    private fun envelopes(data: ByteArray): List<Pair<Int, ByteArray>> {
        val result = mutableListOf<Pair<Int, ByteArray>>()
        var offset = 0
        while (offset < data.size) {
            val size = (4 downTo 1).fold(0) { acc, i -> acc or (data[offset + i].toInt() and 0xff shl (8 * (4 - i))) }
            result += (data[offset].toInt() and 0xff) to data.copyOfRange(offset + 5, offset + 5 + size)
            offset += 5 + size
        }
        return result
    }

    private data class Ended(val code: Code?)

    private fun bidi(procedure: String, body: suspend (BidiStream<TestMessage, TestMessage>) -> Unit) = object : BidiStreamHandler<TestMessage, TestMessage> {
        override val methodSpec = MethodSpec(procedure, TestMessage::class, TestMessage::class, StreamType.BIDI)

        override suspend fun handle(stream: BidiStream<TestMessage, TestMessage>, ctx: HandlerContext) = body(stream)
    }

    private class HandlerFailure : Error("handler failed")

    private companion object {
        const val FLOOD = "test.v1.TestService/Flood"
        const val SEND_ONE = "test.v1.TestService/SendOne"
        const val SEND_ONE_OK = "test.v1.TestService/SendOneOk"
        const val FAIL_AFTER_MS = 300L
        const val RST_STREAM = 0x3
        const val FLAG_END_STREAM = 0x2
    }
}
