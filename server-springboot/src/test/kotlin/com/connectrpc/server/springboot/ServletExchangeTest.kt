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
import com.connectrpc.ConnectException
import jakarta.servlet.AsyncContext
import jakarta.servlet.ReadListener
import jakarta.servlet.ServletInputStream
import jakarta.servlet.ServletOutputStream
import jakarta.servlet.WriteListener
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.lang.reflect.Proxy
import java.util.Collections
import kotlin.time.Duration.Companion.seconds

class ServletExchangeTest {
    /**
     * Tomcat 11 HTTP/2 can answer one `isFinished()` with true while the request's final
     * DATA frame is buffered but unread (`Stream.java:1356` reads the buffer position
     * before the END_STREAM state that `Http2Parser.java:186-212` records after buffering).
     */
    @Test
    fun finishedReportedBeforeTheBufferedFinalFrameStillReadsIt() = runTest {
        val input = object : ServletInputStream() {
            private val body = Buffer().writeUtf8("{}")
            private var staleFinished = true

            override fun isFinished(): Boolean {
                if (staleFinished) {
                    staleFinished = false
                    return true
                }
                return body.exhausted()
            }

            override fun isReady() = !body.exhausted()

            override fun setReadListener(listener: ReadListener) = Unit

            override fun read(): Int = if (body.exhausted()) -1 else body.readByte().toInt() and 0xff

            override fun read(b: ByteArray, off: Int, len: Int): Int = body.read(b, off, len)
        }
        val exchange = ServletExchange(request(input), response(), fake<AsyncContext>(), "/test.v1.TestService/Unary")

        val sink = Buffer()
        while (exchange.readRequestBody(sink, Long.MAX_VALUE) != -1L) continue

        assertThat(sink.readUtf8()).isEqualTo("{}")
    }

    /**
     * Tomcat 11 HTTP/2 answers `isReady()` with false and never calls the listener when an
     * empty END_STREAM lands inside that `isReady()` (InputBuffer.java:250, Stream.java:1336-1337).
     */
    @Test
    fun endOfBodyInsideIsReadyEndsTheRead() = runTest {
        val input = object : ServletInputStream() {
            private var ended = false

            override fun isFinished() = ended

            override fun isReady(): Boolean {
                ended = true
                return false
            }

            override fun setReadListener(listener: ReadListener) = Unit

            override fun read(): Int = throw IllegalStateException("not ready")
        }
        val exchange = ServletExchange(request(input), response(), fake<AsyncContext>(), "/test.v1.TestService/Unary")

        assertThat(exchange.readRequestBody(Buffer(), Long.MAX_VALUE)).isEqualTo(-1L)
    }

    /**
     * Tomcat 11 HTTP/2 answers every `isReady()` with false, and never calls the listener, when
     * the final DATA frame lands inside the first one (Stream.java:1336-1337,1356); the frame
     * stays buffered and counted by `available()`.
     */
    @Test
    fun bufferedBodyTheContainerNeverReportsFailsTheCall() = runTest {
        val input = object : ServletInputStream() {
            override fun isFinished() = false

            override fun isReady() = false

            override fun available() = 2

            override fun setReadListener(listener: ReadListener) = Unit

            override fun read(): Int = throw IllegalStateException("not ready")
        }
        val exchange = ServletExchange(request(input), response(), fake<AsyncContext>(), "/test.v1.TestService/Unary")

        val failure = runCatching { exchange.readRequestBody(Buffer(), Long.MAX_VALUE) }.exceptionOrNull()

        assertThat(failure).isInstanceOf(ConnectException::class.java)
        assertThat((failure as ConnectException).code).isEqualTo(Code.UNAVAILABLE)
    }

    /** Bytes that arrive just after `isReady()` registered read interest are read once the container reports them. */
    @Test
    fun bufferedBodyReportedLaterIsRead() = runTest {
        val input = object : ServletInputStream() {
            private val body = Buffer().writeUtf8("{}")
            var listener: ReadListener? = null
            var reported = false

            override fun isFinished() = reported && body.exhausted()

            override fun isReady() = reported && !body.exhausted()

            override fun available() = body.size.toInt()

            override fun setReadListener(listener: ReadListener) {
                this.listener = listener
            }

            override fun read(): Int = if (body.exhausted()) -1 else body.readByte().toInt() and 0xff

            override fun read(b: ByteArray, off: Int, len: Int): Int = body.read(b, off, len)
        }
        val exchange = ServletExchange(request(input), response(), fake<AsyncContext>(), "/test.v1.TestService/Unary")
        exchange.start(Job())
        launch {
            delay(1.seconds)
            input.reported = true
            input.listener!!.onDataAvailable()
        }

        val sink = Buffer()
        while (exchange.readRequestBody(sink, Long.MAX_VALUE) != -1L) continue

        assertThat(sink.readUtf8()).isEqualTo("{}")
    }

    private fun response(): HttpServletResponse {
        val output = object : ServletOutputStream() {
            override fun isReady() = true

            override fun setWriteListener(listener: WriteListener) = Unit

            override fun write(b: Int) = Unit
        }
        return fake { if (it == "getOutputStream") output else null }
    }

    private fun request(input: ServletInputStream): HttpServletRequest = fake { name ->
        when (name) {
            "getMethod" -> "POST"
            "getProtocol" -> "HTTP/2.0"
            "getHeaderNames" -> Collections.emptyEnumeration<String>()
            "getInputStream" -> input
            else -> null
        }
    }

    private inline fun <reified T> fake(noinline answer: (String) -> Any? = { null }): T = Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, _ -> answer(method.name) } as T
}
