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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Idempotency
import com.connectrpc.server.internal.parseConnectTimeout
import com.connectrpc.server.internal.parseGrpcTimeout
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import java.util.Random
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

class TimeoutTest {
    @Test
    fun connectTimeoutGrammar() {
        assertThat(parseConnectTimeout(null)).isNull()
        assertThat(parseConnectTimeout("")).isNull()
        assertThat(parseConnectTimeout(" \t ")).isNull()
        assertThat(parseConnectTimeout("1")).isEqualTo(1.milliseconds)
        assertThat(parseConnectTimeout("9999999999")).isEqualTo(9999999999.milliseconds)
        for (bad in listOf("0", "-5", "+5", "abc", "12345678901", "00000000001", "1.5", "1e3", "1 2")) {
            assertThatThrownBy { parseConnectTimeout(bad) }.describedAs(bad).isInstanceOf(ConnectException::class.java)
        }
        val random = Random(5)
        repeat(500) {
            val ms = 1 + (random.nextLong() and Long.MAX_VALUE) % 9_999_999_999L
            assertThat(parseConnectTimeout("$ms")).isEqualTo(ms.milliseconds)
            assertThat(parseConnectTimeout(" \t$ms\t ")).isEqualTo(ms.milliseconds)
            assertThat(parseConnectTimeout("$ms".padStart(10, '0'))).isEqualTo(ms.milliseconds)
        }
    }

    @Test
    fun grpcTimeoutGrammar() {
        assertThat(parseGrpcTimeout(null)).isNull()
        assertThat(parseGrpcTimeout("")).isNull()
        assertThat(parseGrpcTimeout("  ")).isNull()
        assertThat(parseGrpcTimeout("1n")).isEqualTo(1.nanoseconds)
        assertThat(parseGrpcTimeout("99999999H")).isEqualTo(99999999.hours)
        assertThat(parseGrpcTimeout("250m")).isEqualTo(250.milliseconds)
        for (bad in listOf("S", "5x", "0S", "00000000S", "-1S", "123456789m", "5", "1.5S", " m", "1h", "1s", "1U", "1N", "1 S", "1SS")) {
            assertThatThrownBy { parseGrpcTimeout(bad) }.describedAs(bad).isInstanceOf(ConnectException::class.java)
        }
        // PROTOCOL-HTTP2.md "Timeout": TimeoutValue is at most 8 digits; units H M S m u n.
        val units = mapOf('H' to 1.hours, 'M' to 1.minutes, 'S' to 1.seconds, 'm' to 1.milliseconds, 'u' to 1.microseconds, 'n' to 1.nanoseconds)
        val random = Random(6)
        repeat(500) {
            val amount = 1 + random.nextInt(99_999_999)
            val (unit, scale) = units.entries.elementAt(random.nextInt(units.size))
            assertThat(parseGrpcTimeout("$amount$unit")).describedAs("$amount$unit").isEqualTo(scale * amount)
        }
    }

    @Test
    fun invalidTimeoutsAreRejectedPerProtocol() {
        val server = server(unary { req, _ -> req }, serverStream { req, _, s -> s.send(req) })
        val connect = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Timeout-Ms" to "abc"))
        assertThat(connect.status).isEqualTo(400)
        assertThat(connect.body.readUtf8()).contains("invalid_argument")

        val stream = server.call("ServerStream", "application/connect+proto", env(0, "a"), mapOf("Connect-Timeout-Ms" to "0"))
        assertThat(stream.status).isEqualTo(200)
        assertThat(frames(stream.bodyBytes).single().text).contains("\"invalid_argument\"")

        val grpc = server.call("Unary", "application/grpc", env(0, "a"), mapOf("grpc-timeout" to "5x"))
        assertThat(grpc.headers["grpc-status"]).containsExactly("3")
        assertThat(grpc.bodyBytes).isEmpty()

        val web = server.call("Unary", "application/grpc-web", env(0, "a"), mapOf("grpc-timeout" to "123456789m"))
        assertThat(web.headers["grpc-status"]).containsExactly("3")
    }

    @Test
    fun emptyTimeoutHeadersMeanNoDeadline() {
        val server = server(unary { req, ctx -> Msg("${req.text}:${ctx.timeRemaining()}") })
        val connect = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Timeout-Ms" to ""))
        assertThat(connect.status).isEqualTo(200)
        assertThat(connect.body.readUtf8()).isEqualTo("a:null")
        val grpc = server.call("Unary", "application/grpc", env(0, "a"), mapOf("grpc-timeout" to " "))
        assertThat(grpc.trailers["grpc-status"]).containsExactly("0")
        assertThat(frames(grpc.bodyBytes).single().text).isEqualTo("a:null")
    }

    @Test
    fun connectIgnoresGrpcTimeout() {
        val server = server(
            unary { req, ctx ->
                delay(50)
                Msg("${req.text}:${ctx.timeRemaining()}")
            },
        )
        val ex = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Grpc-Timeout" to "1n"))
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.body.readUtf8()).isEqualTo("a:null")
    }

    /** The handler sees the time left of the client's timeout, counting down (connect-es `timeoutMs()`). */
    @Test
    fun timeRemainingCountsDownFromTheClientTimeout() {
        val server = server(
            unary { _, ctx ->
                val before = checkNotNull(ctx.timeRemaining())
                delay(100)
                val after = checkNotNull(ctx.timeRemaining())
                Msg("${before.inWholeMilliseconds} ${(before - after).inWholeMilliseconds}")
            },
        )
        val (before, elapsed) = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Timeout-Ms" to "5000"))
            .body.readUtf8().split(" ").map { it.toLong() }
        assertThat(before).isBetween(3_000, 5_000)
        assertThat(elapsed).isGreaterThanOrEqualTo(100)
    }

    /** maxTimeout replaces a longer or missing client timeout and leaves a shorter one alone. */
    @Test
    fun maxTimeoutClampsLongerAndMissingTimeouts() {
        val server = server(
            unary { req, ctx ->
                if (req.text == "report") return@unary Msg("${ctx.timeRemaining()?.inWholeMilliseconds}")
                delay(5_000)
                req
            },
            config = ServerConfig(maxTimeout = 200.milliseconds),
        )
        for (headers in listOf(emptyMap(), mapOf("Connect-Timeout-Ms" to "60000"))) {
            val start = System.nanoTime()
            val ex = server.call("Unary", "application/proto", "a".toByteArray(), headers)
            assertThat(ex.status).describedAs("$headers").isEqualTo(504)
            assertThat((System.nanoTime() - start) / 1_000_000).describedAs("$headers").isLessThan(2_000)
            assertThat(server.call("Unary", "application/proto", "report".toByteArray(), headers).body.readUtf8().toLong())
                .describedAs("$headers").isBetween(0, 200)
        }
        val grpc = server.call("Unary", "application/grpc", env(0, "a"), mapOf("grpc-timeout" to "1H"))
        assertThat(Enveloped.GRPC.outcome(grpc).code).isEqualTo(Code.DEADLINE_EXCEEDED)
        val shorter = server.call("Unary", "application/proto", "report".toByteArray(), mapOf("Connect-Timeout-Ms" to "50"))
        assertThat(shorter.body.readUtf8().toLong()).isBetween(0, 50)
    }

    @Test
    fun deadlineCoversHandler() {
        val server = server(
            unary { req, _ ->
                delay(5_000)
                req
            },
        )
        val start = System.nanoTime()
        val ex = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Timeout-Ms" to "100"))
        assertThat(ex.status).isEqualTo(504)
        assertThat(ex.body.readUtf8()).isEqualTo("""{"code":"deadline_exceeded","message":"deadline exceeded"}""")
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(2_000)
    }

    @Test
    fun deadlineCoversRequestReads() {
        val server = server(
            clientStream { stream, _ ->
                var n = 0
                while (stream.receive() != null) n++
                Msg("$n")
            },
        )
        val exchange = FakeExchange(
            path = "/$SVC/ClientStream",
            requestHeaders = mapOf("content-type" to listOf("application/connect+proto"), "connect-timeout-ms" to listOf("100")),
        )
        // The client sends one message and then stalls without ending the body.
        exchange.bodyChunks.trySend(env(0, "a"))
        runBlocking { server.serve(exchange) }
        assertThat(exchange.status).isEqualTo(200)
        assertThat(frames(exchange.bodyBytes).single().text).contains("\"deadline_exceeded\"")
    }

    /** A GET body that never ends is bounded by the client timeout and by maxTimeout. */
    @Test
    fun deadlineCoversGetBodyProbe() {
        val get = unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req }
        for ((config, headers) in listOf(
            ServerConfig(maxTimeout = 100.milliseconds) to emptyMap(),
            ServerConfig() to mapOf("connect-timeout-ms" to listOf("100")),
        )) {
            // The body channel stays open: the client started a body and sends nothing.
            val exchange = FakeExchange(method = "GET", path = "/$SVC/Get", rawQuery = "encoding=proto&message=a", requestHeaders = headers)
            runBlocking { withTimeout(2_000) { server(get, config = config).serve(exchange) } }
            assertThat(exchange.status).describedAs("$headers").isEqualTo(504)
            assertThat(exchange.header("content-type")).describedAs("$headers").isEqualTo("application/json")
            assertThat(exchange.body.readUtf8()).describedAs("$headers").isEqualTo("""{"code":"deadline_exceeded","message":"deadline exceeded"}""")
        }
    }

    /** The GET body probe and the handler share one deadline: time spent waiting for the body counts. */
    @Test
    fun getBodyProbeSpendsTheCallDeadline() {
        val server = server(unary("Get", Idempotency.NO_SIDE_EFFECTS) { _, ctx -> Msg("${ctx.timeRemaining()!!.inWholeMilliseconds}") })
        val exchange = FakeExchange(
            method = "GET",
            path = "/$SVC/Get",
            rawQuery = "encoding=proto&message=a",
            requestHeaders = mapOf("connect-timeout-ms" to listOf("1000")),
        )
        runBlocking {
            launch {
                delay(400)
                exchange.bodyChunks.close()
            }
            server.serve(exchange)
        }
        assertThat(exchange.status).isEqualTo(200)
        assertThat(exchange.body.readUtf8().toLong()).isBetween(0, 600)
    }

    /**
     * A handler that blocks through its deadline: the call waits for it and
     * then ends with `deadline_exceeded` in the protocol's end of stream,
     * after the messages already sent, whether or not one was. `suspendsAfter`
     * blocks on another dispatcher and sends again, which observes the
     * cancellation; otherwise the handler blocks its own thread, so the
     * timeout cannot cancel it, and returns without suspending.
     */
    @Test
    fun blockingHandlerEndsWithDeadlineExceededOnceItReturns() {
        for (sendFirst in listOf(true, false)) {
            for (suspendsAfter in listOf(true, false)) {
                val case = "sendFirst=$sendFirst suspendsAfter=$suspendsAfter"
                val codes = mutableListOf<Code?>()
                val server = server(
                    serverStream { req, _, stream ->
                        if (sendFirst) stream.send(req)
                        if (suspendsAfter) {
                            withContext(Dispatchers.IO) { Thread.sleep(300) }
                            stream.send(Msg("late"))
                        } else {
                            Thread.sleep(300)
                        }
                    },
                    config = ServerConfig(maxTimeout = 50.milliseconds, observer = { _, _, _ -> ServerObserver.Call { codes += it } }),
                )
                for (protocol in Enveloped.entries) {
                    val start = System.nanoTime()
                    val exchange = server.callEnveloped(protocol, "ServerStream", listOf("a"))
                    assertThat((System.nanoTime() - start) / 1_000_000).describedAs("$case $protocol").isGreaterThanOrEqualTo(300)
                    assertThat(exchange.status).describedAs("$case $protocol").isEqualTo(200)
                    val outcome = protocol.outcome(exchange)
                    assertThat(outcome.messages).describedAs("$case $protocol").isEqualTo(if (sendFirst) listOf("a") else emptyList())
                    assertThat(outcome.code).describedAs("$case $protocol").isEqualTo(Code.DEADLINE_EXCEEDED)
                }
                assertThat(codes).describedAs(case).containsOnly(Code.DEADLINE_EXCEEDED).hasSize(Enveloped.entries.size)
            }
        }
    }

    /** A unary handler that blocks the thread through its deadline and returns: its result is discarded. */
    @Test
    fun blockingUnaryHandlerReturningLateEndsWithDeadlineExceeded() {
        val codes = mutableListOf<Code?>()
        val server = server(
            unary { req, _ ->
                Thread.sleep(300)
                req
            },
            config = ServerConfig(maxTimeout = 50.milliseconds, observer = { _, _, _ -> ServerObserver.Call { codes += it } }),
        )
        val start = System.nanoTime()
        val response = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat((System.nanoTime() - start) / 1_000_000).isGreaterThanOrEqualTo(300)
        assertThat(response.status).isEqualTo(504)
        assertThat(response.body.readUtf8()).isEqualTo("""{"code":"deadline_exceeded","message":"deadline exceeded"}""")
        val grpc = server.call("Unary", "application/grpc", env(0, "a"))
        assertThat(Enveloped.GRPC.outcome(grpc).code).isEqualTo(Code.DEADLINE_EXCEEDED)
        assertThat(codes).containsExactly(Code.DEADLINE_EXCEEDED, Code.DEADLINE_EXCEEDED)
    }

    /**
     * A handler error thrown after the deadline, by a handler that blocked
     * its thread through it, ends with `deadline_exceeded`; the same error
     * thrown before the deadline keeps its own code.
     */
    @Test
    fun handlerErrorAfterDeadlineEndsWithDeadlineExceeded() {
        val codes = mutableListOf<Code?>()
        fun fail(text: String): Nothing {
            if (text.startsWith("late")) Thread.sleep(300)
            if (text.endsWith("runtime")) throw IllegalStateException("boom")
            throw ConnectException(Code.UNAVAILABLE, "handler error")
        }
        val server = server(
            unary { req, _ -> fail(req.text) },
            serverStream { req, _, _ -> fail(req.text) },
            config = ServerConfig(maxTimeout = 50.milliseconds, observer = { _, _, _ -> ServerObserver.Call { codes += it } }),
        )
        val cases = listOf(
            "late" to Code.DEADLINE_EXCEEDED,
            "late-runtime" to Code.DEADLINE_EXCEEDED,
            "early" to Code.UNAVAILABLE,
            "early-runtime" to Code.UNKNOWN,
        )
        for ((text, expected) in cases) {
            codes.clear()
            val unary = server.call("Unary", "application/proto", text.toByteArray())
            assertThat(parseJson(unary.body.readUtf8())["code"]).describedAs(text).isEqualTo(expected.codeName)
            for (protocol in Enveloped.entries) {
                val exchange = server.callEnveloped(protocol, "ServerStream", listOf(text))
                assertThat(protocol.outcome(exchange).code).describedAs("$text $protocol").isEqualTo(expected)
            }
            assertThat(codes).describedAs(text).containsOnly(expected).hasSize(1 + Enveloped.entries.size)
        }
    }
}
