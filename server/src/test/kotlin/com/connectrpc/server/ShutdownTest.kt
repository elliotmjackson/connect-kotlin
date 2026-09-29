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
import com.connectrpc.server.http.HttpExchange
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import kotlin.time.Duration.Companion.milliseconds

class ShutdownTest {
    /** Procedures whose handlers have started. */
    private val entered = Channel<String>(Channel.UNLIMITED)

    /** Handlers send `first` where they can and wait until cancelled, except Unary with `quick`, which returns after 200 ms. */
    private val server = server(
        unary { r, _ ->
            entered.send("Unary")
            if (r.text == "quick") {
                delay(200)
                return@unary r
            }
            awaitCancellation()
        },
        serverStream { _, _, s ->
            s.send(Msg("first"))
            entered.send("ServerStream")
            awaitCancellation()
        },
        clientStream { _, _ ->
            entered.send("ClientStream")
            awaitCancellation()
        },
        bidi { s, _ ->
            s.send(Msg("first"))
            entered.send("Bidi")
            awaitCancellation()
        },
    )

    /** A call of [procedure]; client and bidi streams keep their request body open, as a client still sending. */
    private fun exchange(procedure: String, contentType: String, body: ByteArray): FakeExchange = FakeExchange(
        path = "/$SVC/$procedure",
        requestHeaders = mapOf("content-type" to listOf(contentType)),
    ).also {
        it.bodyChunks.trySend(body)
        if (procedure == "Unary" || procedure == "ServerStream") it.bodyChunks.close()
    }

    private val streams = listOf("ServerStream", "ClientStream", "Bidi").flatMap { procedure -> Enveloped.values().map { procedure to it } }

    @Test
    fun callsInFlightEndWithTheShutdownError() = runBlocking<Unit> {
        val unary = exchange("Unary", "application/proto", "a".toByteArray())
        val calls = streams.map { (procedure, protocol) -> exchange(procedure, protocol.contentType, env(0, "a")) }
        val jobs = (calls + unary).map { launch { server.serve(it) } }
        repeat(jobs.size) { entered.receive() }
        server.shutdown()
        jobs.forEach { it.join() }
        assertThat(unary.status).isEqualTo(503)
        assertThat(parseJson(unary.body.readUtf8())).isEqualTo(mapOf("code" to "unavailable", "message" to "server is shutting down"))
        for ((call, case) in calls.zip(streams)) {
            val (procedure, protocol) = case
            val expected = if (procedure == "ClientStream") emptyList() else listOf("first")
            assertThat(protocol.outcome(call)).describedAs("$procedure $protocol")
                .isEqualTo(Outcome(expected, Code.UNAVAILABLE, "server is shutting down", emptyMap()))
        }
    }

    @Test
    fun callsAfterShutdownFailBeforeTheirRequestIsRead() = runBlocking<Unit> {
        server.shutdown(error = ConnectException(Code.ABORTED, "moved"))
        val unary = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(unary.status).isEqualTo(409)
        assertThat(unary.bytesRead.get()).isZero()
        for ((procedure, protocol) in streams) {
            val call = server.callEnveloped(protocol, procedure, listOf("a"))
            assertThat(protocol.outcome(call).code).describedAs("$procedure $protocol").isEqualTo(Code.ABORTED)
            assertThat(call.bytesRead.get()).describedAs("$procedure $protocol").isZero()
        }
        assertThat(entered.tryReceive().getOrNull()).isNull()
    }

    /** Calls that end within the grace period succeed; the rest are cancelled when it ends. */
    @Test
    fun gracePeriodLetsCallsFinish() = runBlocking<Unit> {
        val quick = exchange("Unary", "application/proto", "quick".toByteArray())
        val stream = exchange("ServerStream", Enveloped.GRPC.contentType, env(0, "a"))
        val jobs = listOf(quick, stream).map { launch { server.serve(it) } }
        repeat(jobs.size) { entered.receive() }
        server.shutdown(gracePeriod = 1_000.milliseconds)
        jobs.forEach { it.join() }
        assertThat(quick.status).isEqualTo(200)
        assertThat(quick.body.readUtf8()).isEqualTo("quick")
        assertThat(Enveloped.GRPC.outcome(stream).code).isEqualTo(Code.UNAVAILABLE)
    }

    /** A GET whose client started a body and sends nothing: its probe for the body is in flight like a handler. */
    @Test
    fun shutdownEndsAGetBodyProbe() = runBlocking<Unit> {
        val (getServer, probing, exchange) = stalledGet()
        val call = launch { getServer.serve(exchange) }
        probing.receive()
        getServer.shutdown()
        withTimeout(2_000) { call.join() }
        assertThat(exchange.fake.status).isEqualTo(503)
        assertThat(parseJson(exchange.fake.body.readUtf8())).isEqualTo(mapOf("code" to "unavailable", "message" to "server is shutting down"))
    }

    @Test
    fun getAfterShutdownFailsBeforeItsBodyIsRead() = runBlocking<Unit> {
        val (getServer, probing, exchange) = stalledGet()
        getServer.shutdown(error = ConnectException(Code.ABORTED, "moved"))
        withTimeout(2_000) { getServer.serve(exchange) }
        assertThat(exchange.fake.status).isEqualTo(409)
        assertThat(probing.tryReceive().getOrNull()).isNull()
    }

    /** A server with a GET procedure, a channel told of each body read, and a GET whose body never arrives. */
    private fun stalledGet(): Triple<ConnectServer, Channel<Unit>, ProbedExchange> {
        val getServer = server(unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req })
        val probing = Channel<Unit>(Channel.UNLIMITED)
        val fake = FakeExchange(method = "GET", path = "/$SVC/Get", rawQuery = "encoding=proto&message=a")
        return Triple(getServer, probing, ProbedExchange(fake, probing))
    }

    private class ProbedExchange(val fake: FakeExchange, private val probing: Channel<Unit>) : HttpExchange by fake {
        override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
            probing.send(Unit)
            return fake.readRequestBody(sink, maxBytes)
        }
    }
}
