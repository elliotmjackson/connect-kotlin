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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.Random

/**
 * Stream lifecycles, each checked on Connect streaming, gRPC and gRPC-Web as
 * that protocol's client reads the response.
 */
class StreamOutcomesTest {
    private val server = server(
        serverStream("Zero") { _, _, _ -> },
        serverStream("FailFirst") { _, ctx, _ ->
            ctx.responseHeaders["x-h"] = mutableListOf("h")
            throw ConnectException(Code.ABORTED, "boom")
        },
        serverStream("FailLate") { req, ctx, s ->
            repeat(req.text.toInt()) { s.send(Msg("m$it")) }
            ctx.responseTrailers["x-t"] = mutableListOf("t")
            throw ConnectException(Code.DATA_LOSS, "late").withMetadata(
                mapOf("x-e" to listOf("e"), "grpc-status" to listOf("0"), "content-type" to listOf("text/html")),
            )
        },
        serverStream("Crash") { _, _, s ->
            s.send(Msg("m"))
            throw IllegalStateException("secret")
        },
        serverStream("SelfCancel") { _, _, _ -> throw CancellationException("handler gave up") },
        serverStream("Slow") { _, _, s ->
            s.send(Msg("before"))
            delay(10_000)
        },
        clientStream { s, _ ->
            val all = mutableListOf<String>()
            while (true) all += (s.receive() ?: break).text
            Msg("${all.size}:${all.joinToString(",")}")
        },
        bidi("IgnoreRequests") { s, _ -> s.send(Msg("hi")) },
    )

    private fun assertAllProtocols(procedure: String, request: List<String>, expected: Outcome, headers: Map<String, String> = emptyMap()) {
        for (protocol in Enveloped.values()) {
            val ex = server.callEnveloped(protocol, procedure, request, headers)
            assertThat(ex.status).describedAs("$protocol $procedure").isEqualTo(200)
            assertThat(protocol.outcome(ex)).describedAs("$protocol $procedure").isEqualTo(expected)
        }
    }

    @Test
    fun streamWithoutMessagesSucceeds() = assertAllProtocols("Zero", listOf("a"), Outcome(emptyList(), null, null, emptyMap()))

    @Test
    fun errorBeforeTheFirstMessageKeepsResponseHeaders() {
        assertAllProtocols("FailFirst", listOf("a"), Outcome(emptyList(), Code.ABORTED, "boom", emptyMap()))
        for (protocol in Enveloped.values()) {
            assertThat(server.callEnveloped(protocol, "FailFirst", listOf("a")).headers["x-h"]).describedAs("$protocol").containsExactly("h")
        }
    }

    @Test
    fun errorAfterMessagesFollowsThemWithTrailersAndErrorMetadata() {
        for (n in listOf(1, 3, 50)) {
            assertAllProtocols(
                "FailLate",
                listOf("$n"),
                Outcome(List(n) { "m$it" }, Code.DATA_LOSS, "late", mapOf("x-t" to listOf("t"), "x-e" to listOf("e"))),
            )
        }
    }

    @Test
    fun unexpectedExceptionIsUnknownWithoutDetail() = assertAllProtocols("Crash", listOf("a"), Outcome(listOf("m"), Code.UNKNOWN, null, emptyMap()))

    @Test
    fun cancellationThrownByTheHandlerIsCanceled() = assertAllProtocols("SelfCancel", listOf("a"), Outcome(emptyList(), Code.CANCELED, "canceled", emptyMap()))

    @Test
    fun deadlineEndsTheStreamAfterSentMessages() {
        for (protocol in Enveloped.values()) {
            // Each protocol reads only its own deadline header.
            val headers = if (protocol == Enveloped.CONNECT) {
                mapOf("connect-timeout-ms" to "50", "grpc-timeout" to "1H")
            } else {
                mapOf("grpc-timeout" to "50m", "connect-timeout-ms" to "3600000")
            }
            assertThat(protocol.outcome(server.callEnveloped(protocol, "Slow", listOf("a"), headers))).describedAs("$protocol")
                .isEqualTo(Outcome(listOf("before"), Code.DEADLINE_EXCEEDED, "deadline exceeded", emptyMap()))
        }
    }

    @Test
    fun clientStreamWithZeroOrManyMessages() {
        assertAllProtocols("ClientStream", emptyList(), Outcome(listOf("0:"), null, null, emptyMap()))
        assertAllProtocols("ClientStream", List(100) { "$it" }, Outcome(listOf("100:" + List(100) { "$it" }.joinToString(",")), null, null, emptyMap()))
        // Zero-length messages are messages.
        assertAllProtocols("ClientStream", listOf("", ""), Outcome(listOf("2:,"), null, null, emptyMap()))
    }

    @Test
    fun handlerMayFinishWithoutReadingRequests() = assertAllProtocols("IgnoreRequests", listOf("a", "b"), Outcome(listOf("hi"), null, null, emptyMap()))

    /** Envelopes need not align with the transport's reads (connect-rust `service.rs:5900`). */
    @Test
    fun envelopesSplitAtAnyByte() {
        val random = Random(7)
        repeat(100) {
            val messages = List(random.nextInt(6)) { i -> "m$i" + "x".repeat(random.nextInt(40)) }
            val body = Buffer().apply { messages.forEach { write(env(0, it)) } }.readByteArray()
            val protocol = Enveloped.values()[random.nextInt(3)]
            val exchange = FakeExchange(path = "/$SVC/ClientStream", requestHeaders = mapOf("content-type" to listOf(protocol.contentType)))
            var offset = 0
            while (offset < body.size) {
                val n = 1 + random.nextInt(minOf(16, body.size - offset))
                exchange.bodyChunks.trySend(body.copyOfRange(offset, offset + n))
                offset += n
            }
            exchange.bodyChunks.close()
            runBlocking { server.serve(exchange) }
            assertThat(protocol.outcome(exchange).messages).describedAs("$protocol $messages")
                .containsExactly("${messages.size}:${messages.joinToString(",")}")
        }
    }

    @Test
    fun compressedMessagesInBothDirections() {
        val big = "y".repeat(5000)
        val echo = server(
            bidi { s, _ ->
                while (true) s.send(s.receive() ?: break)
            },
        )
        for (protocol in Enveloped.values()) {
            val acceptHeader = if (protocol == Enveloped.CONNECT) "connect-accept-encoding" else "grpc-accept-encoding"
            // Per-message compression flag: a compressed and an uncompressed message in one request.
            val body = env(1, gzip(big.toByteArray())) + env(0, "small")
            val ex = echo.call("Bidi", protocol.contentType, body, mapOf(protocol.encodingHeader to "gzip", acceptHeader to "gzip"))
            assertThat(ex.headers[protocol.encodingHeader]).describedAs("$protocol").containsExactly("gzip")
            val data = frames(ex.bodyBytes).filter { (it.flags and 0x82) == 0 }
            // Messages below compressMinBytes are sent uncompressed.
            assertThat(data.map { it.flags }).describedAs("$protocol").containsExactly(1, 0)
            assertThat(gunzip(data[0].payload)).isEqualTo(big)
            assertThat(data[1].text).isEqualTo("small")
        }
    }

    @Test
    fun handlerCanRecoverFromTheSendLimit() {
        val limited = server(
            serverStream { _, _, s ->
                val error = runCatching { s.send(Msg("x".repeat(100))) }.exceptionOrNull() as ConnectException
                s.send(Msg("${error.code}"))
            },
            config = ServerConfig(sendMaxBytes = 50),
        )
        for (protocol in Enveloped.values()) {
            assertThat(protocol.outcome(limited.callEnveloped(protocol, "ServerStream", listOf("a"))))
                .describedAs("$protocol").isEqualTo(Outcome(listOf("RESOURCE_EXHAUSTED"), null, null, emptyMap()))
        }
    }
}
