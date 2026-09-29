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

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

/** Request metadata as handlers see it, response metadata as it reaches the wire. */
class MetadataTest {
    @Test
    fun requestHeaderNamesIgnoreCaseAndKeepEveryValue() {
        lateinit var seen: HandlerContext
        val server = server(
            unary { req, ctx ->
                seen = ctx
                req
            },
        )
        val exchange = FakeExchange(
            path = "/$SVC/Unary",
            requestHeaders = mapOf("Content-Type" to listOf("application/proto"), "X-Multi" to listOf("1", "2"), "x-multi" to listOf("3")),
        )
        exchange.bodyChunks.close()
        runBlocking { server.serve(exchange) }
        assertThat(seen.requestHeaders["X-MULTI"]).containsExactly("1", "2", "3")
        assertThat(seen.requestHeaders.containsKey("x-Multi")).isTrue()
        assertThat(seen.requestHeaders.keys).contains("x-multi").doesNotContain("X-Multi")
        assertThat(seen.requestHeaders).isEqualTo(mapOf("content-type" to listOf("application/proto"), "x-multi" to listOf("1", "2", "3")))
    }

    /**
     * Names must be RFC 9110 tokens and not reserved by a protocol; values
     * have CTLs other than HTAB replaced with SP (RFC 9110 §5.5), and values
     * with characters above U+007E are dropped (protocol.md ASCII-Value).
     */
    @Test
    fun responseMetadataRules() {
        val kept = mapOf(
            "x-token!#$%&'*+.^_`|~09" to listOf("v"),
            "X-Upper" to listOf("a"),
            "x-upper" to listOf("b"),
            "x-ctl" to listOf("a\tb\u0000c\u007fd\re\nf\u001bg"),
            "x-unicode" to listOf("latin-1 é", "ok", "emoji \uD83D\uDE00", "cjk \u4e2d"),
            "x-only-unicode" to listOf("\u00ff"),
            "x-empty-value" to listOf(""),
        )
        val dropped = listOf(
            "", "x a", "x:a", "x(a)", "é", "x-no-values",
            "content-type", "content-length", "content-encoding", "accept-encoding", "transfer-encoding", "trailer", "date",
            "connect-anything", "Connect-Timeout-Ms", "grpc-status", "Grpc-Message",
        )
        fun fill(target: MutableMap<String, MutableList<String>>) {
            kept.forEach { (k, v) -> target[k] = v.toMutableList() }
            dropped.forEach { target[it] = if (it == "x-no-values") mutableListOf() else mutableListOf("bad") }
        }
        val expected = mapOf(
            "x-token!#$%&'*+.^_`|~09" to listOf("v"),
            "x-upper" to listOf("a", "b"),
            "x-ctl" to listOf("a\tb c d e f g"),
            "x-unicode" to listOf("ok"),
            "x-empty-value" to listOf(""),
        )
        val server = server(
            unary { req, ctx ->
                fill(ctx.responseHeaders)
                fill(ctx.responseTrailers)
                req
            },
            serverStream { _, ctx, _ ->
                fill(ctx.responseHeaders)
                fill(ctx.responseTrailers)
            },
        )
        val unary = server.call("Unary", "application/proto", "a".toByteArray())
        val appHeaders = unary.headers.filterKeys { it.startsWith("x-") }
        val appTrailers = unary.headers.filterKeys { it.startsWith("trailer-") }.mapKeys { it.key.removePrefix("trailer-") }
        assertThat(appHeaders).isEqualTo(expected)
        assertThat(appTrailers).isEqualTo(expected)
        assertThat(unary.headers.keys - appHeaders.keys - appTrailers.keys.map { "trailer-$it" }.toSet()).containsExactlyInAnyOrder("content-type", "accept-encoding")

        for (protocol in Enveloped.values()) {
            val ex = server.callEnveloped(protocol, "ServerStream", listOf("a"))
            assertThat(ex.headers.filterKeys { it.startsWith("x-") }).describedAs("$protocol").isEqualTo(expected)
            assertThat(ex.headers["content-type"]).describedAs("$protocol").containsExactly(protocol.contentType)
            assertThat(protocol.outcome(ex).trailers).describedAs("$protocol").isEqualTo(expected)
        }
    }
}
