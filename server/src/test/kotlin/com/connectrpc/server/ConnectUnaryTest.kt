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
import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ConnectException
import okio.ByteString.Companion.encodeUtf8
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class ConnectUnaryTest {
    @Test
    fun successWire() {
        val server = server(
            unary { req, ctx ->
                ctx.responseHeaders["x-h"] = mutableListOf("h1", "h2")
                ctx.responseTrailers["x-t"] = mutableListOf("t")
                Msg("echo:${req.text}:${ctx.requestHeaders["X-IN"]}")
            },
        )
        val ex = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("x-in" to "v"))
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.headers).containsEntry("content-type", listOf("application/proto"))
            .containsEntry("accept-encoding", listOf("gzip,deflate"))
            .containsEntry("x-h", listOf("h1", "h2"))
            .containsEntry("trailer-x-t", listOf("t"))
        assertThat(ex.body.readUtf8()).isEqualTo("echo:a:[v]")
    }

    /** Error metadata is sent as headers, handler trailers keep their `trailer-` prefix. */
    @Test
    fun errorJsonCarriesDetailsMetadataTrailersAndEscapes() {
        val server = server(
            unary { _, ctx ->
                ctx.responseHeaders["x-h"] = mutableListOf("h")
                ctx.responseTrailers["x-t"] = mutableListOf("t")
                throw ConnectException(Code.RESOURCE_EXHAUSTED, "50% \"ünï\"\n")
                    .withErrorDetails(BytesStrategy("proto").errorDetailParser(), listOf(ConnectErrorDetail("type.googleapis.com/pkg.Info", "ab".encodeUtf8())))
                    .withMetadata(mapOf("retry-after" to listOf("30")))
            },
        )
        val ex = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(ex.status).isEqualTo(429)
        assertThat(ex.headers).containsEntry("x-h", listOf("h")).containsEntry("trailer-x-t", listOf("t"))
            .containsEntry("retry-after", listOf("30")).doesNotContainKey("trailer-retry-after")
        assertThat(ex.body.readUtf8()).isEqualTo(
            """{"code":"resource_exhausted","message":"50% \"ünï\"\n","details":[{"type":"pkg.Info","value":"YWI"}]}""",
        )
    }

    @Test
    fun nonConnectExceptionsBecomeUnknownWithoutMessage() {
        val server = server(unary { _, _ -> throw IllegalStateException("db password=hunter2") })
        val ex = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(ex.status).isEqualTo(500)
        assertThat(ex.body.readUtf8()).isEqualTo("""{"code":"unknown"}""")
    }

    @Test
    fun unencodableResponseIsInternal() {
        val server = server(unary { _, _ -> Msg("!") }, serverStream { _, _, s -> s.send(Msg("!")) })
        val unary = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(unary.status).isEqualTo(500)
        assertThat(parseJson(unary.body.readUtf8())).isEqualTo(mapOf("code" to "internal", "message" to "could not marshal response message"))
        for (protocol in Enveloped.values()) {
            assertThat(protocol.outcome(server.callEnveloped(protocol, "ServerStream", listOf("a"))).code).describedAs("$protocol").isEqualTo(Code.INTERNAL_ERROR)
        }
    }

    @Test
    fun undecodableRequestIsInvalidArgument() {
        val server = server(unary { req, _ -> req })
        val ex = server.call("Unary", "application/proto", "!bad".toByteArray())
        assertThat(ex.status).isEqualTo(400)
        assertThat(ex.body.readUtf8()).startsWith("""{"code":"invalid_argument"""")
    }

    @Test
    fun protocolVersionChecks() {
        val server = server(unary { req, _ -> req })
        assertThat(server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Protocol-Version" to "2")).status).isEqualTo(400)
        assertThat(server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Connect-Protocol-Version" to "1")).status).isEqualTo(200)
        val strict = server(unary { req, _ -> req }, config = ServerConfig(requireConnectProtocolHeader = true))
        assertThat(strict.call("Unary", "application/proto", "a".toByteArray()).status).isEqualTo(400)
    }

    @Test
    fun applicationMetadataCannotOverrideOrInjectProtocolFields() {
        val server = server(
            unary { req, ctx ->
                ctx.responseHeaders["Content-Type"] = mutableListOf("text/html")
                ctx.responseHeaders["connect-x"] = mutableListOf("x")
                ctx.responseHeaders["x-crlf"] = mutableListOf("a\r\nx-injected: 1\u0000")
                ctx.responseHeaders["bad name"] = mutableListOf("v")
                ctx.responseTrailers["grpc-status"] = mutableListOf("0")
                req
            },
        )
        val ex = server.call("Unary", "application/json", "a".toByteArray())
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.headers["content-type"]).containsExactly("application/json")
        assertThat(ex.headers["x-crlf"]).containsExactly("a  x-injected: 1 ")
        assertThat(ex.headers).doesNotContainKeys("connect-x", "bad name", "x-injected", "trailer-grpc-status")
    }
}
