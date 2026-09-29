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
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.IOException
import java.net.URLEncoder
import java.util.Base64
import java.util.Random

/** Connect unary over GET (protocol.md "Unary-Get-Request"). */
class ConnectGetTest {
    private val server = server(
        unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, ctx -> Msg("${req.text}|${ctx.queryParams?.get("zzz")}|${ctx.httpMethod}") },
        unary("Idempotent", Idempotency.IDEMPOTENT) { req, _ -> req },
        unary("Echo", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req },
        unary("Big", Idempotency.NO_SIDE_EFFECTS) { _, _ -> Msg("z".repeat(4000)) },
        unary("Post") { req, ctx -> Msg("${req.text}|${ctx.queryParams}|${ctx.httpMethod}") },
    )

    private fun get(procedure: String, query: String, headers: Map<String, String> = emptyMap(), server: ConnectServer = this.server) = server.call(procedure, null, method = "GET", query = query, headers = headers)

    private fun urlEncode(bytes: ByteArray): String = bytes.joinToString("") { "%%%02X".format(it.toInt() and 0xff) }

    @Test
    fun parameterMatrix() {
        val text = "hé y"
        for (encoding in listOf("proto", "json")) {
            for (compression in listOf(null, "identity", "gzip")) {
                for (base64 in listOf(false, true)) {
                    val payload = text.toByteArray().let { if (compression == "gzip") gzip(it) else it }
                    val message = if (base64) Base64.getUrlEncoder().withoutPadding().encodeToString(payload) else urlEncode(payload)
                    val params = mutableListOf("message=$message", "encoding=$encoding", "connect=v1", "zzz=1")
                    if (compression != null) params += "compression=$compression"
                    if (base64) params += "base64=1"
                    val query = params.shuffled(Random(params.hashCode().toLong())).joinToString("&")
                    val ex = get("Get", query)
                    assertThat(ex.status).describedAs(query).isEqualTo(200)
                    assertThat(ex.header("content-type")).describedAs(query).isEqualTo("application/$encoding")
                    assertThat(ex.header("vary")).isEqualTo("Accept-Encoding")
                    assertThat(ex.body.readUtf8()).describedAs(query).isEqualTo("$text|[1]|GET")
                }
            }
        }
    }

    /** protocol.md "Unary-Get-Request": `base64=1` values are URL-safe base64, padded or not. */
    @Test
    fun base64MessagesRoundTrip() {
        val random = Random(4)
        repeat(200) {
            val bytes = ByteArray(1 + random.nextInt(60)) { (0x20 + random.nextInt(0x5f)).toByte() }
            if (bytes[0] == '!'.code.toByte()) bytes[0] = 'a'.code.toByte()
            val encoder = if (random.nextBoolean()) Base64.getUrlEncoder() else Base64.getUrlEncoder().withoutPadding()
            val message = URLEncoder.encode(encoder.encodeToString(bytes), "UTF-8")
            val ex = get("Echo", "encoding=proto&base64=1&message=$message")
            assertThat(ex.status).describedAs(message).isEqualTo(200)
            assertThat(ex.bodyBytes).describedAs(message).isEqualTo(bytes)
        }
    }

    @Test
    fun responseCompressionFollowsAcceptEncoding() {
        val ex = get("Big", "encoding=proto&message=a", mapOf("Accept-Encoding" to "gzip"))
        assertThat(ex.header("content-encoding")).isEqualTo("gzip")
        assertThat(ex.header("vary")).isEqualTo("Accept-Encoding")
        assertThat(gunzip(ex.bodyBytes)).isEqualTo("z".repeat(4000))
    }

    @Test
    fun rejectedRequests() {
        val cases = listOf(
            "encoding=proto" to 400,
            "encoding=proto&base64=1&message=%%" to 400,
            "encoding=proto&base64=1&message=a*b" to 400,
            "encoding=proto&message=a%zz" to 400,
            "encoding=proto&message=a&connect=v2" to 400,
            "encoding=proto&message=a&compression=br" to 501,
            "encoding=proto&message=${urlEncode("not gzip".toByteArray())}&compression=gzip" to 400,
            "encoding=foo&message=a" to 415,
            "message=a" to 415,
        )
        for ((query, status) in cases) {
            val ex = get("Get", query)
            assertThat(ex.status).describedAs(query).isEqualTo(status)
            if (status == 400 || status == 501) {
                assertThat(ex.header("content-type")).describedAs(query).isEqualTo("application/json")
                assertThat(parseJson(ex.body.readUtf8())).describedAs(query).containsKey("code")
            }
        }
    }

    @Test
    fun onlyNoSideEffectsProceduresAcceptGet() {
        for (procedure in listOf("Idempotent", "Post")) {
            val ex = get(procedure, "encoding=proto&message=a")
            assertThat(ex.status).describedAs(procedure).isEqualTo(405)
            assertThat(ex.header("allow")).isEqualTo("POST")
        }
        assertThat(server.call("Post", "application/proto", "a".toByteArray()).body.readUtf8()).isEqualTo("a|null|POST")
    }

    /** A GET carries its message in the URL; a body is refused whether or not Content-Length announces it. */
    @Test
    fun getWithABodyIs415() {
        assertThat(server.call("Get", null, "b".toByteArray(), method = "GET", query = "encoding=proto&message=a").status).isEqualTo(415)
        assertThat(get("Get", "encoding=proto&message=a", mapOf("Content-Length" to "3")).status).isEqualTo(415)
        assertThat(get("Get", "encoding=proto&message=a", mapOf("Content-Length" to "0")).status).isEqualTo(200)
    }

    /**
     * HttpExchange.readRequestBody throws ConnectException to end the call while
     * the client is still connected; the body probe answers it as a unary error.
     */
    @Test
    fun getBodyReadErrorIsAnswered() {
        val codes = mutableListOf<Code?>()
        val server = server(
            unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req },
            config = ServerConfig(observer = { _, _, _ -> ServerObserver.Call { codes += it } }),
        )
        val fake = FakeExchange(method = "GET", path = "/$SVC/Get", rawQuery = "encoding=proto&message=a")
        val exchange = object : HttpExchange by fake {
            override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long = throw ConnectException(Code.UNAVAILABLE, "body unreadable")
        }
        runBlocking { server.serve(exchange) }
        assertThat(fake.status).isEqualTo(503)
        assertThat(fake.header("content-type")).isEqualTo("application/json")
        assertThat(fake.body.readUtf8()).isEqualTo("""{"code":"unavailable","message":"body unreadable"}""")
        assertThat(codes).containsExactly(Code.UNAVAILABLE)
    }

    /**
     * Only an IOException from the adapter means the client has gone away; a
     * ConnectException is answered whatever its cause.
     */
    @Test
    fun getBodyReadErrorWithAnIOExceptionCauseIsAnswered() {
        val fake = FakeExchange(method = "GET", path = "/$SVC/Get", rawQuery = "encoding=proto&message=a")
        val exchange = object : HttpExchange by fake {
            override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long = throw ConnectException(Code.UNAVAILABLE, "body unreadable", IOException("stream stalled"))
        }
        runBlocking { server.serve(exchange) }
        assertThat(fake.status).isEqualTo(503)
        assertThat(fake.body.readUtf8()).isEqualTo("""{"code":"unavailable","message":"body unreadable"}""")
    }

    /** A malformed timeout needs no body to be answered, so the body probe must not hold its answer back. */
    @Test
    fun invalidTimeoutIsAnsweredBeforeTheBodyIsRead() {
        // The body channel stays open: the client started a body and sends nothing.
        val exchange = FakeExchange(
            method = "GET",
            path = "/$SVC/Get",
            rawQuery = "encoding=proto&message=a",
            requestHeaders = mapOf("connect-timeout-ms" to listOf("invalid")),
        )
        runBlocking { withTimeout(2_000) { server.serve(exchange) } }
        assertThat(exchange.status).isEqualTo(400)
        assertThat(parseJson(exchange.body.readUtf8())).containsEntry("code", "invalid_argument")
        assertThat(exchange.bytesRead.get()).isZero()
    }

    @Test
    fun readLimitAppliesToTheMessageParameter() {
        val limited = server(unary("Echo", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req }, config = ServerConfig(readMaxBytes = 64))
        assertThat(get("Echo", "encoding=proto&message=${"a".repeat(64)}", server = limited).status).isEqualTo(200)
        assertThat(get("Echo", "encoding=proto&message=${"a".repeat(65)}", server = limited).status).isEqualTo(429)
        val b64 = Base64.getUrlEncoder().encodeToString(ByteArray(65) { 'a'.code.toByte() })
        assertThat(get("Echo", "encoding=proto&base64=1&message=$b64", server = limited).status).isEqualTo(429)
        val bomb = urlEncode(gzip(ByteArray(10_000) { 'a'.code.toByte() }))
        assertThat(get("Echo", "encoding=proto&compression=gzip&message=$bomb", server = limited).status).isEqualTo(429)
    }

    @Test
    fun requiredConnectParameter() {
        val strict = server(unary("Echo", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req }, config = ServerConfig(requireConnectProtocolHeader = true))
        assertThat(get("Echo", "encoding=proto&message=a", server = strict).status).isEqualTo(400)
        assertThat(get("Echo", "encoding=proto&message=a&connect=v1", server = strict).status).isEqualTo(200)
        // The header form is for POST; a GET needs the query parameter.
        assertThat(get("Echo", "encoding=proto&message=a", mapOf("Connect-Protocol-Version" to "1"), server = strict).status).isEqualTo(400)
    }

    @Test
    fun rawQueryIsNotDoubleDecoded() {
        // `%2525` is the text `%25`, not `%`.
        assertThat(get("Echo", "encoding=proto&message=%2525").body.readUtf8()).isEqualTo("%25")
    }
}
