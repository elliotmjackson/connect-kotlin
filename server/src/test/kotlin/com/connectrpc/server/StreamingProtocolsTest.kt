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
import okio.Buffer
import okio.ByteString.Companion.encodeUtf8
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.Base64

class StreamingProtocolsTest {
    private val detail = ConnectErrorDetail("pkg.Info", "ab".encodeUtf8())

    /** google.rpc.Status{code=5, message="50% ü", details=[Any{type_url="type.googleapis.com/pkg.Info", value="ab"}]}. */
    private val expectedStatus: ByteArray = run {
        val message = "50% ü".toByteArray()
        Buffer().writeByte(0x08).writeByte(5)
            .writeByte(0x12).writeByte(message.size).write(message)
            .writeByte(0x1a).writeByte(34)
            .writeByte(0x0a).writeByte(28).writeUtf8("type.googleapis.com/pkg.Info")
            .writeByte(0x12).writeByte(2).writeUtf8("ab")
            .readByteArray()
    }

    private val server = server(
        unary { req, ctx ->
            ctx.responseHeaders["x-h"] = mutableListOf("h")
            ctx.responseTrailers["x-t"] = mutableListOf("a\r\ngrpc-status: 0")
            ctx.responseTrailers["grpc-status"] = mutableListOf("0")
            if (req.text == "fail") {
                throw ConnectException(Code.NOT_FOUND, "50% ü")
                    .withErrorDetails(BytesStrategy("proto").errorDetailParser(), listOf(detail))
            }
            Msg("echo:${req.text}")
        },
        serverStream { req, ctx, s ->
            ctx.responseTrailers["x-t"] = mutableListOf("t1", "t2")
            s.send(Msg("1:${req.text}"))
            s.send(Msg("2:${req.text}"))
            if (req.text == "fail") throw ConnectException(Code.DATA_LOSS, "lost")
        },
        clientStream { s, _ ->
            val all = mutableListOf<String>()
            while (true) all += (s.receive() ?: break).text
            Msg(all.joinToString(","))
        },
    )

    @Test
    fun connectStreamWire() {
        val ok = server.call("ServerStream", "application/connect+json", env(0, "a"))
        assertThat(ok.status).isEqualTo(200)
        assertThat(ok.header("content-type")).isEqualTo("application/connect+json")
        assertThat(ok.header("connect-accept-encoding")).isEqualTo("gzip,deflate")
        val okFrames = frames(ok.bodyBytes)
        assertThat(okFrames.map { it.flags }).containsExactly(0, 0, 2)
        assertThat(okFrames.map { it.text }).containsExactly("1:a", "2:a", """{"metadata":{"x-t":["t1","t2"]}}""")

        val failed = frames(server.call("ServerStream", "application/connect+json", env(0, "fail")).bodyBytes)
        assertThat(failed.last().text).isEqualTo("""{"error":{"code":"data_loss","message":"lost"},"metadata":{"x-t":["t1","t2"]}}""")

        val clientStream = frames(server.call("ClientStream", "application/connect+proto", env(0, "a") + env(0, "b") + env(0, "c")).bodyBytes)
        assertThat(clientStream.map { it.text }).containsExactly("a,b,c", "{}")
    }

    @Test
    fun requestEnvelopeValidation() {
        fun endStream(body: ByteArray, headers: Map<String, String> = emptyMap()) = frames(server.call("ServerStream", "application/connect+proto", body, headers).bodyBytes).last().text

        assertThat(endStream(env(2, "a"))).contains("\"internal\"", "invalid envelope flags 2")
        assertThat(endStream(env(4, "a"))).contains("\"internal\"", "invalid envelope flags 4")
        assertThat(endStream(env(0x80, "a"))).contains("\"internal\"")
        assertThat(endStream(env(1, "a"))).contains("\"internal\"", "without compression support")
        assertThat(endStream(env(0, "abc").copyOf(6))).contains("\"invalid_argument\"", "promised 3 bytes")
        assertThat(endStream(byteArrayOf(0, 0, 0))).contains("\"invalid_argument\"", "incomplete envelope")
        assertThat(endStream(ByteArray(0))).contains("\"unimplemented\"", "zero messages")
        assertThat(endStream(env(0, "a") + env(0, "b"))).contains("\"unimplemented\"", "multiple messages")
        val grpc = server.call("Unary", "application/grpc", env(0, "a") + env(0, "b"))
        assertThat(grpc.trailers["grpc-status"]).containsExactly("12")
    }

    @Test
    fun grpcWire() {
        val ok = server.call("Unary", "application/grpc", env(0, "a"), mapOf("te" to "trailers"))
        assertThat(ok.status).isEqualTo(200)
        assertThat(ok.headers).containsEntry("content-type", listOf("application/grpc"))
            .containsEntry("grpc-accept-encoding", listOf("gzip,deflate"))
            .containsEntry("x-h", listOf("h"))
            .doesNotContainKey("grpc-status")
        assertThat(frames(ok.bodyBytes).map { it.text }).containsExactly("echo:a")
        assertThat(ok.trailers).containsEntry("grpc-status", listOf("0")).containsEntry("x-t", listOf("a  grpc-status: 0"))

        val failed = server.call("Unary", "application/grpc+json", env(0, "fail"))
        assertThat(failed.bodyBytes).isEmpty()
        assertThat(failed.trailers["grpc-status"]).containsExactly("5")
        assertThat(failed.trailers["grpc-message"]).containsExactly("50%25 %C3%BC")
        val status = Base64.getDecoder().decode(failed.trailers["grpc-status-details-bin"]!!.single())
        assertThat(status).isEqualTo(expectedStatus)
    }

    @Test
    fun grpcWithoutTrailerSupportIsRejected() {
        val ex = server.call("Unary", "application/grpc", env(0, "a"), supportsTrailers = false)
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.headers["grpc-status"]).containsExactly("12")
        assertThat(ex.bodyBytes).isEmpty()
        // gRPC-Web and Connect never need HTTP trailers.
        assertThat(server.call("Unary", "application/grpc-web", env(0, "a"), supportsTrailers = false).trailers).isEmpty()
    }

    @Test
    fun grpcWebTrailerFrame() {
        val failed = server.call("Unary", "application/grpc-web+proto", env(0, "fail"))
        assertThat(failed.trailers).isEmpty()
        val trailer = frames(failed.bodyBytes).single()
        assertThat(trailer.flags).isEqualTo(0x80)
        assertThat(trailer.text).isEqualTo(
            "grpc-status: 5\r\ngrpc-message: 50%25 %C3%BC\r\n" +
                "grpc-status-details-bin: ${Base64.getEncoder().withoutPadding().encodeToString(expectedStatus)}\r\n" +
                "x-t: a  grpc-status: 0\r\n",
        )
        val ok = frames(server.call("Unary", "application/grpc-web", env(0, "a")).bodyBytes)
        assertThat(ok.map { it.flags }).containsExactly(0, 0x80)
        assertThat(ok.last().text).isEqualTo("grpc-status: 0\r\nx-t: a  grpc-status: 0\r\n")
    }
}
