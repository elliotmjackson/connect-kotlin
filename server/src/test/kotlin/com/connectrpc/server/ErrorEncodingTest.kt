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
import com.google.protobuf.UnknownFieldSet
import okio.ByteString.Companion.toByteString
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.Base64
import java.util.Random

/**
 * Errors as each protocol's clients decode them. Expected values are written
 * out from the specifications rather than taken from [Code], and the JSON and
 * protobuf encodings are read back with Moshi and protobuf-java.
 */
class ErrorEncodingTest {
    private class Row(val code: Code, val name: String, val grpcStatus: Int, val httpStatus: Int)

    /**
     * Connect names and HTTP statuses: protocol.md "Error Codes". gRPC numbers:
     * grpc `doc/statuscodes.md`.
     */
    private val table = listOf(
        Row(Code.CANCELED, "canceled", 1, 499),
        Row(Code.UNKNOWN, "unknown", 2, 500),
        Row(Code.INVALID_ARGUMENT, "invalid_argument", 3, 400),
        Row(Code.DEADLINE_EXCEEDED, "deadline_exceeded", 4, 504),
        Row(Code.NOT_FOUND, "not_found", 5, 404),
        Row(Code.ALREADY_EXISTS, "already_exists", 6, 409),
        Row(Code.PERMISSION_DENIED, "permission_denied", 7, 403),
        Row(Code.RESOURCE_EXHAUSTED, "resource_exhausted", 8, 429),
        Row(Code.FAILED_PRECONDITION, "failed_precondition", 9, 400),
        Row(Code.ABORTED, "aborted", 10, 409),
        Row(Code.OUT_OF_RANGE, "out_of_range", 11, 400),
        Row(Code.UNIMPLEMENTED, "unimplemented", 12, 501),
        Row(Code.INTERNAL_ERROR, "internal", 13, 500),
        Row(Code.UNAVAILABLE, "unavailable", 14, 503),
        Row(Code.DATA_LOSS, "data_loss", 15, 500),
        Row(Code.UNAUTHENTICATED, "unauthenticated", 16, 401),
    )

    private val failing = server(
        unary { req, _ -> throw ConnectException(Code.valueOf(req.text), "m") },
        serverStream { req, _, _ -> throw ConnectException(Code.valueOf(req.text), "m") },
    )

    @Test
    fun everyCodeOnEveryProtocol() {
        assertThat(table.map { it.code }).containsExactlyInAnyOrder(*Code.values())
        for (row in table) {
            val unary = failing.call("Unary", "application/proto", row.code.name.toByteArray())
            assertThat(unary.status).describedAs(row.name).isEqualTo(row.httpStatus)
            assertThat(unary.header("content-type")).isEqualTo("application/json")
            assertThat(parseJson(unary.body.readUtf8())).describedAs(row.name).isEqualTo(mapOf("code" to row.name, "message" to "m"))

            val stream = failing.call("ServerStream", "application/connect+proto", env(0, row.code.name))
            assertThat(stream.status).isEqualTo(200)
            assertThat(parseJson(frames(stream.bodyBytes).single().text)).describedAs(row.name)
                .isEqualTo(mapOf("error" to mapOf("code" to row.name, "message" to "m")))

            for (protocol in listOf(Enveloped.GRPC, Enveloped.GRPC_WEB)) {
                val ex = failing.callEnveloped(protocol, "Unary", listOf(row.code.name))
                assertThat(ex.status).isEqualTo(200)
                val fields = protocol.statusFields(ex)
                assertThat(fields["grpc-status"]).describedAs("$protocol ${row.name}").containsExactly("${row.grpcStatus}")
            }
        }
    }

    @Test
    fun connectJsonStringsRoundTrip() {
        val random = Random(1)
        val server = server(
            unary { req, _ -> throw ConnectException(Code.ABORTED, req.text) },
            serverStream { req, ctx, _ ->
                ctx.responseTrailers["x-t"] = mutableListOf(req.text.filter { it in ' '..'~' })
                throw ConnectException(Code.ABORTED, req.text)
            },
        )
        repeat(300) {
            val text = randomText(random)
            val unary = server.call("Unary", "application/proto", text.toByteArray()).body.readUtf8()
            // RFC 8259 §7: control characters must be escaped. Moshi accepts them raw, so check separately.
            assertThat(unary.none { it < ' ' }).describedAs(unary).isTrue()
            assertThat(parseJson(unary)["message"]).describedAs(text).isEqualTo(text.ifEmpty { null })

            val endText = frames(server.call("ServerStream", "application/connect+proto", env(0, text)).bodyBytes).last().text
            assertThat(endText.none { it < ' ' }).describedAs(endText).isTrue()
            val end = parseJson(endText)
            @Suppress("UNCHECKED_CAST")
            assertThat((end["error"] as Map<String, Any?>)["message"]).describedAs(text).isEqualTo(text.ifEmpty { null })
            assertThat(end["metadata"]).describedAs(text).isEqualTo(mapOf("x-t" to listOf(text.filter { it in ' '..'~' })))
        }
    }

    @Test
    fun grpcMessagePercentEncodingRoundTrips() {
        val random = Random(2)
        val server = server(unary { req, _ -> throw ConnectException(Code.ABORTED, req.text) })
        repeat(300) {
            val text = randomText(random).ifEmpty { "x" }
            for (protocol in listOf(Enveloped.GRPC, Enveloped.GRPC_WEB)) {
                val ex = server.callEnveloped(protocol, "Unary", listOf(text))
                val encoded = protocol.statusFields(ex)["grpc-message"]!!.single()
                // PROTOCOL-HTTP2.md "Status-Message": Percent-Byte-Unencoded = %x20-24 / %x26-7E, Percent-Encoded = "%" 2HEXDIGIT.
                assertThat(encoded).describedAs(text).matches("([\\x20-\\x24\\x26-\\x7e]|%[0-9A-F]{2})*")
                assertThat(percentDecode(encoded)).describedAs(text).isEqualTo(text)
            }
        }
    }

    @Test
    fun detailsInConnectJsonAndGrpcStatus() {
        val longMessage = "é".repeat(200) // 400 UTF-8 bytes: a two-byte length varint.
        val binary = ByteArray(300) { it.toByte() }
        val details = listOf(
            ConnectErrorDetail("type.googleapis.com/pkg.v1.Info", "ab".encodeToByteArray().toByteString()),
            ConnectErrorDetail("pkg.v1.Bare", binary.toByteString()),
            ConnectErrorDetail("example.com/x/pkg.v1.Custom", ByteArray(0).toByteString()),
        )
        val server = server(
            unary { _, _ ->
                throw ConnectException(Code.FAILED_PRECONDITION, longMessage)
                    .withErrorDetails(BytesStrategy("proto").errorDetailParser(), details)
            },
        )

        // protocol.md "Error Details": `type` without the type-URL prefix, `value` base64 (padding optional).
        val json = parseJson(server.call("Unary", "application/proto", "a".toByteArray()).body.readUtf8())

        @Suppress("UNCHECKED_CAST")
        val jsonDetails = json["details"] as List<Map<String, String>>
        assertThat(jsonDetails.map { it["type"] }).containsExactly("pkg.v1.Info", "pkg.v1.Bare", "pkg.v1.Custom")
        assertThat(jsonDetails.map { Base64.getDecoder().decode(it["value"]).toByteString() }).containsExactlyElementsOf(details.map { it.payload })
        assertThat(jsonDetails.map { it["value"] }).allSatisfy { assertThat(it).doesNotEndWith("=") }

        for (protocol in listOf(Enveloped.GRPC, Enveloped.GRPC_WEB)) {
            val ex = server.callEnveloped(protocol, "Unary", listOf("a"))
            val fields = protocol.statusFields(ex)
            val bin = fields["grpc-status-details-bin"]!!.single()
            assertThat(bin).doesNotEndWith("=")
            // google.rpc.Status { int32 code = 1; string message = 2; repeated google.protobuf.Any details = 3; }
            val status = UnknownFieldSet.parseFrom(Base64.getDecoder().decode(bin))
            assertThat(status.getField(1).varintList).containsExactly(9L)
            assertThat(status.getField(2).lengthDelimitedList.single().toStringUtf8()).isEqualTo(longMessage)
            val anys = status.getField(3).lengthDelimitedList.map { UnknownFieldSet.parseFrom(it) }
            assertThat(anys.map { it.getField(1).lengthDelimitedList.single().toStringUtf8() }).containsExactly(
                "type.googleapis.com/pkg.v1.Info",
                "type.googleapis.com/pkg.v1.Bare",
                "example.com/x/pkg.v1.Custom",
            )
            assertThat(anys.map { it.getField(2).lengthDelimitedList.single().toByteArray().toByteString() })
                .containsExactlyElementsOf(details.map { it.payload })
        }
    }

    @Test
    fun emptyMessageIsOmitted() {
        val server = server(unary { _, _ -> throw ConnectException(Code.NOT_FOUND) }, serverStream { _, _, _ -> throw ConnectException(Code.NOT_FOUND, "") })
        assertThat(server.call("Unary", "application/proto", "a".toByteArray()).body.readUtf8()).isEqualTo("""{"code":"not_found"}""")
        assertThat(frames(server.call("ServerStream", "application/connect+proto", env(0, "a")).bodyBytes).single().text)
            .isEqualTo("""{"error":{"code":"not_found"}}""")
        val grpc = server.callEnveloped(Enveloped.GRPC, "Unary", listOf("a"))
        assertThat(grpc.trailers).containsOnlyKeys("grpc-status")
    }

    /** Unicode text with the characters JSON and percent-encoding must escape. */
    private fun randomText(random: Random): String {
        val pool = "\"\\/%\u0000\u0001\u001f\t\n\r\u007f\u0080\u00ff\u2028\u2029é中\uD83D\uDE00 aZ9{}[]:,"
        val sb = StringBuilder()
        repeat(random.nextInt(24)) {
            when (random.nextInt(3)) {
                // '!' is excluded: the test codec rejects messages starting with it.
                0 -> sb.appendCodePoint(random.nextInt(0x80).let { if (it == '!'.code) ' '.code else it })

                1 -> sb.appendCodePoint(0x80 + random.nextInt(0xd800 - 0x80))

                else -> {
                    // Pick a whole code point from the pool so surrogate pairs stay paired.
                    val i = random.nextInt(pool.length).let { if (pool[it].isLowSurrogate()) it - 1 else it }
                    sb.appendCodePoint(pool.codePointAt(i))
                }
            }
        }
        return sb.toString()
    }
}
