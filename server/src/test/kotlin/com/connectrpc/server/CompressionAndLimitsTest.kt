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

import com.connectrpc.server.compression.GzipServerCompressionPool
import com.connectrpc.server.compression.ServerCompressionPool
import okio.Buffer
import okio.ForwardingSource
import okio.Source
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.InflaterInputStream

class CompressionAndLimitsTest {
    /** gzip that counts decompressed bytes handed to the server. */
    private class CountingGzip : ServerCompressionPool {
        val produced = AtomicLong()
        override fun name() = "gzip"
        override fun compress(buffer: Buffer): Buffer = GzipServerCompressionPool.compress(buffer)
        override fun decompress(source: Source): Source = object : ForwardingSource(GzipServerCompressionPool.decompress(source)) {
            override fun read(sink: Buffer, byteCount: Long): Long = super.read(sink, byteCount).also { if (it > 0) produced.addAndGet(it) }
        }
    }

    @Test
    fun unknownEncodingIsUnimplementedListingSupported() {
        val server = server(unary { req, _ -> req }, serverStream { req, _, s -> s.send(req) })
        val unary = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Content-Encoding" to "br"))
        assertThat(unary.status).isEqualTo(501)
        assertThat(unary.body.readUtf8()).isEqualTo(
            """{"code":"unimplemented","message":"unknown compression \"br\": supported encodings are gzip,deflate"}""",
        )
        val grpc = server.call("Unary", "application/grpc", env(0, "a"), mapOf("grpc-encoding" to "foo"))
        assertThat(grpc.headers["grpc-status"]).containsExactly("12")
        assertThat(grpc.headers["grpc-accept-encoding"]).containsExactly("gzip,deflate")
        assertThat(grpc.headers["grpc-message"]!!.single()).contains("supported encodings are gzip,deflate")
        val stream = server.call("ServerStream", "application/connect+proto", env(0, "a"), mapOf("Connect-Content-Encoding" to "foo"))
        assertThat(stream.headers["connect-accept-encoding"]).containsExactly("gzip,deflate")
        assertThat(frames(stream.bodyBytes).single().text).contains("\"unimplemented\"")
    }

    /**
     * Response encoding for (request encoding, Accept-Encoding) on every
     * protocol's headers: with an accept header, the client's first supported
     * entry that is not `q=0`, else none (protocol.md "Accept-Encoding";
     * RFC 9110 §12.5.3; grpc `doc/compression.md`); without one, the
     * request's own encoding.
     */
    @Test
    fun negotiationMatrix() {
        val big = "x".repeat(2000)
        val server = server(unary { _, _ -> Msg(big) }, serverStream { _, _, s -> s.send(Msg(big)) })
        val cases = listOf(
            Triple(null, "br, gzip", "gzip"),
            Triple(null, "deflate;q=0.5, gzip", "deflate"),
            Triple(null, "gzip;q=0, deflate", "deflate"),
            Triple(null, "gzip;Q=0.0", null),
            Triple(null, "gzip;q=0.001", "gzip"),
            Triple(null, " GZIP ", "gzip"),
            Triple(null, "br", null),
            Triple(null, "", null),
            Triple(null, null, null),
            Triple("gzip", null, "gzip"),
            Triple("gzip", "deflate", "deflate"),
            Triple("gzip", "deflate, gzip", "deflate"),
            Triple("gzip", "br, gzip", "gzip"),
            Triple("gzip", "br", null),
            Triple("gzip", "gzip;q=0", null),
            Triple("gzip", "", null),
            Triple(" Gzip ", null, "gzip"),
            Triple("identity", "deflate", "deflate"),
            Triple("IDENTITY", null, null),
        )
        for ((sent, accept, expected) in cases) {
            val label = "sent=$sent accept=$accept"
            fun headers(encoding: String, acceptName: String) = buildMap {
                if (sent != null) put(encoding, sent)
                if (accept != null) put(acceptName, accept)
            }
            val request = if (sent?.trim()?.lowercase() == "gzip") gzip("a".toByteArray()) else "a".toByteArray()
            val unary = server.call("Unary", "application/proto", request, headers("content-encoding", "accept-encoding"))
            assertThat(unary.status).describedAs(label).isEqualTo(200)
            assertThat(unary.header("content-encoding")).describedAs(label).isEqualTo(expected)
            for (protocol in Enveloped.values()) {
                val acceptName = if (protocol == Enveloped.CONNECT) "connect-accept-encoding" else "grpc-accept-encoding"
                val flags = if (sent?.trim()?.lowercase() == "gzip") 1 else 0
                val ex = server.call("ServerStream", protocol.contentType, env(flags, request), headers(protocol.encodingHeader, acceptName))
                assertThat(ex.headers[protocol.encodingHeader]?.single()).describedAs("$protocol $label").isEqualTo(expected)
                assertThat(frames(ex.bodyBytes).first().flags).describedAs("$protocol $label").isEqualTo(if (expected == null) 0 else 1)
            }
        }
        val small = server(unary { req, _ -> req }).call("Unary", "application/proto", "a".toByteArray(), mapOf("Accept-Encoding" to "gzip"))
        assertThat(small.header("content-encoding")).isNull()
        assertThat(small.body.readUtf8()).isEqualTo("a")
    }

    /**
     * A client that sends gzip but accepts only deflate can decode every
     * response message (grpc `doc/compression.md` "Compression Method
     * Asymmetry Between Peers").
     */
    @Test
    fun asymmetricClientDecodesResponse() {
        val big = "x".repeat(2000)
        val server = server(unary { req, _ -> Msg(req.text + big) }, serverStream { req, _, s -> s.send(Msg(req.text + big)) })
        fun deflateOnly(encoding: String?, compressed: Boolean, bytes: ByteArray): String {
            if (!compressed) return bytes.toString(Charsets.UTF_8)
            assertThat(encoding).isEqualTo("deflate")
            return InflaterInputStream(bytes.inputStream()).use { it.readBytes() }.toString(Charsets.UTF_8)
        }
        val request = gzip("a".toByteArray())
        for (accept in listOf("deflate", "br")) {
            val unary = server.call(
                "Unary",
                "application/proto",
                request,
                mapOf("content-encoding" to "gzip", "accept-encoding" to accept),
            )
            val unaryEncoding = unary.header("content-encoding")
            assertThat(deflateOnly(unaryEncoding, unaryEncoding != null, unary.bodyBytes)).describedAs(accept).isEqualTo("a$big")
            for (protocol in Enveloped.values()) {
                val acceptName = if (protocol == Enveloped.CONNECT) "connect-accept-encoding" else "grpc-accept-encoding"
                val ex = server.call(
                    "ServerStream",
                    protocol.contentType,
                    env(1, request),
                    mapOf(protocol.encodingHeader to "gzip", acceptName to accept),
                )
                val frame = frames(ex.bodyBytes).first()
                val text = deflateOnly(ex.headers[protocol.encodingHeader]?.single(), frame.flags and 1 == 1, frame.payload)
                assertThat(text).describedAs("$protocol $accept").isEqualTo("a$big")
            }
        }
    }

    /** `deflate` is zlib-wrapped DEFLATE (grpc `doc/compression.md`); raw DEFLATE is rejected. */
    @Test
    fun deflateRoundTripsBothWays() {
        val big = "x".repeat(64_000)
        val server = server(unary { req, _ -> Msg(if (req.text == "big") big else "got:${req.text}") })
        val ex = server.call("Unary", "application/proto", "big".toByteArray(), mapOf("Accept-Encoding" to "deflate"))
        assertThat(ex.header("content-encoding")).isEqualTo("deflate")
        assertThat(ex.bodyBytes.size).isLessThan(big.length / 2)
        assertThat(InflaterInputStream(ex.bodyBytes.inputStream()).use { it.readBytes() }.toString(Charsets.UTF_8)).isEqualTo(big)

        fun deflate(text: String, nowrap: Boolean) = ByteArrayOutputStream().also { out ->
            DeflaterOutputStream(out, Deflater(Deflater.DEFAULT_COMPRESSION, nowrap)).use { it.write(text.toByteArray()) }
        }.toByteArray()
        val zlib = server.call("Unary", "application/proto", deflate("hello", nowrap = false), mapOf("Content-Encoding" to "deflate"))
        assertThat(zlib.body.readUtf8()).isEqualTo("got:hello")
        val raw = server.call("Unary", "application/proto", deflate("hello", nowrap = true), mapOf("Content-Encoding" to "deflate"))
        assertThat(raw.status).isEqualTo(400)
        assertThat(parseJson(raw.body.readUtf8())["code"]).isEqualTo("invalid_argument")
    }

    @Test
    fun withoutCompressionPoolsOnlyIdentityIsAccepted() {
        val server = server(unary { _, _ -> Msg("x".repeat(2000)) }, config = ServerConfig(compressionPools = emptyList()))
        val plain = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Accept-Encoding" to "gzip"))
        assertThat(plain.status).isEqualTo(200)
        assertThat(plain.headers).doesNotContainKeys("content-encoding", "accept-encoding")
        val gz = server.call("Unary", "application/proto", gzip("a".toByteArray()), mapOf("Content-Encoding" to "gzip"))
        assertThat(gz.status).isEqualTo(501)
        assertThat(parseJson(gz.body.readUtf8())["message"]).isEqualTo("unknown compression \"gzip\": supported encodings are identity")
    }

    @Test
    fun emptyCompressedBodyIsNotDecompressed() {
        val server = server(unary { req, _ -> Msg("[${req.text}]") })
        val ex = server.call("Unary", "application/proto", ByteArray(0), mapOf("Content-Encoding" to "gzip"))
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.body.readUtf8()).isEqualTo("[]")
    }

    @Test
    fun unaryBodyOverLimitStopsReading() {
        val server = server(unary { req, _ -> req }, config = ServerConfig(readMaxBytes = 1024))
        val exchange = FakeExchange(path = "/$SVC/Unary", requestHeaders = mapOf("content-type" to listOf("application/proto")))
        repeat(1000) { exchange.bodyChunks.trySend(ByteArray(64 * 1024)) } // 64 MiB offered
        exchange.bodyChunks.close()
        kotlinx.coroutines.runBlocking { server.serve(exchange) }
        assertThat(exchange.status).isEqualTo(429)
        assertThat(exchange.body.readUtf8()).contains("resource_exhausted")
        // The limit, one read chunk and at most 4 MiB drained for the client's sake; not the 64 MiB offered.
        assertThat(exchange.bytesRead.get()).isLessThanOrEqualTo(1024 + 64 * 1024 + 4 * 1024 * 1024 + 64 * 1024)
        val exact = server.call("Unary", "application/proto", ByteArray(1024) { 'a'.code.toByte() })
        assertThat(exact.status).isEqualTo(200)
    }

    @Test
    fun gzipBombStopsAtLimit() {
        val counting = CountingGzip()
        val server = server(
            unary { req, _ -> req },
            serverStream { req, _, s -> s.send(req) },
            config = ServerConfig(readMaxBytes = 1024 * 1024, compressionPools = listOf(counting)),
        )
        val bomb = gzip(ByteArray(64 * 1024 * 1024)) // 64 MiB of zeros, ~64 KiB compressed
        val unary = server.call("Unary", "application/proto", bomb, mapOf("Content-Encoding" to "gzip"))
        assertThat(unary.status).isEqualTo(429)
        assertThat(counting.produced.get()).isLessThanOrEqualTo(1024 * 1024 + 8192)

        counting.produced.set(0)
        val stream = server.call("ServerStream", "application/connect+proto", env(1, bomb), mapOf("Connect-Content-Encoding" to "gzip"))
        assertThat(frames(stream.bodyBytes).single().text).contains("\"resource_exhausted\"")
        assertThat(counting.produced.get()).isLessThanOrEqualTo(1024 * 1024 + 8192)
    }

    @Test
    fun oversizedEnvelopeIsRejectedWithoutBuffering() {
        val server = server(
            clientStream { s, _ ->
                while (s.receive() != null) continue
                Msg("done")
            },
            config = ServerConfig(readMaxBytes = 1024),
        )
        val exchange = FakeExchange(path = "/$SVC/ClientStream", requestHeaders = mapOf("content-type" to listOf("application/connect+proto")))
        exchange.bodyChunks.trySend(Buffer().writeByte(0).writeInt(Int.MAX_VALUE).readByteArray())
        repeat(1000) { exchange.bodyChunks.trySend(ByteArray(64 * 1024)) }
        exchange.bodyChunks.close()
        kotlinx.coroutines.runBlocking { server.serve(exchange) }
        assertThat(frames(exchange.bodyBytes).single().text).isEqualTo(
            """{"error":{"code":"resource_exhausted","message":"message size 2147483647 is larger than configured max 1024"}}""",
        )
        assertThat(exchange.bytesRead.get()).isLessThanOrEqualTo(4 * 1024 * 1024 + 64 * 1024 + 5)
    }

    @Test
    fun sendLimit() {
        val server = server(unary { _, _ -> Msg("x".repeat(100)) }, config = ServerConfig(sendMaxBytes = 10))
        val ex = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(ex.status).isEqualTo(429)
    }

    @Test
    fun defaultReadLimitIsFourMebibytes() {
        val server = server(unary { req, _ -> Msg("${req.text.length}") })
        assertThat(server.call("Unary", "application/proto", ByteArray(4 * 1024 * 1024 + 1)).status).isEqualTo(429)
    }

    @Test
    fun zeroReadLimitMeansUnlimited() {
        val server = server(unary { req, _ -> Msg("${req.text.length}") }, config = ServerConfig(readMaxBytes = 0))
        val ex = server.call("Unary", "application/proto", ByteArray(4 * 1024 * 1024 + 1))
        assertThat(ex.status).isEqualTo(200)
        assertThat(ex.body.readUtf8()).isEqualTo("${4 * 1024 * 1024 + 1}")
    }
}
