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

package com.connectrpc.server.ktor

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.compression.CompressionPool
import com.connectrpc.compression.GzipCompressionPool
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test

/**
 * TDD target for outbound response compression.
 */
class CompressionTest {

    /**
     * When the client advertises gzip support via `Accept-Encoding`, the
     * server should compress the response body and set `Content-Encoding: gzip`.
     *
     * Currently fails: we decompress incoming gzip but never compress
     * outgoing responses (always identity).
     */
    @Test
    fun outboundGzipCompression() {
        val largePayload = ByteArray(64_000) { (it % 256).toByte() }
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Echo",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                TestMessage(largePayload)
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Echo")
                .header("Content-Type", "application/proto")
                .header("Accept-Encoding", "gzip")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            // Use a client that does NOT auto-decompress so we can inspect
            // Content-Encoding directly. (OkHttp transparently decompresses
            // gzip by default unless the caller sets Accept-Encoding manually
            // — which we did, so we'll see the raw bytes.)
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.header("Content-Encoding")).isEqualTo("gzip")
                // The gzipped body should be much smaller than the 64k original.
                val bodyBytes = response.body!!.bytes()
                assertThat(bodyBytes.size).isLessThan(largePayload.size / 2)
            }
        }
    }

    /**
     * Same shape as [outboundGzipCompression] but with deflate. Verifies
     * the COMPRESSION_POOLS map dispatches by name and the deflate pool
     * round-trips correctly via the JDK's Inflater.
     */
    @Test
    fun outboundDeflateCompression() {
        val largePayload = ByteArray(64_000) { (it % 256).toByte() }
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Echo",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                TestMessage(largePayload)
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Echo")
                .header("Content-Type", "application/proto")
                .header("Accept-Encoding", "deflate")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.header("Content-Encoding")).isEqualTo("deflate")
                val compressedBody = response.body!!.bytes()
                assertThat(compressedBody.size).isLessThan(largePayload.size / 2)
                // Decompress and verify roundtrip.
                val decompressed = java.util.zip.InflaterInputStream(
                    java.io.ByteArrayInputStream(compressedBody),
                ).use { it.readBytes() }
                assertThat(decompressed).isEqualTo(largePayload)
            }
        }
    }

    /**
     * Streaming variant: when client sends Connect-Accept-Encoding: gzip on a
     * Connect server-stream, the server should set the response's
     * Connect-Content-Encoding header to gzip and compress each emitted
     * envelope (flag bit 0x01 set, payload gzipped) provided it meets the
     * compress-min-bytes threshold.
     */
    @Test
    fun outboundStreamingGzipCompression() {
        val largePayload = ByteArray(64_000) { (it % 256).toByte() }
        val handler = object : ServerStreamHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Stream",
                TestMessage::class,
                TestMessage::class,
                StreamType.SERVER,
            )

            override suspend fun handle(
                request: TestMessage,
                ctx: HandlerContext,
                stream: ServerMessageStream<TestMessage>,
            ) {
                stream.send(TestMessage(largePayload))
            }
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Stream")
                .header("Content-Type", "application/connect+proto")
                .header("Connect-Accept-Encoding", "gzip")
                .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.header("Connect-Content-Encoding"))
                    .describedAs("server should advertise gzip on streaming response")
                    .isEqualTo("gzip")

                val source = response.body!!.source()
                val first = readEnvelope(source) ?: error("expected at least one envelope")
                assertThat(first.flags and 0x01)
                    .describedAs("envelope must carry the compressed flag")
                    .isEqualTo(0x01)
                // Decompress and verify roundtrip — gzip pool is the only one we register.
                val decompressed = GzipCompressionPool
                    .decompress(Buffer().write(first.payload))
                    .readByteArray()
                assertThat(decompressed).isEqualTo(largePayload)
            }
        }
    }

    /**
     * A user-registered [CompressionPool] should be picked up for both
     * inbound decompression and outbound compression negotiation. The pool
     * here uses a sentinel byte prefix (no real compression) — the test only
     * cares that the registry routes by name end-to-end.
     */
    @Test
    fun customCompressionPoolRegistration() {
        val largePayload = ByteArray(64_000) { (it % 256).toByte() }
        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Echo",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                TestMessage(largePayload)
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .compressionPool(SentinelCompressionPool)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Echo")
                .header("Content-Type", "application/proto")
                .header("Accept-Encoding", "sentinel")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.header("Content-Encoding")).isEqualTo("sentinel")
                val bodyBytes = response.body!!.bytes()
                val decoded = SentinelCompressionPool
                    .decompress(Buffer().write(bodyBytes))
                    .readByteArray()
                assertThat(decoded).isEqualTo(largePayload)
            }
        }
    }
}

/**
 * Minimal custom compression pool: prepends a one-byte sentinel so we can
 * verify the byte stream actually went through this pool's compress() and
 * decompress(). Not a real compressor.
 */
private object SentinelCompressionPool : CompressionPool {
    private const val SENTINEL: Byte = 0x42

    override fun name(): String = "sentinel"

    override fun compress(input: Buffer): Buffer {
        val out = Buffer()
        out.writeByte(SENTINEL.toInt())
        out.writeAll(input)
        return out
    }

    override fun decompress(input: Buffer): Buffer {
        val first = input.readByte()
        require(first == SENTINEL) { "expected sentinel byte, got $first" }
        val out = Buffer()
        out.writeAll(input)
        return out
    }
}
