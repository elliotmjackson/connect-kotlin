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
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
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
}
