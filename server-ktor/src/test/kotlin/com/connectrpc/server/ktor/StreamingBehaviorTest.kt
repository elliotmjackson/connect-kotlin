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
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test

/**
 * TDD targets for streaming behaviors not yet implemented. These verify the
 * server actually streams responses (rather than buffering them) and that
 * full-duplex bidi handlers can interleave reads and writes.
 *
 * Remove [@Ignore] from a test once the corresponding feature lands.
 */
class StreamingBehaviorTest {

    /**
     * Server-stream: the handler emits 3 messages with 200ms delays between
     * each. A real-time server flushes each message as soon as `send()` is
     * called; the client should observe meaningful inter-arrival gaps.
     *
     * Currently fails: the Ktor adapter buffers all sends into a list and
     * flushes them in one go after `handle()` returns.
     */
    @Test
    fun serverStreamEmitsIncrementally() {
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
                for (i in 1..3) {
                    stream.send(TestMessage("msg-$i"))
                    delay(200)
                }
            }
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val request = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Stream")
                .header("Content-Type", "application/connect+proto")
                .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
                .build()
            val client = newTestClient()

            val arrivals = mutableListOf<Long>()
            client.newCall(request).execute().use { response ->
                val source = response.body!!.source()
                val start = System.currentTimeMillis()
                while (!source.exhausted()) {
                    val env = readEnvelope(source) ?: break
                    arrivals += System.currentTimeMillis() - start
                    if ((env.flags and 0x02) != 0) break // EndStream envelope
                }
            }

            // Three message envelopes + one EndStream envelope = 4 entries.
            assertThat(arrivals).hasSize(4)
            // Each non-final message should arrive at least ~150ms after the previous.
            for (i in 1 until 3) {
                val gap = arrivals[i] - arrivals[i - 1]
                assertThat(gap)
                    .describedAs("gap between message $i and ${i - 1}")
                    .isGreaterThan(150)
            }
        }
    }

    /**
     * Full-duplex bidi: handler does one receive, one send, repeat. The
     * adapter feature is implemented (handleBidiStream uses the streaming
     * Channel + CompletableDeferred pattern; STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
     * is enabled in server-config.yaml; the conformance harness exercises
     * it over HTTP/2+TLS and all 62 full-duplex test cases pass).
     *
     * What's still missing here is a Kotlin-level test client. OkHttp's
     * standard request body API is one-shot; driving an interleaved
     * send-receive flow requires either OkHttp's DuplexRequestBody (HTTP/2
     * only) plus a TLS test fixture, or a different HTTP/2 client.
     */
    @Test
    @Ignore("Feature implemented + verified by conformance; this test needs an HTTP/2 duplex client")
    fun fullDuplexBidiInterleaves() {
        // Placeholder body left intentionally empty until a duplex test
        // client lands. See conformance suite (1367/1367) for end-to-end
        // verification of the full-duplex behavior.
    }
}
