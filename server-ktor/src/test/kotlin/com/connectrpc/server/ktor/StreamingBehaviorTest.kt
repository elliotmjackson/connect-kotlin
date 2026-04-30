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
     * Full-duplex bidi: the handler does one receive, one send, repeat. A
     * client that sends req-1 and waits for resp-1 before sending req-2
     * must complete; the server cannot wait for the request stream to close
     * before producing responses.
     *
     * Currently fails: full-duplex is excluded from server-config.yaml because
     * Ktor commits response headers eagerly on `respond()` and our buffered
     * bidi adapter would deadlock the harness.
     */
    @Test
    @Ignore("TDD target: full-duplex bidi requires real-time streaming + lazy header commit")
    fun fullDuplexBidiInterleaves() {
        val handler = object : BidiStreamHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Bidi",
                TestMessage::class,
                TestMessage::class,
                StreamType.BIDI,
            )

            override suspend fun handle(
                stream: BidiStream<TestMessage, TestMessage>,
                ctx: HandlerContext,
            ) {
                while (true) {
                    val req = stream.receive() ?: break
                    stream.send(TestMessage("echo-${req.text()}"))
                }
            }
        }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        // Driving an interleaved bidi from OkHttp requires a duplex request
        // body; left as an exercise for the implementer (this test will be
        // un-ignored when the streaming adapter lands and we pick a duplex
        // client mechanism).
        TestServer.start(registry).use { _ ->
            // intentionally minimal — flesh out when implementation lands
            assertThat(true).isFalse() // placeholder so the test fails until written
        }
    }
}
