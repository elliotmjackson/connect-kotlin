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

package com.connectrpc.server.springboot

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import okio.BufferedSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.TimeUnit

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [ServerStreamTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class ServerStreamTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun connectServerStreamThreeMessages() {
        val client = OkHttpClient.Builder()
            .callTimeout(5, TimeUnit.SECONDS)
            .build()
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Stream")
            .header("Content-Type", "application/connect+proto")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            val bodyBytes = response.body!!.bytes()
            assertThat(response.isSuccessful)
                .describedAs("status=${response.code} body=${String(bodyBytes)}")
                .isTrue()
            val source = Buffer().write(bodyBytes)
            val first = readEnvelope(source) ?: error("expected message envelope")
            assertThat(String(first.payload)).isEqualTo("one")
            val second = readEnvelope(source) ?: error("expected message envelope")
            assertThat(String(second.payload)).isEqualTo("two")
            val third = readEnvelope(source) ?: error("expected message envelope")
            assertThat(String(third.payload)).isEqualTo("three")
            val trailer = readEnvelope(source) ?: error("expected trailer envelope")
            assertThat(trailer.flags and 0x02).isEqualTo(0x02)
        }
    }

    @Test
    fun grpcServerStreamSetsTrailers() {
        val client = OkHttpClient.Builder()
            .callTimeout(5, TimeUnit.SECONDS)
            .build()
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Stream")
            .header("Content-Type", "application/grpc+proto")
            .header("TE", "trailers")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            val source = response.body!!.source()
            // Three message envelopes; gRPC trailers ride in HTTP trailers.
            repeat(3) { readEnvelope(source) ?: error("missing message envelope") }
            // grpc-status comes back as an HTTP trailer.
            val status = response.trailers()["grpc-status"]
            assertThat(status).describedAs("expected grpc-status trailer").isEqualTo("0")
        }
    }

    @Test
    fun grpcWebServerStreamSendsTrailerEnvelope() {
        val client = OkHttpClient.Builder()
            .callTimeout(5, TimeUnit.SECONDS)
            .build()
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Stream")
            .header("Content-Type", "application/grpc-web+proto")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc-web+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            val source = response.body!!.source()
            // Three messages then a gRPC-Web trailer envelope (flag 0x80).
            repeat(3) { readEnvelope(source) ?: error("missing message envelope") }
            val trailer = readEnvelope(source) ?: error("missing trailer envelope")
            assertThat(trailer.flags and 0x80).isEqualTo(0x80)
            val text = String(trailer.payload)
            assertThat(text).contains("grpc-status: 0")
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
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
                    stream.send(TestMessage("one"))
                    stream.send(TestMessage("two"))
                    stream.send(TestMessage("three"))
                }
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(handler)
                .build()
        }
    }
}

internal fun envelope(flags: Int, payload: ByteArray): ByteArray {
    val out = ByteArray(5 + payload.size)
    out[0] = flags.toByte()
    out[1] = (payload.size ushr 24).toByte()
    out[2] = (payload.size ushr 16).toByte()
    out[3] = (payload.size ushr 8).toByte()
    out[4] = payload.size.toByte()
    System.arraycopy(payload, 0, out, 5, payload.size)
    return out
}

internal data class ReadEnvelope(val flags: Int, val payload: ByteArray)

internal fun readEnvelope(source: BufferedSource): ReadEnvelope? {
    if (source.exhausted()) return null
    val flags = source.readByte().toInt() and 0xff
    val len = source.readInt()
    val payload = source.readByteArray(len.toLong())
    return ReadEnvelope(flags, payload)
}
