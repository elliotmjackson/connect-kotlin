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
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
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
    classes = [ClientStreamBidiTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class ClientStreamBidiTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun connectClientStreamConcatenates() {
        val client = OkHttpClient.Builder()
            .callTimeout(5, TimeUnit.SECONDS)
            .build()
        val body = Buffer().apply {
            write(envelope(0, "alpha".toByteArray()))
            write(envelope(0, "beta".toByteArray()))
            write(envelope(0, "gamma".toByteArray()))
        }.readByteArray()
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Sum")
            .header("Content-Type", "application/connect+proto")
            .post(body.toRequestBody("application/connect+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            val src = Buffer().write(response.body!!.bytes())
            val msg = readEnvelope(src) ?: error("expected single response message")
            assertThat(String(msg.payload)).isEqualTo("alpha,beta,gamma")
            val trailer = readEnvelope(src) ?: error("expected end-stream envelope")
            assertThat(trailer.flags and 0x02).isEqualTo(0x02)
        }
    }

    @Test
    fun connectBidiHalfDuplexEchoesEachInput() {
        val client = OkHttpClient.Builder()
            .callTimeout(5, TimeUnit.SECONDS)
            .build()
        val body = Buffer().apply {
            write(envelope(0, "x".toByteArray()))
            write(envelope(0, "y".toByteArray()))
        }.readByteArray()
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/EchoEach")
            .header("Content-Type", "application/connect+proto")
            .post(body.toRequestBody("application/connect+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            val src = Buffer().write(response.body!!.bytes())
            val first = readEnvelope(src) ?: error("expected first echo")
            assertThat(String(first.payload)).isEqualTo("x")
            val second = readEnvelope(src) ?: error("expected second echo")
            assertThat(String(second.payload)).isEqualTo("y")
            val trailer = readEnvelope(src) ?: error("expected end-stream envelope")
            assertThat(trailer.flags and 0x02).isEqualTo(0x02)
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val sum = object : ClientStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Sum",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.CLIENT,
                )

                override suspend fun handle(
                    stream: ClientMessageStream<TestMessage>,
                    ctx: HandlerContext,
                ): TestMessage {
                    val parts = mutableListOf<String>()
                    while (true) {
                        val msg = stream.receive() ?: break
                        parts += msg.text()
                    }
                    return TestMessage(parts.joinToString(","))
                }
            }
            val echoEach = object : BidiStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/EchoEach",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.BIDI,
                )

                override suspend fun handle(
                    stream: BidiStream<TestMessage, TestMessage>,
                    ctx: HandlerContext,
                ) {
                    while (true) {
                        val msg = stream.receive() ?: break
                        stream.send(msg)
                    }
                }
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(sum)
                .register(echoEach)
                .build()
        }
    }
}
