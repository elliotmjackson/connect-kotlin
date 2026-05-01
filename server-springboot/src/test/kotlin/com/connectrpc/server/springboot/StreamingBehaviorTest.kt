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
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
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
    classes = [StreamingBehaviorTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class StreamingBehaviorTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(15, TimeUnit.SECONDS).build()

    /**
     * Server-stream messages should arrive incrementally — the client must
     * receive the first envelope before the handler finishes producing the
     * later ones. We assert this by giving the handler a perceptible delay
     * between `send`s and watching the time the first envelope arrives.
     */
    @Test
    fun serverStreamEmitsIncrementally() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Slow")
            .header("Content-Type", "application/connect+proto")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            val source = response.body!!.source()
            val started = System.currentTimeMillis()
            val first = readEnvelope(source) ?: error("expected first envelope")
            val firstAt = System.currentTimeMillis() - started
            assertThat(String(first.payload)).isEqualTo("first")
            // First envelope should land well before the handler's 800ms total.
            assertThat(firstAt)
                .describedAs("first envelope must arrive incrementally, not after all sends complete")
                .isLessThan(700)
            val second = readEnvelope(source) ?: error("expected second envelope")
            assertThat(String(second.payload)).isEqualTo("second")
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val slow = object : ServerStreamHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Slow",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.SERVER,
                )

                override suspend fun handle(
                    request: TestMessage,
                    ctx: HandlerContext,
                    stream: ServerMessageStream<TestMessage>,
                ) {
                    stream.send(TestMessage("first"))
                    delay(800)
                    stream.send(TestMessage("second"))
                }
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(slow)
                .build()
        }
    }
}
