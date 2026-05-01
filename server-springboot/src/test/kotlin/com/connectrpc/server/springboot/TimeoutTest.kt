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
import com.connectrpc.server.UnaryHandler
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
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [TimeoutTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class TimeoutTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(10, TimeUnit.SECONDS).build()

    @Test
    fun connectTimeoutCancelsHandler() {
        sawCancellation.set(false)
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Slow")
            .header("Content-Type", "application/proto")
            .header("Connect-Timeout-Ms", "100")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        val started = System.currentTimeMillis()
        client.newCall(req).execute().use { response ->
            val elapsed = System.currentTimeMillis() - started
            assertThat(response.code).isEqualTo(504)
            assertThat(elapsed)
                .describedAs("response time should be near the 100ms deadline, not the 5s handler delay")
                .isLessThan(2000)
            assertThat(response.body!!.string()).contains("deadline_exceeded")
        }
        assertThat(sawCancellation.get())
            .describedAs("handler must observe CancellationException")
            .isTrue()
    }

    @Test
    fun grpcTimeoutHeaderEnforced() {
        sawCancellation.set(false)
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Slow")
            .header("Content-Type", "application/grpc+proto")
            .header("TE", "trailers")
            .header("Grpc-Timeout", "100m")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            val status = response.header("grpc-status") ?: response.trailers()["grpc-status"]
            // gRPC code 4 = DEADLINE_EXCEEDED
            assertThat(status).isEqualTo("4")
        }
        assertThat(sawCancellation.get()).isTrue()
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val slow = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Slow",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                    try {
                        delay(5_000)
                        return TestMessage("late")
                    } catch (ex: kotlinx.coroutines.CancellationException) {
                        sawCancellation.set(true)
                        throw ex
                    }
                }
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(slow)
                .build()
        }
    }

    companion object {
        val sawCancellation = AtomicBoolean(false)
    }
}
