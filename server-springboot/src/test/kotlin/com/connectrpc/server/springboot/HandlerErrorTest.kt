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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
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
    classes = [HandlerErrorTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class HandlerErrorTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun connectExceptionPassesThroughVerbatim() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Forbidden")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(403)
            val body = response.body!!.string()
            assertThat(body).contains("permission_denied")
            assertThat(body).contains("not allowed")
        }
    }

    @Test
    fun npeMapsToUnknown() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Crash")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isFalse()
            val body = response.body!!.string()
            assertThat(body).contains("unknown")
            // Don't leak internal details — but the message field is fine.
            assertThat(body).contains("oh no")
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val forbidden = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Forbidden",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    throw ConnectException(Code.PERMISSION_DENIED, "not allowed")
            }
            val crash = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Crash",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    throw IllegalStateException("oh no")
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(forbidden)
                .register(crash)
                .build()
        }
    }
}
