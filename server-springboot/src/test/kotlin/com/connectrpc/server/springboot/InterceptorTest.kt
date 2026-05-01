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
import com.connectrpc.server.ServerInterceptor
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
    classes = [InterceptorTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class InterceptorTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun globalInterceptorWrapsHandler() {
        callsViaInterceptor.set(0)
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/proto")
            .post("hi".toByteArray().toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
            assertThat(response.body!!.string()).isEqualTo("intercepted:hi")
        }
        assertThat(callsViaInterceptor.get()).isEqualTo(1)
    }

    @Test
    fun perProcedureInterceptorOnlyAppliesToItsRoute() {
        perProcedureCalls.set(0)
        val protected = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Protected")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(protected).execute().use { it.close() }
        assertThat(perProcedureCalls.get())
            .describedAs("per-procedure interceptor must run for /Protected")
            .isEqualTo(1)

        val unrelated = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(unrelated).execute().use { it.close() }
        assertThat(perProcedureCalls.get())
            .describedAs("per-procedure interceptor must not run for unrelated routes")
            .isEqualTo(1)
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val unary = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Unary",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    request
            }
            val protected_ = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Protected",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    request
            }

            val global = object : ServerInterceptor {
                override fun <Req : Any, Res : Any> wrapUnary(
                    next: UnaryHandler<Req, Res>,
                ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
                    override val methodSpec = next.methodSpec
                    override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                        callsViaInterceptor.incrementAndGet()
                        @Suppress("UNCHECKED_CAST")
                        val req = request as TestMessage
                        @Suppress("UNCHECKED_CAST")
                        return next.handle(TestMessage("intercepted:${req.text()}") as Req, ctx)
                    }
                }
            }

            val perProcedure = object : ServerInterceptor {
                override fun <Req : Any, Res : Any> wrapUnary(
                    next: UnaryHandler<Req, Res>,
                ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
                    override val methodSpec = next.methodSpec
                    override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                        perProcedureCalls.incrementAndGet()
                        return next.handle(request, ctx)
                    }
                }
            }

            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .interceptor(global)
                .register(unary)
                .register(protected_, listOf(perProcedure))
                .build()
        }
    }

    companion object {
        val callsViaInterceptor = java.util.concurrent.atomic.AtomicInteger(0)
        val perProcedureCalls = java.util.concurrent.atomic.AtomicInteger(0)
    }
}
