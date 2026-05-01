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
import com.connectrpc.server.ServerInterceptor
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Verifies that registered [ServerInterceptor]s wrap the handler invocation
 * and run in the correct (registration) order.
 */
class InterceptorTest {

    @Test
    fun unaryInterceptorRunsBeforeAndAfterHandler() {
        val sawRequest = AtomicBoolean(false)
        val sawResponse = AtomicBoolean(false)
        val handlerRan = AtomicBoolean(false)

        val interceptor = object : ServerInterceptor {
            override fun <Req : Any, Res : Any> wrapUnary(
                next: UnaryHandler<Req, Res>,
            ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                    sawRequest.set(true)
                    val res = next.handle(request, ctx)
                    sawResponse.set(true)
                    return res
                }
            }
        }

        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Unary",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                handlerRan.set(true)
                // Interceptor must have observed the request BEFORE we got here.
                assertThat(sawRequest.get()).isTrue()
                assertThat(sawResponse.get())
                    .describedAs("response observation must come AFTER handler returns")
                    .isFalse()
                return TestMessage("ok")
            }
        }

        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .interceptor(interceptor)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
            }
        }

        assertThat(handlerRan.get()).isTrue()
        assertThat(sawRequest.get()).isTrue()
        assertThat(sawResponse.get()).isTrue()
    }

    @Test
    fun multipleInterceptorsRunInRegistrationOrder() {
        val order = mutableListOf<String>()

        fun named(name: String): ServerInterceptor = object : ServerInterceptor {
            override fun <Req : Any, Res : Any> wrapUnary(
                next: UnaryHandler<Req, Res>,
            ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                    synchronized(order) { order += "$name-in" }
                    val res = next.handle(request, ctx)
                    synchronized(order) { order += "$name-out" }
                    return res
                }
            }
        }

        val handler = object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec(
                "test.v1.TestService/Unary",
                TestMessage::class,
                TestMessage::class,
                StreamType.UNARY,
            )

            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                synchronized(order) { order += "handler" }
                return TestMessage("ok")
            }
        }

        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .interceptor(named("A"))
            .interceptor(named("B"))
            .interceptor(named("C"))
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
            }
        }

        // First-registered runs outermost: A enters first, exits last.
        assertThat(order).containsExactly(
            "A-in", "B-in", "C-in", "handler", "C-out", "B-out", "A-out",
        )
    }

    @Test
    fun perProcedureInterceptorOnlyAppliesToRegisteredProcedure() {
        val interceptedA = AtomicInteger(0)
        val interceptedB = AtomicInteger(0)

        fun counting(counter: AtomicInteger): ServerInterceptor = object : ServerInterceptor {
            override fun <Req : Any, Res : Any> wrapUnary(
                next: UnaryHandler<Req, Res>,
            ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                    counter.incrementAndGet()
                    return next.handle(request, ctx)
                }
            }
        }

        fun handler(path: String): UnaryHandler<TestMessage, TestMessage> =
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    path,
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    TestMessage("ok")
            }

        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler("test.v1.TestService/A"), listOf(counting(interceptedA)))
            .register(handler("test.v1.TestService/B"))
            .build()

        TestServer.start(registry).use { server ->
            fun hit(path: String) {
                val req = Request.Builder()
                    .url("${server.baseUrl}/$path")
                    .header("Content-Type", "application/proto")
                    .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                    .build()
                newTestClient().newCall(req).execute().use { it.close() }
            }
            hit("test.v1.TestService/A")
            hit("test.v1.TestService/B")
            hit("test.v1.TestService/A")
        }

        // Procedure A's per-procedure interceptor saw exactly its own calls.
        assertThat(interceptedA.get()).isEqualTo(2)
        // Procedure B's counter is irrelevant (no interceptor); just sanity.
        assertThat(interceptedB.get()).isEqualTo(0)
    }
}
