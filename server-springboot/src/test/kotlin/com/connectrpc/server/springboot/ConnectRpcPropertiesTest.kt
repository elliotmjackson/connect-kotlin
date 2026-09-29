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
import okhttp3.Dispatcher
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Bean
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Each `connectrpc.*` limit and switch reaches the calls it governs. */
class ConnectRpcPropertiesTest {
    companion object {
        private val running = AtomicInteger()
        private val peak = AtomicInteger()

        @ClassRule
        @JvmField
        val server = SpringBootServer(
            TestApp::class.java,
            "connectrpc.read-max-bytes=128",
            "connectrpc.send-max-bytes=64",
            "connectrpc.compress-min-bytes=16",
            "connectrpc.require-connect-protocol-header=true",
            "connectrpc.handler-threads=1",
            "connectrpc.max-timeout=2s",
        )
    }

    private val client = OkHttpClient.Builder().callTimeout(10, TimeUnit.SECONDS).build()

    @Test
    fun limitsAndSwitchesApply() {
        data class Case(val name: String, val body: Int, val headers: Map<String, String>, val status: Int, val code: String?, val contentEncoding: String?)
        val version = mapOf("Connect-Protocol-Version" to "1")
        val cases = listOf(
            Case("within limits", 8, version, 200, null, null),
            Case("missing Connect-Protocol-Version", 8, emptyMap(), 400, "invalid_argument", null),
            Case("request over read-max-bytes", 129, version, 429, "resource_exhausted", null),
            Case("response over send-max-bytes", 100, version, 429, "resource_exhausted", null),
            Case("response below compress-min-bytes", 8, version + ("Accept-Encoding" to "gzip"), 200, null, null),
            Case("response at least compress-min-bytes", 32, version + ("Accept-Encoding" to "gzip"), 200, null, "gzip"),
        )
        for (case in cases) {
            val request = Request.Builder()
                .url("http://127.0.0.1:${server.port}/test.v1.TestService/Echo")
                .apply { case.headers.forEach { (name, value) -> header(name, value) } }
                .post(ByteArray(case.body).toRequestBody("application/proto".toMediaType()))
                .build()
            client.newCall(request).execute().use { response ->
                assertThat(response.code).describedAs(case.name).isEqualTo(case.status)
                assertThat(response.header("Content-Encoding")).describedAs(case.name).isEqualTo(case.contentEncoding)
                if (case.code != null) assertThat(response.body!!.string()).describedAs(case.name).contains("\"code\":\"${case.code}\"")
            }
        }
    }

    /** `connectrpc.max-timeout` ends a call whose client sent no timeout. */
    @Test
    fun maxTimeoutBoundsCallsWithoutTimeout() {
        val request = Request.Builder()
            .url("http://127.0.0.1:${server.port}/test.v1.TestService/Slow")
            .header("Connect-Protocol-Version", "1")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(request).execute().use { response ->
            assertThat(response.code).isEqualTo(504)
            assertThat(response.body!!.string()).contains("\"code\":\"deadline_exceeded\"")
        }
    }

    /** `connectrpc.handler-threads` bounds how many handlers run at once. */
    @Test
    fun handlerThreadsBoundsConcurrentHandlers() {
        val calls = 4
        val concurrent = client.newBuilder()
            .dispatcher(Dispatcher().apply { maxRequestsPerHost = calls })
            .build()
        val pool = Executors.newFixedThreadPool(calls)
        try {
            val results = List(calls) {
                pool.submit<Int> {
                    val request = Request.Builder()
                        .url("http://127.0.0.1:${server.port}/test.v1.TestService/Block")
                        .header("Connect-Protocol-Version", "1")
                        .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                        .build()
                    concurrent.newCall(request).execute().use { it.code }
                }
            }
            assertThat(results.map { it.get(10, TimeUnit.SECONDS) }).containsOnly(200)
            assertThat(peak.get()).isEqualTo(1)
        } finally {
            pool.shutdownNow()
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Echo", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = request
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Block", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        peak.accumulateAndGet(running.incrementAndGet(), ::maxOf)
                        // Blocks the thread, as JDBC or a blocking HTTP client would.
                        Thread.sleep(200)
                        running.decrementAndGet()
                        return request
                    }
                },
            )
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Slow", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        delay(30_000)
                        return request
                    }
                },
            )
            .build()
    }
}
