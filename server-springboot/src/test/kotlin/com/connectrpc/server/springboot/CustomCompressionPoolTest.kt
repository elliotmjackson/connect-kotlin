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
import com.connectrpc.compression.CompressionPool
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
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
    classes = [CustomCompressionPoolTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
)
class CustomCompressionPoolTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    @Test
    fun customPoolIsRegisteredForBothInboundAndOutbound() {
        val largePayload = ByteArray(64_000) { (it % 256).toByte() }
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Echo")
            .header("Content-Type", "application/proto")
            .header("Accept-Encoding", "sentinel")
            .post(largePayload.toRequestBody("application/proto".toMediaType()))
            .build()
        OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()
            .newCall(req).execute().use { response ->
                assertThat(response.header("Content-Encoding")).isEqualTo("sentinel")
                val body = response.body!!.bytes()
                val decoded = SentinelCompressionPool.decompress(Buffer().write(body)).readByteArray()
                assertThat(decoded).isEqualTo(largePayload)
            }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val handler = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Echo",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    request
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .compressionPool(SentinelCompressionPool)
                .register(handler)
                .build()
        }
    }
}

/** Marker pool — prepends a sentinel byte; verifies registry routing by name. */
internal object SentinelCompressionPool : CompressionPool {
    private const val SENTINEL: Byte = 0x42

    override fun name(): String = "sentinel"

    override fun compress(input: Buffer): Buffer = Buffer().apply {
        writeByte(SENTINEL.toInt())
        writeAll(input)
    }

    override fun decompress(input: Buffer): Buffer {
        val first = input.readByte()
        require(first == SENTINEL) { "expected sentinel byte, got $first" }
        return Buffer().apply { writeAll(input) }
    }
}
