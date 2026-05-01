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
    classes = [ProtocolValidationTest.TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["connectrpc.requireConnectProtocolHeader=true"],
)
class ProtocolValidationTest {

    @Value("\${local.server.port}")
    private var port: Int = 0

    private val client get() = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun unsupportedContentTypeReturns415() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/xml")
            .post(ByteArray(0).toRequestBody("application/xml".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(415)
        }
    }

    @Test
    fun unknownProcedureReturns404() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Missing")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(404)
        }
    }

    @Test
    fun nonPostNonGetVerbReturns405() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/proto")
            .delete(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(405)
        }
    }

    @Test
    fun connectRejectsMissingProtocolVersionHeader() {
        // requireConnectProtocolHeader=true via @SpringBootTest property.
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/proto")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isFalse()
            val body = response.body!!.string()
            assertThat(body).contains("invalid_argument")
            assertThat(body).contains("Connect-Protocol-Version")
        }
    }

    @Test
    fun connectAcceptsProtocolVersionOne() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/proto")
            .header("Connect-Protocol-Version", "1")
            .post("hi".toByteArray().toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.isSuccessful).isTrue()
        }
    }

    @Test
    fun grpcRejectsMissingTeTrailers() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Unary")
            .header("Content-Type", "application/grpc+proto")
            // no TE: trailers
            .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            // Gets surfaced as a regular response header per our trailer-only workaround.
            val status = response.header("grpc-status") ?: response.trailers()["grpc-status"]
            assertThat(status).isNotNull().isNotEqualTo("0")
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry {
            val handler = object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Unary",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
                    request
            }
            return HandlerRegistry.builder()
                .codec(TestSerializationStrategy)
                .register(handler)
                .build()
        }
    }
}
