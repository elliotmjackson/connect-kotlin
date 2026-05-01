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
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class MessageSizeTest {

    private fun registry() = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Unary",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = request
            },
        )
        .build()

    @Test
    fun underLimitSucceeds() {
        TestServer.start(registry(), maxReceiveMessageSize = 128).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .post(ByteArray(100).toRequestBody("application/proto".toMediaType()))
                .build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
            }
        }
    }

    @Test
    fun overLimitReturnsResourceExhausted() {
        TestServer.start(registry(), maxReceiveMessageSize = 128).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .post(ByteArray(256).toRequestBody("application/proto".toMediaType()))
                .build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.code).isEqualTo(429)
                val body = response.body!!.string()
                assertThat(body).contains("resource_exhausted")
                assertThat(body).contains("128")
            }
        }
    }

    @Test
    fun zeroLimitMeansUnlimited() {
        TestServer.start(registry(), maxReceiveMessageSize = 0).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                .post(ByteArray(1_000_000).toRequestBody("application/proto".toMediaType()))
                .build()
            newTestClient().newCall(req).execute().use { response ->
                assertThat(response.isSuccessful).isTrue()
            }
        }
    }
}
