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

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import java.util.concurrent.TimeUnit

/** Without `connectrpc.read-max-bytes`, request messages are limited to 4 MiB. */
class DefaultReadLimitTest {
    private val client = OkHttpClient.Builder().callTimeout(20, TimeUnit.SECONDS).build()

    @Test
    fun messageOfExactlyTheDefaultLimitIsAccepted() {
        call(DEFAULT_LIMIT).use { response ->
            assertThat(response.code).isEqualTo(200)
            assertThat(response.body!!.bytes().size).isEqualTo(DEFAULT_LIMIT + "pong:".length)
        }
    }

    @Test
    fun messageOneByteOverTheDefaultLimitIsResourceExhausted() {
        call(DEFAULT_LIMIT + 1).use { response ->
            assertThat(response.code).isEqualTo(429)
            assertThat(response.body!!.string()).contains("\"code\":\"resource_exhausted\"")
        }
    }

    private fun call(size: Int) = client.newCall(
        Request.Builder()
            .url("http://127.0.0.1:${server.port}/test.v1.TestService/Unary")
            .post(ByteArray(size) { 'x'.code.toByte() }.toRequestBody("application/proto".toMediaType()))
            .build(),
    ).execute()

    companion object {
        private const val DEFAULT_LIMIT = 4 * 1024 * 1024

        @ClassRule
        @JvmField
        val server = SpringBootServer(ServletBridgeTest.TestApp::class.java)
    }
}
