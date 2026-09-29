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
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import java.io.IOException
import java.util.concurrent.TimeUnit

class HandlerErrorTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java)
    }

    private val port: Int get() = server.port

    private val client get() = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    /** A handler [Error] is not caught by the engine; the call must still fail, not answer an empty 200. */
    @Test
    fun unaryHandlerErrorFailsTheResponse() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Todo")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(500)
        }
    }

    /**
     * After the response has started, the exchange is aborted: the body breaks off,
     * never ending with the end-stream message clients read as the call's status.
     */
    @Test
    fun streamingHandlerErrorAbortsTheResponse() {
        val req = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/TodoStream")
            .post(envelope(0, ByteArray(0)).toRequestBody("application/connect+proto".toMediaType()))
            .build()
        client.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            val source = response.body!!.source()
            val flags = mutableListOf<Int>()
            val failure = catchThrowable { generateSequence { readEnvelope(source) }.forEach { flags += it.flags } }
            assertThat(failure).isInstanceOf(IOException::class.java)
            // Only messages, if the one sent arrived before the abort.
            assertThat(flags).allMatch { it == 0 }
        }
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Todo", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = TODO("not written yet")
                },
            )
            .register(
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/TodoStream", TestMessage::class, TestMessage::class, StreamType.SERVER)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        stream.send(TestMessage("first"))
                        TODO("not written yet")
                    }
                },
            )
            .build()
    }
}
