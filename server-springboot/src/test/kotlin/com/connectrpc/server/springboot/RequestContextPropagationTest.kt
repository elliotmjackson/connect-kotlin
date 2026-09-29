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
import jakarta.servlet.Filter
import jakarta.servlet.http.HttpServletRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.slf4j.MDC
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.web.DefaultSecurityFilterChain
import org.springframework.security.web.FilterChainProxy
import org.springframework.security.web.authentication.AuthenticationFilter
import org.springframework.security.web.context.RequestAttributeSecurityContextRepository
import org.springframework.security.web.context.SecurityContextHolderFilter
import org.springframework.security.web.util.matcher.AnyRequestMatcher
import org.springframework.web.context.request.RequestAttributes
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.util.concurrent.TimeUnit

/**
 * Handlers see the servlet request's Spring context even though they run on
 * coroutine threads after the filter chain has returned and cleared it.
 */
class RequestContextPropagationTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java)
    }

    private val client = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun unaryHandlerSeesRequestSecurityAndMdcContext() {
        client.newCall(request("Whoami", "alice", "req-1")).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            assertThat(response.body!!.string()).isEqualTo("header=alice;attribute=kept;user=alice;mdc=req-1")
        }
    }

    @Test
    fun serverStreamHandlerSeesContextAcrossSuspensions() {
        client.newCall(request("WhoamiStream", "bob", "req-2", streaming = true)).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            val source = response.body!!.source()
            val messages = generateSequence { readEnvelope(source) }.filter { it.flags == 0 }.map { String(it.payload) }.toList()
            assertThat(messages).containsExactly(
                "header=bob;attribute=kept;user=bob;mdc=req-2",
                "header=bob;attribute=kept;user=bob;mdc=req-2",
            )
        }
    }

    /** Coroutine threads are shared; one call's context must not stay on them. */
    @Test
    fun contextDoesNotLeakIntoLaterCalls() {
        repeat(20) { client.newCall(request("Whoami", "carol", "req-3")).execute().close() }
        client.newCall(request("Whoami", user = null, requestId = null)).execute().use { response ->
            assertThat(response.body!!.string()).isEqualTo("header=null;attribute=kept;user=null;mdc=null")
        }
    }

    private fun request(method: String, user: String?, requestId: String?, streaming: Boolean = false): Request {
        val contentType = if (streaming) "application/connect+proto" else "application/proto"
        val body = if (streaming) envelope(0, ByteArray(0)) else ByteArray(0)
        return Request.Builder()
            .url("http://127.0.0.1:${server.port}/test.v1.TestService/$method")
            .apply { if (user != null) header("X-User", user) }
            .apply { if (requestId != null) header("X-Request-Id", requestId) }
            .post(body.toRequestBody(contentType.toMediaType()))
            .build()
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        /**
         * A Spring Security filter chain that authenticates from `X-User`. Like
         * every `FilterChainProxy`, it clears `SecurityContextHolder` when the
         * chain returns.
         */
        @Bean
        open fun springSecurityFilterChain(): Filter {
            val manager = AuthenticationManager { UsernamePasswordAuthenticationToken.authenticated(it.name, null, emptyList()) }
            val authentication = AuthenticationFilter(manager) { request ->
                request.getHeader("X-User")?.let { UsernamePasswordAuthenticationToken.unauthenticated(it, null) }
            }
            authentication.setSuccessHandler { _, _, _ -> }
            return FilterChainProxy(
                DefaultSecurityFilterChain(
                    AnyRequestMatcher.INSTANCE,
                    SecurityContextHolderFilter(RequestAttributeSecurityContextRepository()),
                    authentication,
                ),
            )
        }

        @Bean
        open fun mdcFilter(): Filter = Filter { request, response, chain ->
            (request as HttpServletRequest).getHeader("X-Request-Id")?.let { MDC.put("requestId", it) }
            try {
                chain.doFilter(request, response)
            } finally {
                MDC.remove("requestId")
            }
        }

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Whoami", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                        RequestContextHolder.currentRequestAttributes().setAttribute("probe", "kept", RequestAttributes.SCOPE_REQUEST)
                        delay(10)
                        return TestMessage(withContext(Dispatchers.IO) { describe() })
                    }
                },
            )
            .register(
                object : ServerStreamHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/WhoamiStream", TestMessage::class, TestMessage::class, StreamType.SERVER)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext, stream: ServerMessageStream<TestMessage>) {
                        RequestContextHolder.currentRequestAttributes().setAttribute("probe", "kept", RequestAttributes.SCOPE_REQUEST)
                        stream.send(TestMessage(describe()))
                        delay(50)
                        stream.send(TestMessage(withContext(Dispatchers.IO) { describe() }))
                    }
                },
            )
            .build()

        private fun describe(): String {
            val attributes = RequestContextHolder.currentRequestAttributes() as ServletRequestAttributes
            return "header=${attributes.request.getHeader("X-User")}" +
                ";attribute=${attributes.getAttribute("probe", RequestAttributes.SCOPE_REQUEST)}" +
                ";user=${SecurityContextHolder.getContext().authentication?.name}" +
                ";mdc=${MDC.get("requestId")}"
        }
    }
}
