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
import com.connectrpc.MethodSpec
import com.connectrpc.ProtocolClientConfig
import com.connectrpc.ResponseMessage
import com.connectrpc.StreamType
import com.connectrpc.impl.ProtocolClient
import com.connectrpc.okhttp.ConnectOkHttpClient
import com.connectrpc.protocols.NetworkProtocol
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import kotlinx.coroutines.runBlocking
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
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.TimeUnit

/**
 * A Spring MVC application with Connect services registered alongside `@RestController` routes.
 */
class MvcCoexistenceTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java)

        @ClassRule
        @JvmField
        val prefixedServer = SpringBootServer(TestApp::class.java, "connectrpc.path-prefix=/rpc/")

        @ClassRule
        @JvmField
        val contextPathServer = SpringBootServer(TestApp::class.java, "server.servlet.context-path=/api")

        private val UNARY = MethodSpec(
            "test.v1.TestService/Unary",
            TestMessage::class,
            TestMessage::class,
            StreamType.UNARY,
        )
    }

    private val http = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    private fun connectClient(baseUrl: String) = ProtocolClient(
        ConnectOkHttpClient(http),
        ProtocolClientConfig(
            host = baseUrl,
            serializationStrategy = TestSerializationStrategy,
            networkProtocol = NetworkProtocol.CONNECT,
        ),
    )

    private fun root() = "http://127.0.0.1:${server.port}"

    @Test
    fun restControllerJsonRouteStillServed() {
        val req = Request.Builder()
            .url("${root()}/card-authorization/evaluate")
            .post("""{"amountMinor":1250}""".toRequestBody("application/json".toMediaType()))
            .build()
        http.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            assertThat(response.header("Content-Type")).startsWith("application/json")
            assertThat(response.body!!.string()).isEqualTo("""{"approved":true,"amountMinor":1250}""")
        }
    }

    @Test
    fun restControllerGetRouteStillServed() {
        val req = Request.Builder().url("${root()}/health").get().build()
        http.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            assertThat(response.body!!.string()).isEqualTo("ok")
        }
    }

    @Test
    fun unknownMethodUnderRegisteredServiceIsConnectUnimplemented() = runBlocking<Unit> {
        val missing = MethodSpec(
            "test.v1.TestService/Missing",
            TestMessage::class,
            TestMessage::class,
            StreamType.UNARY,
        )
        val response = connectClient(root()).unary(TestMessage("ping"), emptyMap(), missing)
        assertThat(response).isInstanceOf(ResponseMessage.Failure::class.java)
        assertThat((response as ResponseMessage.Failure).cause.code).isEqualTo(Code.UNIMPLEMENTED)

        // https://connectrpc.com/docs/protocol#http-to-error-code: 404 → unimplemented.
        val raw = Request.Builder()
            .url("${root()}/test.v1.TestService/Missing")
            .post("ping".toByteArray().toRequestBody("application/proto".toMediaType()))
            .build()
        http.newCall(raw).execute().use { rawResponse ->
            assertThat(rawResponse.code).isEqualTo(404)
            assertThat(rawResponse.body!!.string()).doesNotContain("\"path\"")
        }
    }

    @Test
    fun pathOutsideRegisteredServicesReachesSpringMvc() {
        val req = Request.Builder()
            .url("${root()}/other.v1.OtherService/Method")
            .post("ping".toByteArray().toRequestBody("application/proto".toMediaType()))
            .build()
        http.newCall(req).execute().use { response ->
            assertThat(response.code).isEqualTo(404)
            // Spring Boot's error controller renders its default error attributes.
            assertThat(response.body!!.string())
                .contains("\"status\":404")
                .contains("\"path\":\"/other.v1.OtherService/Method\"")
        }
    }

    @Test
    fun pathPrefixMountsServicesUnderBasePath() = runBlocking<Unit> {
        val base = "http://127.0.0.1:${prefixedServer.port}"
        val prefixed = connectClient("$base/rpc").unary(TestMessage("ping"), emptyMap(), UNARY)
        assertThat((prefixed as ResponseMessage.Success).message.text()).isEqualTo("echo:ping")

        val unprefixed = Request.Builder()
            .url("$base/test.v1.TestService/Unary")
            .post("ping".toByteArray().toRequestBody("application/proto".toMediaType()))
            .build()
        http.newCall(unprefixed).execute().use { response ->
            assertThat(response.code).isEqualTo(404)
            assertThat(response.body!!.string()).contains("\"path\":\"/test.v1.TestService/Unary\"")
        }

        val health = Request.Builder().url("$base/health").get().build()
        http.newCall(health).execute().use { response ->
            assertThat(response.body!!.string()).isEqualTo("ok")
        }
    }

    @Test
    fun servletContextPathPrecedesServicePaths() = runBlocking<Unit> {
        val base = "http://127.0.0.1:${contextPathServer.port}/api"
        val response = connectClient(base).unary(TestMessage("ping"), emptyMap(), UNARY)
        assertThat((response as ResponseMessage.Success).message.text()).isEqualTo("echo:ping")

        val health = Request.Builder().url("$base/health").get().build()
        http.newCall(health).execute().use { healthResponse ->
            assertThat(healthResponse.body!!.string()).isEqualTo("ok")
        }
    }

    @RestController
    open class CardAuthorizationController {
        @PostMapping("/card-authorization/evaluate")
        open fun evaluate(@RequestBody request: Map<String, Any>): Map<String, Any> = linkedMapOf("approved" to true, "amountMinor" to request.getValue("amountMinor"))

        @GetMapping("/health")
        open fun health(): String = "ok"
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class TestApp {
        @Bean
        open fun cardAuthorizationController() = CardAuthorizationController()

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = UNARY

                    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage = TestMessage("echo:" + request.text())
                },
            )
            .build()
    }
}
