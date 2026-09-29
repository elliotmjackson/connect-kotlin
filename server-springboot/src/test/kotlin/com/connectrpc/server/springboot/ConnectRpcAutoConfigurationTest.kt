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
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/** When [ConnectRpcAutoConfiguration] mounts Connect services and when it leaves every path to Spring MVC. */
class ConnectRpcAutoConfigurationTest {
    @Test
    fun servicesAreMountedByDefault() {
        run(OneServiceApp::class.java) { port ->
            assertThat(post(port, "/test.v1.TestService/Ping")).isEqualTo(200 to "hi")
            assertThat(get(port, "/health")).isEqualTo(200 to "ok")
        }
    }

    @Test
    fun disabledPropertyLeavesServicePathsToSpringMvc() {
        run(OneServiceApp::class.java, "connectrpc.enabled=false") { port ->
            val (status, body) = post(port, "/test.v1.TestService/Ping")
            assertThat(status).isEqualTo(404)
            // Spring Boot's error controller, not the Connect servlet, answered.
            assertThat(body).contains("\"path\":\"/test.v1.TestService/Ping\"")
            assertThat(get(port, "/health")).isEqualTo(200 to "ok")
        }
    }

    /** With no service there is nothing to map; the servlet must not be mapped to every path in place of Spring MVC. */
    @Test
    fun emptyRegistryLeavesEveryPathToSpringMvc() {
        run(NoServiceApp::class.java) { port ->
            assertThat(get(port, "/health")).isEqualTo(200 to "ok")
            assertThat(post(port, "/test.v1.TestService/Ping").second).contains("\"path\":\"/test.v1.TestService/Ping\"")
        }
    }

    /** `connectrpc.path-prefix` values are normalized to one leading and no trailing slash. */
    @Test
    fun pathPrefixSpellingsMountAtTheSamePath() {
        for (prefix in listOf("rpc", "/rpc", "rpc/", "/rpc/", " /rpc/ ")) {
            run(OneServiceApp::class.java, "connectrpc.path-prefix=$prefix") { port ->
                assertThat(post(port, "/rpc/test.v1.TestService/Ping")).describedAs("'$prefix'").isEqualTo(200 to "hi")
            }
        }
        for (prefix in listOf("", "/", " ")) {
            run(OneServiceApp::class.java, "connectrpc.path-prefix=$prefix") { port ->
                assertThat(post(port, "/test.v1.TestService/Ping")).describedAs("'$prefix'").isEqualTo(200 to "hi")
            }
        }
    }

    private fun run(source: Class<*>, vararg properties: String, block: (Int) -> Unit) = SpringApplicationBuilder(source)
        .web(WebApplicationType.SERVLET)
        .properties("server.port=0", "server.address=127.0.0.1", *properties)
        .run()
        .use { context -> block(checkNotNull((context as WebServerApplicationContext).webServer).port) }

    private fun post(port: Int, path: String): Pair<Int, String> = rawExchange(port, "POST $path HTTP/1.1", listOf("Content-Type: application/proto"), "hi".toByteArray())
        .let { it.status to String(it.body) }

    private fun get(port: Int, path: String): Pair<Int, String> = rawExchange(port, "GET $path HTTP/1.1").let { it.status to String(it.body) }

    @RestController
    open class HealthController {
        @GetMapping("/health")
        open fun health(): String = "ok"
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class OneServiceApp {
        @Bean
        open fun healthController() = HealthController()

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Ping", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = request
                },
            )
            .build()
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    open class NoServiceApp {
        @Bean
        open fun healthController() = HealthController()

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder().codec(TestSerializationStrategy).build()
    }
}
