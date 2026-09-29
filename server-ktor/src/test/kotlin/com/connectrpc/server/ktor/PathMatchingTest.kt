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
import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.TimeUnit

/**
 * Procedure paths are matched byte for byte (protocol.md:146, "case-sensitive"),
 * so a path an authorization layer reads can't differ from the procedure served.
 */
class PathMatchingTest {
    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec("test.v1.TestService/Unary", TestMessage::class, TestMessage::class, StreamType.UNARY)
                override suspend fun handle(request: TestMessage, ctx: HandlerContext) = request
            },
        )
        .build()

    @Test
    fun onlyTheCanonicalPathIsServedAtTheRoot() {
        TestServer.start(registry).use { server ->
            assertThat(status(server.port, "/test.v1.TestService/Unary")).isEqualTo(200)
            for (path in listOf(
                "//test.v1.TestService/Unary",
                "/test.v1.TestService//Unary",
                "/test.v1.TestService/Unary/",
                "/test.v1.TestService/%55nary",
                "/test.v1.TestService%2FUnary",
                "/test.v1.TestService/./Unary",
                "/x/../test.v1.TestService/Unary",
            )) {
                assertThat(status(server.port, path)).describedAs(path).isEqualTo(404)
            }
        }
    }

    @Test
    fun mountedUnderAPrefixRoute() {
        val server = embeddedServer(
            factory = Netty,
            environment = applicationEnvironment { },
            configure = {
                connector {
                    host = "127.0.0.1"
                    port = 0
                }
            },
            module = { routing { route("/api/v1") { connectRpc(ConnectServer(registry)) } } },
        ).start(wait = false)
        try {
            val port = runBlocking { server.engine.resolvedConnectors().first().port }
            assertThat(status(port, "/api/v1/test.v1.TestService/Unary")).isEqualTo(200)
            for (path in listOf(
                "/test.v1.TestService/Unary",
                "/api/v1//test.v1.TestService/Unary",
                "/api//v1/test.v1.TestService/Unary",
                "/a%70i/v1/test.v1.TestService/Unary",
                "/api/v1/test.v1.TestService/%55nary",
            )) {
                assertThat(status(port, path)).describedAs(path).isEqualTo(404)
            }
        } finally {
            server.stop(0, 500, TimeUnit.MILLISECONDS)
        }
    }

    /** Sends a Connect unary POST with [path] on the request line exactly as given. */
    private fun status(port: Int, path: String): Int = rawExchange(port, "POST $path HTTP/1.1", listOf("Content-Type: application/proto"), "hi".toByteArray()).status
}
