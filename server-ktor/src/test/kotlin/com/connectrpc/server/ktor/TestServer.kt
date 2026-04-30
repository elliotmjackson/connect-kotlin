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

import com.connectrpc.server.HandlerRegistry
import io.ktor.server.application.Application
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.util.concurrent.TimeUnit

/**
 * Spins up an embedded Ktor server with the given handler registry on an
 * ephemeral port. Use [TestServer.use] for setUp/tearDown.
 */
internal class TestServer private constructor(
    private val server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>,
    val port: Int,
) : AutoCloseable {
    val baseUrl: String get() = "http://127.0.0.1:$port"

    override fun close() {
        server.stop(0, 500, TimeUnit.MILLISECONDS)
    }

    companion object {
        fun start(
            registry: HandlerRegistry,
            withH2c: Boolean = false,
            maxReceiveMessageSize: Int = 0,
        ): TestServer {
            val server = embeddedServer(
                factory = Netty,
                environment = applicationEnvironment { },
                configure = {
                    enableH2c = withH2c
                    enableHttp2 = false
                },
                module = {
                    connectRpc(registry, maxReceiveMessageSize = maxReceiveMessageSize)
                },
            )
            server.start(wait = false)
            val port = runBlocking {
                server.engine.resolvedConnectors().first().port
            }
            return TestServer(server, port)
        }
    }
}

/** OkHttp client tuned for tests: short timeouts, optional H2-prior-knowledge. */
internal fun newTestClient(
    h2cPriorKnowledge: Boolean = false,
    callTimeoutMs: Long = 10_000,
): OkHttpClient {
    val builder = OkHttpClient.Builder()
        .callTimeout(callTimeoutMs, TimeUnit.MILLISECONDS)
        .connectTimeout(2, TimeUnit.SECONDS)
        .readTimeout(callTimeoutMs, TimeUnit.MILLISECONDS)
        .writeTimeout(2, TimeUnit.SECONDS)
    if (h2cPriorKnowledge) {
        builder.protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
    }
    return builder.build()
}
