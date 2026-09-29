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
import com.connectrpc.server.ServerConfig
import io.ktor.server.application.Application
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import java.security.KeyStore
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Spins up an embedded Ktor server with the given handler registry on an
 * ephemeral port. Use [TestServer.use] for setUp/tearDown.
 */
internal class TestServer private constructor(
    private val server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>,
    val port: Int,
    private val tls: Boolean,
) : AutoCloseable {
    val baseUrl: String get() = if (tls) "https://localhost:$port" else "http://127.0.0.1:$port"

    override fun close() {
        server.stop(0, 500, TimeUnit.MILLISECONDS)
    }

    /** Stops as the JVM shutdown hook does, with the engine's configured grace period and timeout. */
    fun stopGracefully() {
        server.stop()
    }

    companion object {
        /** Self-signed certificate for `localhost`, shared by TLS tests. */
        val certificate: HeldCertificate by lazy {
            HeldCertificate.Builder().addSubjectAlternativeName("localhost").build()
        }

        fun start(
            registry: HandlerRegistry,
            config: ServerConfig = ServerConfig(),
            withH2c: Boolean = false,
            withTls: Boolean = false,
            shutdownGracePeriod: Duration = 1.seconds,
            module: Application.() -> Unit = {},
        ): TestServer {
            val server = embeddedServer(
                factory = Netty,
                environment = applicationEnvironment { },
                configure = {
                    if (withTls) {
                        val keyStore = KeyStore.getInstance("PKCS12").apply {
                            load(null, null)
                            setKeyEntry("tls", certificate.keyPair.private, KEY_PASSWORD, arrayOf<java.security.cert.Certificate>(certificate.certificate))
                        }
                        sslConnector(keyStore, "tls", { KEY_PASSWORD }, { KEY_PASSWORD }) {
                            host = "127.0.0.1"
                            port = 0
                        }
                        enableHttp2 = true
                    } else {
                        connector {
                            host = "127.0.0.1"
                            port = 0
                        }
                        enableH2c = withH2c
                        enableHttp2 = withH2c
                    }
                },
                module = {
                    module()
                    connectRpc(registry, config, shutdownGracePeriod)
                },
            )
            server.start(wait = false)
            val port = runBlocking {
                server.engine.resolvedConnectors().first().port
            }
            return TestServer(server, port, withTls)
        }

        private val KEY_PASSWORD = "password".toCharArray()
    }
}

/** OkHttp client for [TestServer]'s TLS mode: trusts [TestServer.certificate], negotiates HTTP/2 via ALPN. */
internal fun newTlsTestClient(): OkHttpClient {
    val certificates = HandshakeCertificates.Builder().addTrustedCertificate(TestServer.certificate.certificate).build()
    return newTestClient().newBuilder()
        .sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager)
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .build()
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
