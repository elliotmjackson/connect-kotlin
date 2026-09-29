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

package com.connectrpc.conformance.server

import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ktor.connectRpc
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking

fun main() {
    val request = readServerCompatRequest(System.`in`)
    val http2 = wantsHttp2(request)
    val certPem = serverCertPem(request)
    val registry = buildConformanceRegistry()
    val config = if (request.messageReceiveLimit > 0) {
        ServerConfig(readMaxBytes = request.messageReceiveLimit.toLong())
    } else {
        ServerConfig()
    }
    val server = embeddedServer(
        factory = Netty,
        environment = applicationEnvironment { },
        configure = {
            if (certPem != null) {
                sslConnector(
                    keyStore = buildServerKeyStore(certPem, request.serverCreds.key.toByteArray()),
                    keyAlias = TLS_KEY_ALIAS,
                    keyStorePassword = { TLS_KEY_PASSWORD },
                    privateKeyPassword = { TLS_KEY_PASSWORD },
                ) {
                    host = "127.0.0.1"
                    port = 0
                    if (!request.clientTlsCert.isEmpty) {
                        trustStore = buildClientTrustStore(request.clientTlsCert.toByteArray())
                    }
                }
            } else {
                connector {
                    host = "127.0.0.1"
                    port = 0
                }
            }
            enableHttp2 = http2
            enableH2c = http2 && certPem == null
            maxInitialLineLength = MAX_REQUEST_HEAD_BYTES
            maxHeaderSize = MAX_REQUEST_HEAD_BYTES
        },
        module = {
            connectRpc(registry, config)
        },
    )
    server.start(wait = false)

    val port = runBlocking { server.engine.resolvedConnectors().first().port }
    writeServerCompatResponse(System.out, port, certPem)

    Runtime.getRuntime().addShutdownHook(Thread { server.stop(500, 1000) })

    // Block forever; harness will SIGTERM us.
    Thread.currentThread().join()
}

/**
 * Request line and header limit: Go's `http.DefaultMaxHeaderBytes`
 * (`net/http/server.go:931`), which the reference server runs with. Netty's
 * 4096-byte request-line default rejects ordinary Connect GET URLs.
 */
private const val MAX_REQUEST_HEAD_BYTES = 1 shl 20
