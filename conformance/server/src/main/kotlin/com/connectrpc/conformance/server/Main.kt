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

import com.connectrpc.conformance.v1.HTTPVersion
import com.connectrpc.conformance.v1.ServerCompatRequest
import com.connectrpc.conformance.v1.ServerCompatResponse
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.server.ktor.connectRpc
import com.google.protobuf.ByteString
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    runBlockingMain(args, System.`in`, System.out)
}

internal fun runBlockingMain(
    @Suppress("UNUSED_PARAMETER") args: Array<String>,
    input: InputStream,
    output: OutputStream,
) {
    val request = readServerCompatRequest(input)
    val wantHttp2 = when (request.httpVersion) {
        HTTPVersion.HTTP_VERSION_1 -> false
        HTTPVersion.HTTP_VERSION_2 -> true
        else -> {
            System.err.println("server only supports HTTP/1.1 and HTTP/2, got ${request.httpVersion}")
            exitProcess(1)
        }
    }

    val tlsCertPem: ByteArray? = if (request.useTls) {
        val creds = request.serverCreds
        if (creds.cert.isEmpty || creds.key.isEmpty) {
            System.err.println("use_tls=true but server_creds is missing cert/key")
            exitProcess(1)
        }
        creds.cert.toByteArray()
    } else {
        null
    }

    val registry = buildConformanceRegistry()

    val maxReceiveSize = request.messageReceiveLimit
    val server = if (request.useTls) {
        val keyStore = buildServerKeyStore(
            request.serverCreds.cert.toByteArray(),
            request.serverCreds.key.toByteArray(),
        )
        val clientTrustStore = if (request.clientTlsCert.isEmpty) {
            null
        } else {
            buildClientTrustStore(request.clientTlsCert.toByteArray())
        }
        embeddedServer(
            factory = Netty,
            environment = applicationEnvironment { },
            configure = {
                sslConnector(
                    keyStore = keyStore,
                    keyAlias = TLS_KEY_ALIAS,
                    keyStorePassword = { TLS_KEY_PASSWORD },
                    privateKeyPassword = { TLS_KEY_PASSWORD },
                ) {
                    host = "127.0.0.1"
                    port = 0
                    if (clientTrustStore != null) {
                        trustStore = clientTrustStore
                    }
                }
                enableHttp2 = wantHttp2
                enableH2c = false
            },
            module = {
                connectRpc(registry, maxReceiveMessageSize = maxReceiveSize)
            },
        )
    } else {
        embeddedServer(
            factory = Netty,
            environment = applicationEnvironment { },
            configure = {
                connector {
                    host = "127.0.0.1"
                    port = 0
                }
                enableHttp2 = wantHttp2
                enableH2c = wantHttp2
            },
            module = {
                connectRpc(registry, maxReceiveMessageSize = maxReceiveSize)
            },
        )
    }
    server.start(wait = false)

    val port = kotlinx.coroutines.runBlocking {
        server.engine.resolvedConnectors().first().port
    }

    val responseBuilder = ServerCompatResponse.newBuilder()
        .setHost("127.0.0.1")
        .setPort(port)
    if (tlsCertPem != null) {
        responseBuilder.pemCert = ByteString.copyFrom(tlsCertPem)
    }
    writeServerCompatResponse(output, responseBuilder.build())

    Runtime.getRuntime().addShutdownHook(
        Thread {
            server.stop(500, 1000)
        },
    )

    // Block forever; harness will SIGTERM us.
    Thread.currentThread().join()
}

private fun buildConformanceRegistry(): HandlerRegistry =
    HandlerRegistry.builder()
        .codec(GoogleJavaProtobufStrategy())
        .codec(GoogleJavaJSONStrategy(buildConformanceTypeRegistry()))
        .registerAll(ConformanceServiceImpl().handlers())
        .build()

/**
 * The conformance handlers pack the original request into [Any] for error
 * details. Protobuf JSON requires a [com.google.protobuf.TypeRegistry] that
 * can resolve those type URLs back to descriptors — feed it everything
 * declared in the conformance v1 protos. Public so the springboot
 * conformance binary can reuse it.
 */
fun buildConformanceTypeRegistry(): com.google.protobuf.TypeRegistry =
    com.google.protobuf.TypeRegistry.newBuilder()
        .add(com.connectrpc.conformance.v1.ServiceProto.getDescriptor().messageTypes)
        .add(com.connectrpc.conformance.v1.ConfigProto.getDescriptor().messageTypes)
        .add(com.connectrpc.conformance.v1.ServerCompatProto.getDescriptor().messageTypes)
        .add(com.connectrpc.conformance.v1.ClientCompatProto.getDescriptor().messageTypes)
        .add(com.connectrpc.conformance.v1.SuiteProto.getDescriptor().messageTypes)
        .build()

private fun readServerCompatRequest(input: InputStream): ServerCompatRequest {
    val len = input.readBigEndianInt()
        ?: throw EOFException("EOF before reading ServerCompatRequest length prefix")
    val bytes = input.readN(len)
        ?: throw EOFException("EOF after $len-byte length prefix")
    return ServerCompatRequest.parseFrom(bytes)
}

private fun writeServerCompatResponse(output: OutputStream, response: ServerCompatResponse) {
    val bytes = response.toByteArray()
    output.write(intToBigEndianBytes(bytes.size))
    output.write(bytes)
    output.flush()
}

private fun InputStream.readN(len: Int): ByteArray? {
    val bytes = ByteArray(len)
    var off = 0
    var remain = len
    while (remain > 0) {
        val n = read(bytes, off, remain)
        if (n <= 0) {
            return if (off == 0) null else throw EOFException("read $off of $len bytes")
        }
        off += n
        remain -= n
    }
    return bytes
}

private fun InputStream.readBigEndianInt(): Int? {
    val bytes = readN(4) ?: return null
    return (bytes[0].toInt() and 0xff shl 24) or
        (bytes[1].toInt() and 0xff shl 16) or
        (bytes[2].toInt() and 0xff shl 8) or
        (bytes[3].toInt() and 0xff)
}

private fun intToBigEndianBytes(value: Int): ByteArray = byteArrayOf(
    (value ushr 24).toByte(),
    (value ushr 16).toByte(),
    (value ushr 8).toByte(),
    value.toByte(),
)
