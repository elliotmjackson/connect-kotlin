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

package com.connectrpc.conformance.server.springboot

import com.connectrpc.conformance.server.ConformanceServiceImpl
import com.connectrpc.conformance.server.buildConformanceTypeRegistry
import com.connectrpc.conformance.v1.HTTPVersion
import com.connectrpc.conformance.v1.ServerCompatRequest
import com.connectrpc.conformance.v1.ServerCompatResponse
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.server.HandlerRegistry
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.web.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import kotlin.system.exitProcess

@SpringBootApplication
open class ConformanceServerApp {
    @Bean
    open fun connectRpcRegistry(): HandlerRegistry =
        HandlerRegistry.builder()
            .codec(GoogleJavaProtobufStrategy())
            .codec(GoogleJavaJSONStrategy(buildConformanceTypeRegistry()))
            .registerAll(ConformanceServiceImpl().handlers())
            .build()
}

fun main(args: Array<String>) {
    val request = readServerCompatRequest(System.`in`)
    val wantHttp2 = when (request.httpVersion) {
        HTTPVersion.HTTP_VERSION_1 -> false
        HTTPVersion.HTTP_VERSION_2 -> true
        else -> {
            System.err.println("only supports HTTP/1.1 and HTTP/2, got ${request.httpVersion}")
            exitProcess(1)
        }
    }
    if (request.useTls) {
        System.err.println("TLS not yet supported in :conformance:server-springboot")
        exitProcess(1)
    }
    if (wantHttp2) {
        // HTTP/2 cleartext on Tomcat needs an UpgradeProtocol on the connector;
        // this binary currently runs HTTP/1.1 only. Conformance will skip h2c
        // configurations until that lands.
        System.err.println("HTTP/2 cleartext not yet supported in :conformance:server-springboot")
        exitProcess(1)
    }

    val props = mutableMapOf<String, Any>(
        "server.port" to 0,
        "server.address" to "127.0.0.1",
        "spring.main.banner-mode" to "off",
        "logging.level.root" to "OFF",
        "spring.main.web-application-type" to "servlet",
        "connectrpc.maxReceiveMessageSize" to request.messageReceiveLimit.toLong(),
    )

    val app = SpringApplication(ConformanceServerApp::class.java)
    app.setDefaultProperties(props.mapValues { it.value as Any })
    val ctx = app.run(*args)
    val webCtx = ctx as WebServerApplicationContext
    val port = webCtx.webServer.port

    val response = ServerCompatResponse.newBuilder()
        .setHost("127.0.0.1")
        .setPort(port)
        .build()
    writeServerCompatResponse(System.out, response)

    Runtime.getRuntime().addShutdownHook(Thread { ctx.close() })
    Thread.currentThread().join()
}

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
