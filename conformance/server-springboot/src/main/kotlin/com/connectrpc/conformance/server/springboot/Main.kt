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
import com.connectrpc.conformance.server.TLS_KEY_ALIAS
import com.connectrpc.conformance.server.TLS_KEY_PASSWORD
import com.connectrpc.conformance.server.buildClientTrustStore
import com.connectrpc.conformance.server.buildConformanceTypeRegistry
import com.connectrpc.conformance.server.buildServerKeyStore
import com.connectrpc.conformance.v1.HTTPVersion
import com.connectrpc.conformance.v1.ServerCompatRequest
import com.connectrpc.conformance.v1.ServerCompatResponse
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.server.HandlerRegistry
import com.google.protobuf.ByteString
import org.apache.coyote.http2.Http2Protocol
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.web.context.WebServerApplicationContext
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import java.io.EOFException
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.security.KeyStore
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

    /**
     * Tomcat's HTTP/2 anti-DoS rejects the conformance suite's tight loops
     * with ENHANCE_YOUR_CALM. The conformance traffic is legitimate; relax
     * the overhead thresholds so the suite can run.
     *
     * Also wires h2c when `connectrpc.h2c=true` is set — adds an
     * [Http2Protocol] upgrade protocol on the connector so Tomcat accepts
     * both prior-knowledge h2c (gRPC's default cleartext mode) and the
     * HTTP/1.1 Upgrade dance.
     */
    @Bean
    open fun http2Customizer(
        @org.springframework.beans.factory.annotation.Value("\${connectrpc.h2c:false}")
        h2c: Boolean,
    ): WebServerFactoryCustomizer<TomcatServletWebServerFactory> =
        WebServerFactoryCustomizer { factory ->
            factory.addConnectorCustomizers({ connector ->
                if (h2c) {
                    val protocol = Http2Protocol()
                    relaxOverhead(protocol)
                    connector.addUpgradeProtocol(protocol)
                }
                for (upgrade in connector.findUpgradeProtocols()) {
                    if (upgrade is Http2Protocol) relaxOverhead(upgrade)
                }
            })
        }

    private fun relaxOverhead(p: Http2Protocol) {
        p.overheadCountFactor = 0
        p.overheadDataThreshold = 0
        p.overheadWindowUpdateThreshold = 0
        p.overheadContinuationThreshold = 0
    }
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

    val props = mutableMapOf<String, Any>(
        "server.port" to 0,
        "server.address" to "127.0.0.1",
        "spring.main.banner-mode" to "off",
        "logging.level.root" to "OFF",
        "spring.main.web-application-type" to "servlet",
        "connectrpc.maxReceiveMessageSize" to request.messageReceiveLimit,
        // Tomcat's default 2MB form-post and swallow caps would intercept
        // oversized messages with HTTP 429; we want the adapter's
        // RESOURCE_EXHAUSTED error path to win.
        "server.tomcat.max-http-form-post-size" to -1,
        "server.tomcat.max-swallow-size" to -1,
    )

    if (wantHttp2 && !request.useTls) {
        // h2c — handled by ConformanceServerApp.h2cConnectorCustomizer below.
        props["connectrpc.h2c"] = true
    }

    var tlsCertPem: ByteArray? = null
    if (request.useTls) {
        val creds = request.serverCreds
        if (creds.cert.isEmpty || creds.key.isEmpty) {
            System.err.println("use_tls=true but server_creds is missing cert/key")
            exitProcess(1)
        }
        tlsCertPem = creds.cert.toByteArray()

        val keyStore = buildServerKeyStore(creds.cert.toByteArray(), creds.key.toByteArray())
        val keyStoreFile = writeKeyStoreToTempFile(keyStore, "server-")

        props["server.ssl.enabled"] = true
        props["server.ssl.key-store"] = "file:$keyStoreFile"
        props["server.ssl.key-store-type"] = "PKCS12"
        props["server.ssl.key-store-password"] = String(TLS_KEY_PASSWORD)
        props["server.ssl.key-alias"] = TLS_KEY_ALIAS
        props["server.ssl.key-password"] = String(TLS_KEY_PASSWORD)
        if (wantHttp2) {
            props["server.http2.enabled"] = true
        }

        if (!request.clientTlsCert.isEmpty) {
            val trustStore = buildClientTrustStore(request.clientTlsCert.toByteArray())
            val trustStoreFile = writeKeyStoreToTempFile(trustStore, "client-trust-")
            props["server.ssl.trust-store"] = "file:$trustStoreFile"
            props["server.ssl.trust-store-type"] = "PKCS12"
            props["server.ssl.trust-store-password"] = String(TLS_KEY_PASSWORD)
            props["server.ssl.client-auth"] = "need"
        }
    }

    val app = SpringApplication(ConformanceServerApp::class.java)
    app.setDefaultProperties(props.mapValues { it.value as Any })
    val ctx = app.run(*args)
    val webCtx = ctx as WebServerApplicationContext
    val port = webCtx.webServer.port

    val responseBuilder = ServerCompatResponse.newBuilder()
        .setHost("127.0.0.1")
        .setPort(port)
    if (tlsCertPem != null) {
        responseBuilder.pemCert = ByteString.copyFrom(tlsCertPem)
    }
    writeServerCompatResponse(System.out, responseBuilder.build())

    Runtime.getRuntime().addShutdownHook(Thread { ctx.close() })
    Thread.currentThread().join()
}

private fun writeKeyStoreToTempFile(keyStore: KeyStore, prefix: String): String {
    val tmp = Files.createTempFile(prefix, ".p12").toFile()
    tmp.deleteOnExit()
    FileOutputStream(tmp).use { keyStore.store(it, TLS_KEY_PASSWORD) }
    return tmp.absolutePath
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
