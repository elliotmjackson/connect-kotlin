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

import com.connectrpc.conformance.server.TLS_KEY_ALIAS
import com.connectrpc.conformance.server.TLS_KEY_PASSWORD
import com.connectrpc.conformance.server.buildClientTrustStore
import com.connectrpc.conformance.server.buildConformanceRegistry
import com.connectrpc.conformance.server.buildServerKeyStore
import com.connectrpc.conformance.server.readServerCompatRequest
import com.connectrpc.conformance.server.serverCertPem
import com.connectrpc.conformance.server.wantsHttp2
import com.connectrpc.conformance.server.writeServerCompatResponse
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.springboot.ConnectRpcAutoConfiguration
import org.springframework.boot.SpringApplication
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean
import java.io.FileOutputStream
import java.nio.file.Files
import java.security.KeyStore

// The auto-configurations of @SpringBootApplication that contribute beans here: the Tomcat
// web server, the Connect servlet, the SSL bundle registry the web server factory receives,
// and the lifecycle processor with Spring Boot's 30 s shutdown-phase timeout. Evaluating
// the rest (task executors, AOP, info, JMX) only costs startup CPU.
@SpringBootConfiguration(proxyBeanMethods = false)
@ImportAutoConfiguration(
    TomcatServletWebServerAutoConfiguration::class,
    ConnectRpcAutoConfiguration::class,
    SslAutoConfiguration::class,
    LifecycleAutoConfiguration::class,
)
open class ConformanceServerApp {
    @Bean
    open fun connectRpcRegistry(): HandlerRegistry = buildConformanceRegistry()
}

fun main(args: Array<String>) {
    val request = readServerCompatRequest(System.`in`)
    val certPem = serverCertPem(request)

    val props = mutableMapOf<String, Any>(
        "server.port" to 0,
        "server.address" to "127.0.0.1",
        "spring.main.banner-mode" to "off",
        "logging.level.root" to "OFF",
        "spring.main.web-application-type" to "servlet",
        // server.max-http-request-header-size stays at its 8 KiB default: every conformance
        // v1.0.5 request fits in 1 KiB, Connect GET included, and Tomcat allocates the limit
        // for each HTTP/1.1 request in progress (see server-springboot/README.md).
    )
    // Zero means the test sets no limit (conformance testing_servers.md), whereas
    // connectrpc.read-max-bytes=0 would disable the 4 MiB default.
    if (request.messageReceiveLimit > 0) props["connectrpc.read-max-bytes"] = request.messageReceiveLimit

    if (wantsHttp2(request)) {
        // Without SSL this selects h2c on Tomcat 11:
        // https://docs.spring.io/spring-boot/4.1/how-to/webserver.html#howto.webserver.configure-http2
        props["server.http2.enabled"] = true
        // The conformance client (connect-go v1.19.1 over Go's HTTP/2 transport) sends
        // each envelope's 5-byte prefix and its payload as separate DATA frames. With
        // Tomcat's default overheadDataThreshold of 1024, two consecutive 5-byte frames
        // raise the connection's overhead count by 1024 / 5 = 204, past its starting
        // value of -10 * overheadCountFactor = -100, and Tomcat closes the connection
        // with GOAWAY ENHANCE_YOUR_CALM (Tomcat 11 Http2UpgradeHandler
        // startRequestBodyFrame; https://tomcat.apache.org/tomcat-11.0-doc/config/http2.html).
        // Zero disables only this small-DATA check; every other overhead limit keeps
        // its default.
        props["connectrpc.tomcat.http2-overhead-data-threshold"] = 0
    }

    if (certPem != null) {
        val keyStore = buildServerKeyStore(certPem, request.serverCreds.key.toByteArray())
        val keyStoreFile = writeKeyStoreToTempFile(keyStore, "server-")

        props["server.ssl.enabled"] = true
        props["server.ssl.key-store"] = "file:$keyStoreFile"
        props["server.ssl.key-store-type"] = "PKCS12"
        props["server.ssl.key-store-password"] = String(TLS_KEY_PASSWORD)
        props["server.ssl.key-alias"] = TLS_KEY_ALIAS
        props["server.ssl.key-password"] = String(TLS_KEY_PASSWORD)

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
    val port = checkNotNull(webCtx.webServer) { "servlet web server did not start" }.port
    Runtime.getRuntime().addShutdownHook(Thread { ctx.close() })
    writeServerCompatResponse(System.out, port, certPem)

    Thread.currentThread().join()
}

private fun writeKeyStoreToTempFile(keyStore: KeyStore, prefix: String): String {
    val tmp = Files.createTempFile(prefix, ".p12").toFile()
    tmp.deleteOnExit()
    FileOutputStream(tmp).use { keyStore.store(it, TLS_KEY_PASSWORD) }
    return tmp.absolutePath
}
