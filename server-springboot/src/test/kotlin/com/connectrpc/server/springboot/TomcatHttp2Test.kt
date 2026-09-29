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
import org.apache.coyote.http2.Http2Protocol
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.tomcat.TomcatWebServer
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.annotation.Bean

/** The `connectrpc.tomcat.http2-*` properties change Tomcat's HTTP/2 connector only when set, each on its own. */
class TomcatHttp2Test {
    private val tomcatDefaults = Http2Protocol()

    @Test
    fun thresholdIsAppliedWhenSet() {
        val protocol = http2Limits("connectrpc.tomcat.http2-overhead-data-threshold=0")
        assertThat(protocol.dataThreshold).isEqualTo(0)
        assertThat(protocol.resetFactor).isEqualTo(tomcatDefaults.overheadResetFactor)
    }

    @Test
    fun resetFactorIsAppliedWhenSet() {
        val protocol = http2Limits("connectrpc.tomcat.http2-overhead-reset-factor=20")
        assertThat(protocol.resetFactor).isEqualTo(20)
        assertThat(protocol.dataThreshold).isEqualTo(tomcatDefaults.overheadDataThreshold)
    }

    @Test
    fun tomcatDefaultsAreKeptWhenUnset() {
        val protocol = http2Limits()
        assertThat(protocol.dataThreshold).isEqualTo(tomcatDefaults.overheadDataThreshold)
        assertThat(protocol.resetFactor).isEqualTo(tomcatDefaults.overheadResetFactor)
    }

    /** Applications that declare their own factory, e.g. to add connectors, get them too. */
    @Test
    fun limitsAreAppliedToApplicationFactory() {
        val protocol = http2Limits(
            "connectrpc.tomcat.http2-overhead-data-threshold=0",
            "connectrpc.tomcat.http2-overhead-reset-factor=20",
            source = OwnFactoryApp::class.java,
        )
        assertThat(protocol.dataThreshold).isEqualTo(0)
        assertThat(protocol.resetFactor).isEqualTo(20)
    }

    private class Limits(val dataThreshold: Int, val resetFactor: Int)

    private fun http2Limits(vararg properties: String, source: Class<*> = TestApp::class.java): Limits = SpringApplicationBuilder(source)
        .web(WebApplicationType.SERVLET)
        .properties("server.port=0", "server.address=127.0.0.1", "server.http2.enabled=true", *properties)
        .run()
        .use { context ->
            val webServer = (context as WebServerApplicationContext).webServer as TomcatWebServer
            val protocol = webServer.tomcat.connector.findUpgradeProtocols().filterIsInstance<Http2Protocol>().single()
            Limits(protocol.overheadDataThreshold, protocol.overheadResetFactor)
        }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
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
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class OwnFactoryApp : TestApp() {
        @Bean
        open fun tomcatServletWebServerFactory() = TomcatServletWebServerFactory()
    }
}
