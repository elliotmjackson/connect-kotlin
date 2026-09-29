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

import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ServerObserver
import jakarta.servlet.http.HttpServlet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import org.apache.catalina.Engine
import org.apache.coyote.http2.Http2Protocol
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.thread.Threading
import org.springframework.boot.tomcat.TomcatConnectorCustomizer
import org.springframework.boot.tomcat.TomcatContextCustomizer
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.task.VirtualThreadTaskExecutor
import kotlin.time.toKotlinDuration

/**
 * Registers a [ConnectServlet] for the application's [HandlerRegistry] bean.
 *
 * The servlet is mapped under `<connectrpc.path-prefix>/<package>.<Service>/` once per
 * registered service, matching the Connect request path
 * `"/" [Routing-Prefix "/"] Procedure-Name` (https://connectrpc.com/docs/protocol#unary-request).
 * Path-prefix mappings take precedence over the default `/` mapping of Spring MVC's
 * `DispatcherServlet` (Jakarta Servlet 6.1 §12.1), so every other path keeps reaching
 * Spring MVC, as with Spring Boot's gRPC servlet support (`GrpcServletRegistration`).
 *
 * Declare a [ServerConfig], [ConnectServer] or [ConnectServlet] bean to replace the one
 * built from [ConnectRpcProperties], or a `ServletRegistrationBean` named
 * `connectRpcServletRegistration` to replace the mapping. A [ServerObserver] bean is
 * set as the [ServerConfig.observer] of the configuration built from the properties.
 * A [ConnectServerLifecycle] shuts the server down before the web server's graceful
 * shutdown, giving calls in flight `connectrpc.shutdown-grace-period`.
 *
 * Calls run on virtual threads when Spring Boot's `Threading.VIRTUAL` is active
 * (`spring.threads.virtual.enabled=true` on Java 21+), otherwise on a view of
 * [Dispatchers.IO] limited to `connectrpc.handler-threads` (see [ConnectServlet]).
 *
 * The `connectrpc.tomcat.http2-*` limits are opt-in because Spring Boot's gRPC servlet
 * support leaves Tomcat's limits as they are (reference guide `io/grpc.adoc`,
 * "Switching to a Servlet Container"). They customize only the main connector, also
 * when the application declares its own `TomcatServletWebServerFactory`
 * (spring-boot-tomcat 4.1.0 `TomcatWebServerFactory.java:400-410`). Tomcat's reuse of
 * request-processing objects is turned off on every connector (see
 * [ConnectTomcat.applyRequestObjectIsolation]).
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(HttpServlet::class)
@ConditionalOnBean(HandlerRegistry::class)
@ConditionalOnBooleanProperty(name = ["connectrpc.enabled"], matchIfMissing = true)
@EnableConfigurationProperties(ConnectRpcProperties::class)
class ConnectRpcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun connectRpcServerConfig(properties: ConnectRpcProperties, observer: ObjectProvider<ServerObserver>): ServerConfig = properties.toServerConfig(observer.ifAvailable)

    @Bean
    @ConditionalOnMissingBean
    fun connectRpcServer(registry: HandlerRegistry, config: ServerConfig): ConnectServer = ConnectServer(registry, config)

    @Bean
    @ConditionalOnMissingBean
    fun connectRpcServerLifecycle(server: ConnectServer, properties: ConnectRpcProperties): ConnectServerLifecycle = ConnectServerLifecycle(server, properties.shutdownGracePeriod.toKotlinDuration())

    @Bean
    @ConditionalOnMissingBean
    fun connectRpcServlet(
        server: ConnectServer,
        properties: ConnectRpcProperties,
        environment: Environment,
        beanFactory: BeanFactory,
    ): ConnectServlet {
        val dispatcher = if (Threading.VIRTUAL.isActive(environment)) {
            VirtualThreadTaskExecutor("connectrpc-").asCoroutineDispatcher()
        } else {
            Dispatchers.IO.limitedParallelism(properties.handlerThreads)
        }
        return ConnectServlet(server, normalizePathPrefix(properties.pathPrefix), dispatcher, mvcLocaleContextResolver(beanFactory))
    }

    @Bean
    @ConditionalOnMissingBean(name = [SERVLET_REGISTRATION_BEAN_NAME])
    fun connectRpcServletRegistration(servlet: ConnectServlet): ServletRegistrationBean<ConnectServlet> {
        val mappings = servlet.server.procedures
            .map { it.substringBeforeLast('/') }
            .toSortedSet()
            .map { "${servlet.pathPrefix}/$it/*" }
        val bean = ServletRegistrationBean(servlet, *mappings.toTypedArray())
        bean.setName("connectRpcServlet")
        bean.setAsyncSupported(true)
        bean.setLoadOnStartup(1)
        // ServletRegistrationBean maps "/*" when given no mappings, which would shadow Spring MVC.
        bean.isEnabled = mappings.isNotEmpty()
        return bean
    }

    /** Only loaded when Tomcat is on the classpath; applies the HTTP/2 limits that are set. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.apache.coyote.http2.Http2Protocol", "org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory"])
    class TomcatConfiguration {
        /**
         * Connector customizers run after Spring Boot adds the `Http2Protocol` for
         * `server.http2.enabled` (spring-boot-tomcat 4.1.0
         * `TomcatWebServerFactory.java:428-438`).
         */
        @Bean
        fun connectRpcTomcatHttp2Customizer(properties: ConnectRpcProperties): WebServerFactoryCustomizer<TomcatServletWebServerFactory> {
            val dataThreshold = properties.tomcat.http2OverheadDataThreshold
            val resetFactor = properties.tomcat.http2OverheadResetFactor
            return WebServerFactoryCustomizer { factory ->
                if (dataThreshold == null && resetFactor == null) return@WebServerFactoryCustomizer
                factory.addConnectorCustomizers(
                    TomcatConnectorCustomizer { connector ->
                        connector.findUpgradeProtocols().filterIsInstance<Http2Protocol>().forEach { protocol ->
                            dataThreshold?.let { protocol.overheadDataThreshold = it }
                            resetFactor?.let { protocol.overheadResetFactor = it }
                        }
                    },
                )
            }
        }
    }

    /**
     * Applies [ConnectTomcat.applyRequestObjectIsolation] to every connector of the Tomcat
     * service. Connector customizers reach only the main connector and connectors the
     * factory passes to `customizeConnector`; `addAdditionalConnectors` connectors skip
     * them (spring-boot-tomcat 4.1.0 `TomcatWebServerFactory.java:332-342,408-411`). A
     * context customizer runs after every connector is in the service and after all
     * connector customizers (`TomcatServletWebServerFactory.java:163-166,226-227,340-341`),
     * so it reaches each connector and overrides `server.tomcat.processor-cache`.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.apache.coyote.http2.Http2Protocol", "org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory"])
    class TomcatObjectReuseConfiguration {
        @Bean
        fun connectRpcTomcatObjectReuseCustomizer(): WebServerFactoryCustomizer<TomcatServletWebServerFactory> = WebServerFactoryCustomizer { factory ->
            factory.addContextCustomizers(
                TomcatContextCustomizer { context ->
                    (context.parent.parent as Engine).service.findConnectors().forEach(ConnectTomcat::applyRequestObjectIsolation)
                },
            )
        }
    }

    private companion object {
        const val SERVLET_REGISTRATION_BEAN_NAME = "connectRpcServletRegistration"
    }
}

/** `""` for the root, otherwise the prefix with one leading and no trailing slash. */
internal fun normalizePathPrefix(prefix: String): String {
    val trimmed = prefix.trim().trim('/')
    return if (trimmed.isEmpty()) "" else "/$trimmed"
}
