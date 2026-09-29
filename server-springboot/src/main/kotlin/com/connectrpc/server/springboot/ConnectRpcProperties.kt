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

import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ServerObserver
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import kotlin.time.toKotlinDuration

/**
 * `connectrpc.*` configuration properties bound by [ConnectRpcAutoConfiguration].
 * The limits map onto [ServerConfig].
 *
 * Descriptions mirror `META-INF/spring-configuration-metadata.json`, which is
 * maintained by hand because `spring-boot-configuration-processor` is a Java
 * annotation processor and does not see Kotlin sources.
 */
@ConfigurationProperties("connectrpc")
class ConnectRpcProperties {
    /** Whether to register the Connect servlet for the application's HandlerRegistry. */
    var enabled: Boolean = true

    /**
     * Largest accepted request message, in bytes, both on the wire and after
     * decompression. Zero means unlimited.
     */
    var readMaxBytes: Long = ServerConfig.DEFAULT_READ_MAX_BYTES

    /** Largest response message a handler may send, in bytes, before compression. Zero means unlimited. */
    var sendMaxBytes: Long = 0

    /**
     * Whether to reject Connect unary requests without `Connect-Protocol-Version: 1`
     * (GET: without the `connect=v1` query parameter).
     */
    var requireConnectProtocolHeader: Boolean = false

    /** Smallest response message, in bytes, eligible for outbound compression. */
    var compressMinBytes: Long = 1024

    /**
     * Longest a call may read its request and run its handler: a longer or
     * missing client timeout is replaced by this one. It does not bound writing
     * the response: Tomcat bounds HTTP/1.1 socket writes by
     * `server.tomcat.connection-timeout` (60 s by default) and HTTP/2 socket
     * writes by 5 s, but not an HTTP/2 stream whose client keeps its connection
     * open and grants no flow-control window (see the module README,
     * "Production settings"). Unset sets no limit: a client that sends no
     * deadline and stalls its request body holds its call, and its request
     * buffers, until it disconnects, as the servlet's async timeout is
     * disabled. Set it on servers that untrusted clients can reach, e.g. `30s`
     * for unary services.
     */
    var maxTimeout: Duration? = null

    /**
     * How long calls in flight may run once the application context starts to
     * close; then they end with `unavailable`. Defaults to 30 seconds, the time
     * Spring Boot's graceful shutdown gives active requests
     * (`spring.lifecycle.timeout-per-shutdown-phase`).
     */
    var shutdownGracePeriod: Duration = Duration.ofSeconds(30)

    /**
     * Largest number of calls whose handlers run at once when virtual threads are
     * off; handlers may block. Defaults to Tomcat's default `maxThreads`.
     */
    var handlerThreads: Int = DEFAULT_HANDLER_THREADS

    /**
     * Path under which every registered service is mounted, for example `/rpc`
     * serves `acme.v1.FooService/Bar` at `/rpc/acme.v1.FooService/Bar`.
     * Empty mounts services at the root; many gRPC clients cannot send a path prefix.
     */
    var pathPrefix: String = ""

    /** Settings applied only when Tomcat is the servlet container. */
    val tomcat = Tomcat()

    class Tomcat {
        /**
         * `overheadDataThreshold` for Tomcat's HTTP/2 connector: the average
         * non-final DATA frame size below which Tomcat counts frames as overhead
         * and eventually closes the connection; zero or less disables that check.
         * Unset keeps Tomcat's value, 1024 by default
         * (https://tomcat.apache.org/tomcat-11.0-doc/config/http2.html).
         */
        var http2OverheadDataThreshold: Int? = null

        /**
         * `overheadResetFactor` for Tomcat's HTTP/2 connector: how much each RST_STREAM
         * received or sent adds to the connection's overhead count, which Tomcat closes
         * the connection for once it exceeds zero; less than zero is treated as zero.
         * Unset keeps Tomcat's value, 50 by default
         * (https://tomcat.apache.org/tomcat-11.0-doc/config/http2.html). Tomcat added
         * this count as its fix for the HTTP/2 rapid reset attack, CVE-2023-44487
         * (https://tomcat.apache.org/security-11.html), so a lower value lets a client
         * open and reset more streams before Tomcat closes its connection.
         */
        var http2OverheadResetFactor: Int? = null
    }

    internal fun toServerConfig(observer: ServerObserver?) = ServerConfig(
        readMaxBytes = readMaxBytes,
        sendMaxBytes = sendMaxBytes,
        compressMinBytes = compressMinBytes,
        requireConnectProtocolHeader = requireConnectProtocolHeader,
        maxTimeout = maxTimeout?.toKotlinDuration(),
        observer = observer,
    )
}
