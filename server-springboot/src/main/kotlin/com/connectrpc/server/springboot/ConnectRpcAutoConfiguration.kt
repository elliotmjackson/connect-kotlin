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

import com.connectrpc.server.HandlerRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.annotation.Bean

/**
 * Registers a [ConnectServlet] mounted at the root path whenever a [HandlerRegistry] bean
 * is on the application context. Drop a [HandlerRegistry] bean into your config
 * and the rest is automatic:
 *
 * ```
 * @Bean
 * fun connectRpcRegistry(): HandlerRegistry =
 *     HandlerRegistry.builder()
 *         .codec(GoogleJavaProtobufStrategy())
 *         .registerAll(MyServiceImpl().handlers())
 *         .build()
 * ```
 *
 * Override the [ConnectRpcOptions] bean to customise message limits,
 * `Connect-Protocol-Version` enforcement, and outbound compression
 * thresholds.
 */
@AutoConfiguration
@ConditionalOnBean(HandlerRegistry::class)
class ConnectRpcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun connectRpcOptions(
        @Value("\${connectrpc.maxReceiveMessageSize:0}") maxReceiveMessageSize: Int,
        @Value("\${connectrpc.requireConnectProtocolHeader:false}") requireConnectProtocolHeader: Boolean,
        @Value("\${connectrpc.compressMinBytes:1024}") compressMinBytes: Int,
    ): ConnectRpcOptions = ConnectRpcOptions(
        maxReceiveMessageSize = maxReceiveMessageSize,
        requireConnectProtocolHeader = requireConnectProtocolHeader,
        compressMinBytes = compressMinBytes,
    )

    @Bean
    fun connectServletRegistration(
        registry: HandlerRegistry,
        options: ConnectRpcOptions,
    ): ServletRegistrationBean<ConnectServlet> {
        val bean = ServletRegistrationBean(ConnectServlet(registry, options), "/*")
        bean.setName("connectRpcServlet")
        bean.setAsyncSupported(true)
        bean.setLoadOnStartup(1)
        return bean
    }
}
