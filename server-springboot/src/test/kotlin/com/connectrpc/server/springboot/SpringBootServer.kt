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

import org.junit.rules.ExternalResource
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.ConfigurableApplicationContext

/**
 * JUnit 4 class rule running a Spring Boot servlet application on a random local port.
 *
 * Spring Framework 7 deprecates `SpringRunner` and the rest of its JUnit 4 support
 * (https://github.com/spring-projects/spring-framework/wiki/Spring-Framework-7.0-Release-Notes),
 * so the application is started through [SpringApplicationBuilder] instead.
 */
class SpringBootServer(
    private val source: Class<*>,
    private vararg val properties: String,
) : ExternalResource() {
    private var running: ConfigurableApplicationContext? = null

    val context: ConfigurableApplicationContext
        get() = checkNotNull(running)

    val port: Int
        get() = checkNotNull((context as WebServerApplicationContext).webServer).port

    override fun before() {
        running = SpringApplicationBuilder(source)
            .web(WebApplicationType.SERVLET)
            .properties("server.port=0", "server.address=127.0.0.1", *properties)
            .run()
    }

    override fun after() {
        running?.close()
        running = null
    }
}
