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

import jakarta.servlet.http.HttpServletRequest
import org.springframework.util.ClassUtils
import org.springframework.web.filter.ServerHttpObservationFilter

private val OBSERVATION_PRESENT = ClassUtils.isPresent(
    "org.springframework.web.filter.ServerHttpObservationFilter",
    ServerObservation::class.java.classLoader,
) &&
    ClassUtils.isPresent("io.micrometer.observation.Observation", ServerObservation::class.java.classLoader)

/**
 * Sets the path pattern of the request's `http.server.requests` observation to
 * `<pathPrefix>/<Procedure-Name>` when [path] names one of [procedures].
 *
 * Spring Boot's `WebMvcObservationAutoConfiguration` registers
 * `ServerHttpObservationFilter`, whose default convention tags a request without a
 * path pattern `UNKNOWN` (spring-web 7.0.8
 * `DefaultServerRequestObservationConvention.java:132-153`); Spring MVC sets the
 * pattern from its handler mapping, which never sees Connect requests. Web
 * frameworks contribute it through the context the filter exposes
 * (`ServerHttpObservationFilter.java:46-50,166-169`). Unregistered paths keep
 * the default, so arbitrary URLs never become tag values.
 *
 * @param path the percent-encoded path after [pathPrefix], `/<Procedure-Name>`
 *     for a Connect call.
 */
internal fun recordProcedurePattern(request: HttpServletRequest, pathPrefix: String, path: String, procedures: Set<String>) {
    if (OBSERVATION_PRESENT) ServerObservation.record(request, pathPrefix, path, procedures)
}

/** Only loaded when spring-web's observation support and Micrometer are on the classpath. */
private object ServerObservation {
    fun record(request: HttpServletRequest, pathPrefix: String, path: String, procedures: Set<String>) {
        val context = ServerHttpObservationFilter.findObservationContext(request).orElse(null) ?: return
        if (path.length > 1 && path.substring(1) in procedures) context.pathPattern = pathPrefix + path
    }
}
