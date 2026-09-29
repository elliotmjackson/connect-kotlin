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
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.ThreadContextElement
import org.slf4j.MDC
import org.springframework.beans.factory.BeanFactory
import org.springframework.context.i18n.LocaleContext
import org.springframework.context.i18n.LocaleContextHolder
import org.springframework.context.i18n.SimpleLocaleContext
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.util.ClassUtils
import org.springframework.web.context.request.RequestAttributes
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import org.springframework.web.servlet.DispatcherServlet
import org.springframework.web.servlet.LocaleContextResolver
import org.springframework.web.servlet.LocaleResolver
import java.util.function.Supplier
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

private val LOADER = RequestThreadContext::class.java.classLoader
private val SECURITY_PRESENT = ClassUtils.isPresent("org.springframework.security.core.context.SecurityContextHolder", LOADER)
private val MDC_PRESENT = ClassUtils.isPresent("org.slf4j.MDC", LOADER)
private val WEBMVC_PRESENT = ClassUtils.isPresent("org.springframework.web.servlet.LocaleResolver", LOADER)

/**
 * The locale Spring MVC's `DispatcherServlet` exposes for a request, from the
 * application's `localeResolver` bean (spring-webmvc 7.0
 * `DispatcherServlet.buildLocaleContext`); `null` when spring-webmvc is absent or
 * no such bean is defined, where `DispatcherServlet` falls back to
 * `AcceptHeaderLocaleResolver`, i.e. `request.getLocale()`.
 */
internal fun mvcLocaleContextResolver(beanFactory: BeanFactory): ((HttpServletRequest) -> LocaleContext)? = if (WEBMVC_PRESENT) Mvc.localeContextResolver(beanFactory) else null

/**
 * The servlet request's thread-bound Spring state, installed on whichever
 * thread runs the call's coroutines and removed when they suspend, the
 * [ThreadContextElement] contract (kotlinx-coroutines `MDCContext` does the
 * same for MDC alone).
 *
 * Handlers therefore see what a Spring MVC controller on the request thread sees:
 * - [RequestContextHolder] / [LocaleContextHolder]: the filter chain's
 *   `RequestContextFilter` marks its attributes inactive when the chain
 *   returns, which for an async request is before the call ends
 *   (spring-web 7.0 `RequestContextFilter.doFilterInternal`), so the call gets
 *   its own [ServletRequestAttributes], completed by [requestCompleted] like
 *   `FrameworkServlet` does for its requests. The locale is resolved like
 *   `DispatcherServlet` does (see [mvcLocaleContextResolver]), otherwise it is the
 *   one `RequestContextFilter` installed, `request.getLocale()`.
 * - `SecurityContextHolder`: the (possibly deferred) context the Spring
 *   Security filter chain established, captured before the chain clears it.
 * - SLF4J `MDC`: a copy of the request thread's map. As with `MDCContext`,
 *   changes a handler makes are not carried across suspensions.
 *
 * Micrometer context-propagation would cover the same holders through
 * registered `ThreadLocalAccessor`s, but it is not on the Spring Boot 4.1
 * `spring-boot-starter-webmvc` classpath.
 */
internal class RequestThreadContext private constructor(
    private val attributes: ServletRequestAttributes,
    private val locale: LocaleContext,
    private val security: Any?,
    private val mdc: Map<String, String>?,
) : AbstractCoroutineContextElement(Key),
    ThreadContextElement<RequestThreadContext.Saved> {

    companion object Key : CoroutineContext.Key<RequestThreadContext> {
        /**
         * Captures the state of the current (request) thread; [localeContext]
         * overrides the locale the filter chain installed.
         */
        fun capture(
            request: HttpServletRequest,
            response: HttpServletResponse,
            localeContext: ((HttpServletRequest) -> LocaleContext)?,
        ) = RequestThreadContext(
            attributes = ServletRequestAttributes(request, response),
            locale = localeContext?.invoke(request) ?: LocaleContextHolder.getLocaleContext() ?: SimpleLocaleContext(request.locale),
            security = if (SECURITY_PRESENT) Security.capture() else null,
            mdc = if (MDC_PRESENT) Mdc.capture() ?: emptyMap() else null,
        )
    }

    class Saved(
        val attributes: RequestAttributes?,
        val locale: LocaleContext?,
        val security: Any?,
        val mdc: Map<String, String>?,
    )

    override fun updateThreadContext(context: CoroutineContext): Saved {
        val saved = Saved(
            attributes = RequestContextHolder.getRequestAttributes(),
            locale = LocaleContextHolder.getLocaleContext(),
            security = if (security != null) Security.capture() else null,
            mdc = if (mdc != null) Mdc.capture() else null,
        )
        RequestContextHolder.setRequestAttributes(attributes)
        LocaleContextHolder.setLocaleContext(locale)
        if (security != null) Security.install(security)
        if (mdc != null) Mdc.install(mdc)
        return saved
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: Saved) {
        RequestContextHolder.setRequestAttributes(oldState.attributes)
        LocaleContextHolder.setLocaleContext(oldState.locale)
        if (security != null) Security.install(oldState.security)
        if (mdc != null) Mdc.install(oldState.mdc)
    }

    /** Runs request destruction callbacks; call before the async request completes. */
    fun requestCompleted() = attributes.requestCompleted()

    /** Only loaded when Spring Security is on the classpath. */
    private object Security {
        fun capture(): Supplier<SecurityContext> = SecurityContextHolder.getContextHolderStrategy().deferredContext

        @Suppress("UNCHECKED_CAST")
        fun install(context: Any?) {
            if (context == null) {
                SecurityContextHolder.clearContext()
            } else {
                SecurityContextHolder.getContextHolderStrategy().setDeferredContext(context as Supplier<SecurityContext>)
            }
        }
    }

    /** Only loaded when SLF4J is on the classpath. */
    private object Mdc {
        fun capture(): Map<String, String>? = MDC.getCopyOfContextMap()

        fun install(map: Map<String, String>?) {
            if (map == null) MDC.clear() else MDC.setContextMap(map)
        }
    }
}

/** Only loaded when spring-webmvc is on the classpath. */
private object Mvc {
    fun localeContextResolver(beanFactory: BeanFactory): ((HttpServletRequest) -> LocaleContext)? {
        if (!beanFactory.containsBean(DispatcherServlet.LOCALE_RESOLVER_BEAN_NAME)) return null
        val resolver = beanFactory.getBean(DispatcherServlet.LOCALE_RESOLVER_BEAN_NAME, LocaleResolver::class.java)
        return if (resolver is LocaleContextResolver) {
            resolver::resolveLocaleContext
        } else {
            { request -> LocaleContext { resolver.resolveLocale(request) } }
        }
    }
}
