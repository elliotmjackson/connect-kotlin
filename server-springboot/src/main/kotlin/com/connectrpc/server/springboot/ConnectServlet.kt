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
import jakarta.servlet.http.HttpServlet
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.springframework.context.i18n.LocaleContext

/**
 * Default number of calls whose handlers may run (or block) at once: Tomcat's
 * default `maxThreads` (https://tomcat.apache.org/tomcat-11.0-doc/config/executor.html),
 * the blocking capacity a Spring MVC controller gets.
 */
const val DEFAULT_HANDLER_THREADS: Int = 200

/**
 * Serves a [ConnectServer] from a servlet container. [ConnectRpcAutoConfiguration]
 * maps it under `<pathPrefix>/<package>.<Service>/` for every registered service.
 *
 * Each request becomes one asynchronous servlet request (`startAsync`) and one
 * coroutine running [ConnectServer.serve] on [dispatcher]. Request and response
 * bodies use Servlet non-blocking I/O, so a call only occupies a thread while
 * it computes: slow or stalled clients hold no threads. The container's async
 * timeout is disabled (`AsyncContext.setTimeout(0)`, "zero or less indicates
 * no timeout"); RPC deadlines are enforced by [ConnectServer], over request
 * reads and the handler. The deadline does not bound response writes. Tomcat's
 * socket write timeouts end a write to a client that stops reading its
 * connection, but no Tomcat timeout ends an HTTP/2 stream whose client withholds
 * its flow-control window, so untrusted HTTP/2 clients need a proxy that bounds
 * each stream's writes (see the module README, "Production settings"). A
 * response that fails part way is aborted: on Tomcat its HTTP/2
 * stream is reset or its HTTP/1.1 connection closed, and the async request
 * ends (see [ServletExchange]). A call whose client sends no deadline has no
 * time limit on its request and handler unless `connectrpc.max-timeout`
 * ([ConnectRpcProperties.maxTimeout]) is set, which public-facing servers need.
 *
 * Handlers run with the servlet request's Spring context: `RequestContextHolder`,
 * `LocaleContextHolder`, Spring Security's `SecurityContextHolder` and the SLF4J
 * MDC (see [RequestThreadContext]).
 *
 * The default [dispatcher] is a view of [Dispatchers.IO] limited to
 * [DEFAULT_HANDLER_THREADS]. Handlers written for Spring MVC block (JDBC, blocking
 * HTTP clients), and on [Dispatchers.Default] as few blocked handlers as there are
 * CPU cores would stall every call. Views of [Dispatchers.IO] are not bounded by
 * its own 64-thread limit and do not take threads from other views (kotlinx-coroutines
 * `Dispatchers.IO` KDoc, "Elasticity for limited parallelism"), so blocking here
 * starves neither the rest of the application nor this servlet's I/O, which never
 * holds a thread.
 *
 * If `serve` throws (a handler [Error]), the response is failed instead of being
 * completed as a success (see [ServletExchange.fail]) and the throwable still
 * reaches the thread's uncaught-exception handler.
 *
 * @param pathPrefix path, with one leading and no trailing slash, preceding
 *     `/<Procedure-Name>`; `""` for the root.
 * @param dispatcher runs the calls.
 * @param localeContextResolver resolves the locale handlers see through
 *     `LocaleContextHolder`; `null` keeps the one the filter chain installed.
 */
class ConnectServlet(
    val server: ConnectServer,
    val pathPrefix: String = "",
    dispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(DEFAULT_HANDLER_THREADS),
    private val localeContextResolver: ((HttpServletRequest) -> LocaleContext)? = null,
) : HttpServlet() {

    private val scope = CoroutineScope(SupervisorJob() + dispatcher + CoroutineName("connectrpc"))

    override fun service(request: HttpServletRequest, response: HttpServletResponse) {
        val async = request.startAsync(request, response)
        async.timeout = 0
        val exchange = ServletExchange(request, response, async, relativePath(request))
        val threadContext = RequestThreadContext.capture(request, response, localeContextResolver)
        recordProcedurePattern(request, pathPrefix, exchange.path, server.procedures)
        val call = scope.launch(threadContext, CoroutineStart.LAZY) { server.serve(exchange) }
        // A completion handler, not a finally block: a LAZY job cancelled by the container
        // before its first dispatch never runs its body.
        call.invokeOnCompletion { cause ->
            if (cause != null && cause !is CancellationException) exchange.fail()
            threadContext.requestCompleted()
            exchange.complete()
        }
        exchange.start(call)
        call.start()
    }

    /** Cancels calls still running when the container takes the servlet out of service. */
    override fun destroy() {
        scope.cancel()
    }

    /**
     * The path after the context path and [pathPrefix], still percent-encoded
     * (`getRequestURI` and `getContextPath` are not decoded, Servlet 6.1
     * `HttpServletRequest`).
     */
    private fun relativePath(request: HttpServletRequest): String {
        val path = request.requestURI.removePrefix(request.contextPath)
        return if (path.startsWith("$pathPrefix/")) path.substring(pathPrefix.length) else path
    }
}
