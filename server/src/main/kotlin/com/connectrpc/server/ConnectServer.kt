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

package com.connectrpc.server

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.server.http.HttpExchange
import com.connectrpc.server.internal.CaseInsensitiveHeaders
import com.connectrpc.server.internal.ConnectUnaryCall
import com.connectrpc.server.internal.EMPTY
import com.connectrpc.server.internal.EnvelopedCall
import com.connectrpc.server.internal.LOG
import com.connectrpc.server.internal.Protocol
import com.connectrpc.server.internal.Query
import com.connectrpc.server.internal.Route
import com.connectrpc.server.internal.Runtime
import com.connectrpc.server.internal.canonicalContentType
import com.connectrpc.server.internal.connectErrorJson
import com.connectrpc.server.internal.first
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.io.IOException
import java.util.logging.Level
import kotlin.time.Duration

/**
 * Serves the procedures of a [HandlerRegistry] over the Connect, gRPC and
 * gRPC-Web protocols, independent of the HTTP server.
 *
 * Transport adapters turn each HTTP request into an [HttpExchange] and call
 * [serve]; see that interface for the adapter's obligations.
 */
class ConnectServer(
    registry: HandlerRegistry,
    config: ServerConfig = ServerConfig(),
) {
    private val runtime = Runtime(config)
    private val observer = config.observer

    private val routes: Map<String, Route> = registry.handlers.mapValues { (procedure, handler) ->
        @Suppress("UNCHECKED_CAST")
        Route(
            procedure = procedure,
            handler = handler as Handler<Any, Any>,
            codecs = registry.codecs,
            interceptors = registry.interceptors + registry.interceptorsFor(procedure),
        )
    }

    /** Procedure names served, e.g. `connectrpc.eliza.v1.ElizaService/Say`; paths are `/` + name. */
    val procedures: Set<String> get() = routes.keys

    /**
     * Serves one request to completion. Returns normally when the response has
     * been handed to the transport, which may still be writing it (see
     * [ServerObserver.Call.onEnd]), even if the calling coroutine is cancelled
     * after that, or when the transport failed and nothing more can be sent;
     * rethrows cancellation of the calling coroutine that comes earlier.
     *
     * Requests are classified as connect-go does (`connecthttp/handler.go:42-122`):
     * - unknown path: 404;
     * - method other than POST, or GET where not allowed: 405 with `Allow`;
     * - unsupported Content-Type or GET `encoding`: 415 with `Accept-Post`;
     * - GET with a body: 415.
     */
    suspend fun serve(exchange: HttpExchange) {
        val headers = CaseInsensitiveHeaders.of(exchange.requestHeaders)
        val route = routes[exchange.path.removePrefix("/")]
        val request = classify(exchange, headers, route)
        val observed = observer?.onStart(route?.procedure, request.protocol, headers)
        var code: Code? = Code.CANCELED
        try {
            code = request.serve()
        } catch (e: IOException) {
            currentCoroutineContext().ensureActive()
            LOG.log(Level.FINE, "transport failed for ${exchange.path}", e)
        } finally {
            observed?.onEnd(code)
        }
    }

    /**
     * Stops serving calls. Calls that start from now on fail with [error]
     * before their request is read. Calls in flight get [gracePeriod] to
     * finish; then their handlers are cancelled and the calls end with
     * [error], in the protocol's error shape, instead of being cut off when
     * the HTTP server closes their connections. Long-lived streams need this:
     * HTTP servers stop gracefully by waiting for requests to end (connect-go
     * leaves it to the application, faq.md:295-310; connect-node's
     * `shutdownTimeoutMs` and `shutdownError` do the same, server-plugins.md
     * 86-91).
     *
     * Returns once the handlers still running after [gracePeriod] have been
     * cancelled; their responses are then sent as usual. The adapters call it
     * when their HTTP server begins to stop. Calling it again keeps the first
     * call's error.
     *
     * @param gracePeriod How long calls in flight may run before they are
     *     cancelled.
     * @param error The error calls end with; `unavailable` by default, which
     *     clients may retry elsewhere.
     */
    suspend fun shutdown(
        gracePeriod: Duration = Duration.ZERO,
        error: ConnectException = ConnectException(Code.UNAVAILABLE, "server is shutting down"),
    ) = runtime.shutdown(gracePeriod, error)

    /** A classified request: its protocol, null for an HTTP status alone, and how to serve it. */
    private class Request(val protocol: String?, val serve: suspend () -> Code?)

    private fun classify(exchange: HttpExchange, headers: Headers, route: Route?): Request {
        // protocol.md "HTTP to Error Code": clients read 404 as unimplemented.
        route ?: return Request(null) {
            exchange.respond(404, emptyMap(), EMPTY)
            Code.UNIMPLEMENTED
        }
        return when (exchange.method) {
            "POST" -> {
                val contentType = headers.first("content-type")?.trim()
                val (protocol, codec) = contentType?.let(::canonicalContentType)?.let(route.contentTypes::get)
                    ?: return unsupportedMediaType(exchange, route)
                Request(protocol.protocolName) {
                    if (protocol == Protocol.CONNECT_UNARY) {
                        ConnectUnaryCall(runtime, route, exchange, headers, codec, contentType, query = null).run()
                    } else {
                        EnvelopedCall(runtime, route, exchange, headers, protocol, codec, contentType).run()?.code
                    }
                }
            }

            "GET" -> {
                if (!route.allowsGet) return methodNotAllowed(exchange, route)
                val query = try {
                    Query.parse(exchange.rawQuery)
                } catch (e: ConnectException) {
                    return Request(Protocol.CONNECT_UNARY.protocolName) {
                        exchange.respond(400, mapOf("content-type" to listOf("application/json")), connectErrorJson(e))
                        e.code
                    }
                }
                val codec = query.first("encoding")?.let(route.getCodecs::get)
                    ?: return unsupportedMediaType(exchange, route)
                Request(Protocol.CONNECT_UNARY.protocolName) {
                    ConnectUnaryCall(runtime, route, exchange, headers, codec, "application/${codec.name}", query).run()
                }
            }

            else -> methodNotAllowed(exchange, route)
        }
    }

    // protocol.md "HTTP to Error Code": clients read 405 and 415 as unknown.
    private fun methodNotAllowed(exchange: HttpExchange, route: Route) = Request(null) {
        exchange.respond(405, mapOf("allow" to listOf(route.allow)), EMPTY)
        Code.UNKNOWN
    }

    private fun unsupportedMediaType(exchange: HttpExchange, route: Route) = Request(null) {
        exchange.respond(415, mapOf("accept-post" to listOf(route.acceptPost)), EMPTY)
        Code.UNKNOWN
    }
}
