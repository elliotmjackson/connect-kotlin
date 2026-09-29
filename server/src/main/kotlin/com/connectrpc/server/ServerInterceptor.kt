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

/**
 * Interceptors can be registered with a [HandlerRegistry] to observe and/or
 * alter calls: authentication, logging, tracing, metrics.
 *
 * [interceptCall] runs once per call of every stream type, before any request
 * message is read: put policy that must hold for every call, such as
 * authentication, there. Each `wrap*` function returns a handler that calls,
 * or replaces, the handler it is given, for work on messages. The defaults
 * pass the call through unchanged, so implement only what you need.
 * [ConnectServer] calls the `wrap*` functions once per procedure, when it is
 * created.
 *
 * The first interceptor registered is outermost: it sees the call first and
 * its result last, as in connect-go. Every [interceptCall] runs before the
 * request is read and before any `wrap*` handler.
 *
 * Example, rejecting calls without a known bearer token:
 * ```
 * class AuthInterceptor(private val tokens: Set<String>) : ServerInterceptor {
 *     override suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T {
 *         val token = ctx.requestHeaders["authorization"]?.firstOrNull()?.removePrefix("Bearer ")
 *         if (token !in tokens) throw ConnectException(Code.UNAUTHENTICATED, "invalid token")
 *         return next()
 *     }
 * }
 * ```
 *
 * Example, logging failed unary calls with `java.util.logging`:
 * ```
 * class LoggingInterceptor(private val log: Logger) : ServerInterceptor {
 *     override fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>) =
 *         unaryHandler(next.methodSpec) { request, ctx ->
 *             try {
 *                 next.handle(request, ctx)
 *             } catch (e: ConnectException) {
 *                 log.warning("${ctx.procedure} failed: ${e.code.codeName}")
 *                 throw e
 *             }
 *         }
 * }
 * ```
 */
interface ServerInterceptor {
    /**
     * Runs one call of any stream type: returns [next]'s result or throws.
     * The request body has not been read, so a
     * [com.connectrpc.ConnectException] thrown before [next] fails the call
     * without its messages being read, decompressed or decoded, in the error
     * shape of the call's protocol. [next] runs the inner interceptors, then
     * reads the request and runs the handler; errors from it pass through
     * here, including a request that fails to decode. Run [next] inside
     * `withContext` to give the handler coroutine context elements, such as
     * the authenticated principal.
     *
     * @param ctx The call's request metadata and response headers and trailers.
     * @param next The rest of the call.
     * @return The result of [next].
     */
    suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T = next()

    /** Returns the handler to call instead of [next] for a unary procedure. */
    fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>): UnaryHandler<Req, Res> = next

    /** Returns the handler to call instead of [next] for a server-streaming procedure. */
    fun <Req : Any, Res : Any> wrapServerStream(
        next: ServerStreamHandler<Req, Res>,
    ): ServerStreamHandler<Req, Res> = next

    /** Returns the handler to call instead of [next] for a client-streaming procedure. */
    fun <Req : Any, Res : Any> wrapClientStream(
        next: ClientStreamHandler<Req, Res>,
    ): ClientStreamHandler<Req, Res> = next

    /** Returns the handler to call instead of [next] for a bidirectional-streaming procedure. */
    fun <Req : Any, Res : Any> wrapBidi(
        next: BidiStreamHandler<Req, Res>,
    ): BidiStreamHandler<Req, Res> = next
}
