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
 * Server-side interceptor for cross-cutting concerns: auth, logging,
 * tracing, request/response transformation, etc.
 *
 * An interceptor returns a wrapped [Handler] that intercepts the original.
 * Override only the stream-type methods you care about; the defaults are
 * identity (return [next] unchanged).
 *
 * Interceptors are applied in registration order. The first interceptor
 * registered runs outermost — it sees the request first and the response
 * last. Mirrors connect-go's `Interceptor` semantics.
 *
 * Example — request logging:
 * ```
 * class LoggingInterceptor(private val log: Logger) : ServerInterceptor {
 *     override fun <Req : Any, Res : Any> wrapUnary(
 *         next: UnaryHandler<Req, Res>,
 *     ): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
 *         override val methodSpec = next.methodSpec
 *         override suspend fun handle(request: Req, ctx: HandlerContext): Res {
 *             log.info("→ {}", ctx.procedure)
 *             try {
 *                 val response = next.handle(request, ctx)
 *                 log.info("← {} ok", ctx.procedure)
 *                 return response
 *             } catch (ex: Throwable) {
 *                 log.warn("← {} {}", ctx.procedure, ex.message)
 *                 throw ex
 *             }
 *         }
 *     }
 * }
 * ```
 */
interface ServerInterceptor {
    fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>): UnaryHandler<Req, Res> = next

    fun <Req : Any, Res : Any> wrapServerStream(
        next: ServerStreamHandler<Req, Res>,
    ): ServerStreamHandler<Req, Res> = next

    fun <Req : Any, Res : Any> wrapClientStream(
        next: ClientStreamHandler<Req, Res>,
    ): ClientStreamHandler<Req, Res> = next

    fun <Req : Any, Res : Any> wrapBidi(
        next: BidiStreamHandler<Req, Res>,
    ): BidiStreamHandler<Req, Res> = next
}
