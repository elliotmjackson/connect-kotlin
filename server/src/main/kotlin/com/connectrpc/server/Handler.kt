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

import com.connectrpc.MethodSpec

/**
 * Server-side implementation of a single RPC procedure, one subtype per
 * [com.connectrpc.StreamType].
 *
 * Handlers throw [com.connectrpc.ConnectException] to fail the call with a
 * code; the server encodes it for the protocol in use. Any other exception is
 * logged and sent as `unknown` without its message.
 */
sealed interface Handler<Req : Any, Res : Any> {
    /** The procedure path, message classes and stream type served by this handler. */
    val methodSpec: MethodSpec<Req, Res>
}

/** Handles a unary procedure: one request, one response. */
interface UnaryHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    /**
     * Handles one call.
     *
     * @param request The request message.
     * @param ctx The call's request metadata and response headers and trailers.
     * @return The response message.
     */
    suspend fun handle(request: Req, ctx: HandlerContext): Res
}

/** Handles a server-streaming procedure: one request, a stream of responses. */
interface ServerStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    /**
     * Handles one call. The response stream ends when this function returns.
     *
     * @param request The request message.
     * @param ctx The call's request metadata and response headers and trailers.
     * @param stream The stream to send response messages on.
     */
    suspend fun handle(request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>)
}

/** Handles a client-streaming procedure: a stream of requests, one response. */
interface ClientStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    /**
     * Handles one call.
     *
     * @param stream The stream to receive request messages from.
     * @param ctx The call's request metadata and response headers and trailers.
     * @return The response message.
     */
    suspend fun handle(stream: ClientMessageStream<Req>, ctx: HandlerContext): Res
}

/** Handles a bidirectional-streaming procedure: streams of requests and responses. */
interface BidiStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    /**
     * Handles one call. The response stream ends when this function returns.
     *
     * @param stream The stream to receive request messages from and send response messages on.
     * @param ctx The call's request metadata and response headers and trailers.
     */
    suspend fun handle(stream: BidiStream<Req, Res>, ctx: HandlerContext)
}

/** Returns a [UnaryHandler] for [methodSpec] that calls [implementation]. */
fun <Req : Any, Res : Any> unaryHandler(
    methodSpec: MethodSpec<Req, Res>,
    implementation: suspend (request: Req, ctx: HandlerContext) -> Res,
): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
    override val methodSpec = methodSpec
    override suspend fun handle(request: Req, ctx: HandlerContext): Res = implementation(request, ctx)
}

/** Returns a [ServerStreamHandler] for [methodSpec] that calls [implementation]. */
fun <Req : Any, Res : Any> serverStreamHandler(
    methodSpec: MethodSpec<Req, Res>,
    implementation: suspend (request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>) -> Unit,
): ServerStreamHandler<Req, Res> = object : ServerStreamHandler<Req, Res> {
    override val methodSpec = methodSpec
    override suspend fun handle(request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>) = implementation(request, ctx, stream)
}

/** Returns a [ClientStreamHandler] for [methodSpec] that calls [implementation]. */
fun <Req : Any, Res : Any> clientStreamHandler(
    methodSpec: MethodSpec<Req, Res>,
    implementation: suspend (stream: ClientMessageStream<Req>, ctx: HandlerContext) -> Res,
): ClientStreamHandler<Req, Res> = object : ClientStreamHandler<Req, Res> {
    override val methodSpec = methodSpec
    override suspend fun handle(stream: ClientMessageStream<Req>, ctx: HandlerContext): Res = implementation(stream, ctx)
}

/** Returns a [BidiStreamHandler] for [methodSpec] that calls [implementation]. */
fun <Req : Any, Res : Any> bidiStreamHandler(
    methodSpec: MethodSpec<Req, Res>,
    implementation: suspend (stream: BidiStream<Req, Res>, ctx: HandlerContext) -> Unit,
): BidiStreamHandler<Req, Res> = object : BidiStreamHandler<Req, Res> {
    override val methodSpec = methodSpec
    override suspend fun handle(stream: BidiStream<Req, Res>, ctx: HandlerContext) = implementation(stream, ctx)
}
