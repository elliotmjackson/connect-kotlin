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
 * Server-side implementation of a single RPC procedure.
 *
 * Handlers come in four flavors, one per [com.connectrpc.StreamType]. Each carries
 * the [MethodSpec] that identifies its procedure path and the request/response
 * Kotlin classes used by codecs.
 *
 * Handlers may throw [com.connectrpc.ConnectException] to signal a typed error;
 * the framework serializes it according to the active protocol.
 */
sealed interface Handler<Req : Any, Res : Any> {
    val methodSpec: MethodSpec<Req, Res>
}

/** Request-response procedure. */
interface UnaryHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    suspend fun handle(request: Req, ctx: HandlerContext): Res
}

/** Single request, streamed responses. */
interface ServerStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    suspend fun handle(request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>)
}

/** Streamed requests, single response. */
interface ClientStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    suspend fun handle(stream: ClientMessageStream<Req>, ctx: HandlerContext): Res
}

/** Streamed requests and responses. */
interface BidiStreamHandler<Req : Any, Res : Any> : Handler<Req, Res> {
    suspend fun handle(stream: BidiStream<Req, Res>, ctx: HandlerContext)
}
