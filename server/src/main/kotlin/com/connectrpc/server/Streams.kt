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

import com.connectrpc.Headers

/**
 * Server-side view of an inbound message stream from the client.
 * Used by client-streaming and bidi handlers.
 */
interface ClientMessageStream<Req : Any> {
    /** Headers sent by the client at the start of the request. */
    val headers: Headers

    /**
     * Reads the next message from the client.
     * Returns null when the client has finished sending.
     */
    suspend fun receive(): Req?
}

/**
 * Server-side view of an outbound message stream to the client.
 * Used by server-streaming handlers and (in combination with [ClientMessageStream])
 * by bidi handlers.
 *
 * Response headers and trailers are populated through [HandlerContext.responseHeaders]
 * and [HandlerContext.responseTrailers]. Mutate those before the first [send] for
 * headers, and any time before the handler returns for trailers.
 */
interface ServerMessageStream<Res : Any> {
    /** Sends a message to the client. */
    suspend fun send(message: Res)
}

/**
 * Server-side view of a bidirectional stream: combines [ClientMessageStream]
 * reads with [ServerMessageStream] writes. Sends and receives are
 * independent — handlers may interleave them freely (full-duplex over
 * HTTP/2) or read-then-write (half-duplex). The transport may buffer either
 * side; do not rely on a [send] being delivered before a subsequent
 * [receive] returns.
 */
interface BidiStream<Req : Any, Res : Any> :
    ClientMessageStream<Req>,
    ServerMessageStream<Res>
