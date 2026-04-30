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
 */
interface ServerMessageStream<Res : Any> {
    /**
     * Sets a response header. Must be called before the first [send].
     * Calls after the first [send] are an error.
     */
    fun setHeader(name: String, values: List<String>)

    /** Sends a message to the client. Implicitly flushes pending headers if any. */
    suspend fun send(message: Res)

    /**
     * Sets a response trailer. Must be called before [handle] returns; once the
     * stream closes, trailers are flushed alongside the closing frame.
     */
    fun setTrailer(name: String, values: List<String>)
}

/** Bidi stream: combined client-side reads and server-side writes. */
interface BidiStream<Req : Any, Res : Any> :
    ClientMessageStream<Req>,
    ServerMessageStream<Res>
