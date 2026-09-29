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

package com.connectrpc.server.http

import com.connectrpc.Headers
import okio.Buffer

/**
 * One HTTP request and its response, as seen by [com.connectrpc.server.ConnectServer].
 *
 * A transport adapter (Ktor, Servlet, ...) implements this interface over its
 * framework's request/response objects and calls
 * [com.connectrpc.server.ConnectServer.serve] once per request. All protocol
 * behaviour (routing, content negotiation, compression, limits, timeouts,
 * framing, errors and trailers) lives in the server; the adapter only moves
 * bytes and headers.
 *
 * Cancellation: the coroutine that calls `serve` is the call's lifetime. The
 * adapter cancels it when the client goes away (connection close, HTTP/2
 * RST_STREAM). [readRequestBody] and [ResponseSink.write] throw
 * [java.io.IOException] or [kotlinx.coroutines.CancellationException] once the
 * client is gone. Once [respond] or [respondStreaming] has returned, the
 * call has ended with the code of that response: cancelling it then, as the
 * adapter may because the response's end closed the HTTP/2 stream or the
 * connection, does not change the code `serve` reports.
 *
 * Writes: the call's deadline does not bound [respond], [respondStreaming] or
 * [ResponseSink.write]; a client that stops reading holds them until the
 * transport gives up on it (its write timeout, the HTTP/2 stream or
 * connection closing), which the adapter documents.
 *
 * Aborting: when [respond] or [respondStreaming] ends with an exception,
 * cancellation included (a handler [Error] after the response started
 * cancels it), or [StreamingBody.writeTo] throws, the response is
 * incomplete: the adapter must rethrow and, at the latest once `serve`
 * returns, abort the exchange (reset the HTTP/2 stream, close the HTTP/1.1
 * connection) so that the client cannot take part of a response for all of
 * it and the transport releases the request. Bytes already sent stay sent:
 * a response whose last byte had reached the client is whole to it. Aborting
 * must not cancel the coroutine calling `serve` before it returns: the call
 * ends by itself, with the code it reports to
 * [com.connectrpc.server.ServerObserver].
 */
interface HttpExchange {
    /** Request method, upper case (e.g. `POST`, `GET`). */
    val method: String

    /**
     * Request path relative to the adapter's mount point, starting with `/`,
     * not percent-decoded (e.g. `/connectrpc.eliza.v1.ElizaService/Say`).
     */
    val path: String

    /** Raw query string without the leading `?`, still percent-encoded; null if absent. */
    val rawQuery: String?

    /** Request headers. Name case does not matter; repeated fields keep every value. */
    val requestHeaders: Headers

    /**
     * Whether [respondStreaming] can end the response with HTTP trailers. The
     * gRPC protocol requires them (PROTOCOL-HTTP2.md "Responses"); requests
     * that need them are rejected when this is false.
     */
    val supportsTrailers: Boolean

    /**
     * Reads at most [maxBytes] (> 0) bytes of the request body into [sink].
     * Suspends until at least one byte is available or the body has ended.
     * Throws [java.io.IOException] when the client has gone away, or a
     * [com.connectrpc.ConnectException] to end the call with that error while
     * the client is still connected.
     *
     * @return the number of bytes appended, or -1 at the end of the body.
     */
    suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long

    /**
     * Sends a complete response. [headers] names are lower case; the adapter
     * sets `Content-Length` from [body]. Called at most once per exchange, and
     * never together with [respondStreaming].
     */
    suspend fun respond(status: Int, headers: Headers, body: ByteArray)

    /**
     * Sends a streamed response: commits [status] and [headers], then calls
     * [StreamingBody.writeTo]. Returns once the response, including any trailers
     * returned by [body], has been handed to the transport.
     */
    suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody)
}

/** Producer of a streamed response body. */
fun interface StreamingBody {
    /**
     * Writes the body to [sink] and returns the HTTP trailers to end the
     * response with. Trailers are only returned when
     * [HttpExchange.supportsTrailers] is true; otherwise the result is empty.
     */
    suspend fun writeTo(sink: ResponseSink): Headers
}

/** The write side of a streamed response body. */
interface ResponseSink {
    /**
     * Writes all of [source], consuming it. Suspends while the transport is
     * not accepting more bytes, which is how back-pressure reaches handlers.
     */
    suspend fun write(source: Buffer)

    /** Sends everything written so far to the client. */
    suspend fun flush()
}
