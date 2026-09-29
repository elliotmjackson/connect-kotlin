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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.StreamResult
import com.connectrpc.asConnectException
import com.connectrpc.http.Cancelable
import com.connectrpc.http.HTTPClientInterface
import com.connectrpc.http.HTTPMethod
import com.connectrpc.http.HTTPRequest
import com.connectrpc.http.HTTPResponse
import com.connectrpc.http.Stream
import com.connectrpc.http.UnaryHTTPRequest
import com.connectrpc.server.ConnectServer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okio.Buffer
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * An [HTTPClientInterface] that hands every request to a [ConnectServer] in
 * the same process, without a network. Meant for tests.
 *
 * Pass it to [com.connectrpc.impl.ProtocolClient] to run generated clients
 * against registered handlers over any protocol and codec:
 *
 * ```
 * val client = ProtocolClient(
 *     InMemoryHTTPClient(ConnectServer(registry)),
 *     ProtocolClientConfig(host = "http://in-memory", serializationStrategy = GoogleJavaProtobufStrategy()),
 * )
 * ```
 *
 * Exchanges behave like HTTP/2: request and response bodies are full duplex
 * and responses can end with trailers, so gRPC and bidirectional streams work.
 * The URL's path is the procedure path; the host is ignored. This is the
 * counterpart of connect-es `createRouterTransport` and of connect-py tests
 * that use `httpx.ASGITransport`.
 *
 * @param server The server that serves every request.
 * @param coroutineContext The context each call runs in on the server side.
 */
class InMemoryHTTPClient @JvmOverloads constructor(
    private val server: ConnectServer,
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : HTTPClientInterface {
    private val scope = CoroutineScope(coroutineContext + SupervisorJob(coroutineContext[Job]))

    override fun unary(request: UnaryHTTPRequest, onResult: (HTTPResponse) -> Unit): Cancelable {
        // A copy, so the request stays intact for client interceptors, as ConnectOkHttpClient does.
        val body = request.message.copy()
        val delivered = AtomicBoolean(false)
        val deliver = { response: HTTPResponse -> if (delivered.compareAndSet(false, true)) onResult(response) }
        val job = scope.launch {
            val exchange = UnaryExchange(request, body)
            val response = try {
                server.serve(exchange)
                exchange.response ?: failure(Code.UNKNOWN, "server returned without a response")
            } catch (e: CancellationException) {
                failure(Code.CANCELED, "call canceled", e)
            } catch (e: Throwable) {
                failure(Code.UNKNOWN, e.message, e)
            }
            deliver(response)
        }
        // A call canceled before it started never ran the block above.
        job.invokeOnCompletion { deliver(failure(Code.CANCELED, "call canceled")) }
        return { job.cancel() }
    }

    override fun stream(
        request: HTTPRequest,
        duplex: Boolean,
        onResult: suspend (StreamResult<Buffer>) -> Unit,
    ): Stream {
        val exchange = StreamExchange(request, onResult)
        val job = scope.launch {
            try {
                server.serve(exchange)
                exchange.complete(null)
            } catch (e: CancellationException) {
                exchange.complete(ConnectException(Code.CANCELED, "call canceled", e))
            } catch (e: Throwable) {
                exchange.complete(asConnectException(e))
            }
        }
        job.invokeOnCompletion {
            // Later sends fail instead of waiting for a reader that is gone.
            exchange.requestBody.close(IOException("call ended"))
            // A call canceled before it started never ran the block above.
            if (!exchange.completed) {
                scope.launch { exchange.complete(ConnectException(Code.CANCELED, "call canceled")) }
            }
        }
        return Stream(
            onSend = { buffer ->
                val chunk = Buffer()
                chunk.writeAll(buffer)
                exchange.requestBody.send(chunk)
                Result.success(Unit)
            },
            onSendClose = { exchange.requestBody.close() },
            onReceiveClose = {
                exchange.requestBody.close()
                if (!exchange.completed) job.cancel()
            },
        )
    }

    private fun failure(code: Code, message: String?, cause: Throwable? = null) = HTTPResponse(
        status = null,
        headers = emptyMap(),
        message = Buffer(),
        trailers = emptyMap(),
        cause = ConnectException(code, message, cause),
    )
}

/** The request side shared by unary and streaming exchanges. */
private abstract class InMemoryExchange(request: HTTPRequest, httpMethod: HTTPMethod) : HttpExchange {
    override val method: String = httpMethod.string
    override val path: String = request.url.encodedPath
    override val rawQuery: String? = request.url.encodedQuery.ifEmpty { null }

    // The client passes the content type apart from the headers; HTTP clients
    // send it from the body (OkHttp `RequestBody.contentType`), and GET has none.
    override val requestHeaders: Headers = request.headers.filterKeys { !it.equals("content-type", ignoreCase = true) } +
        if (httpMethod == HTTPMethod.POST) mapOf("content-type" to listOf(request.contentType)) else emptyMap()

    override val supportsTrailers: Boolean get() = true
}

private class UnaryExchange(request: UnaryHTTPRequest, private val body: Buffer) : InMemoryExchange(request, request.httpMethod) {
    var response: HTTPResponse? = null

    override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long = body.read(sink, maxBytes)

    override suspend fun respond(status: Int, headers: Headers, body: ByteArray) {
        response = HTTPResponse(status, headers, Buffer().write(body), emptyMap())
    }

    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) {
        val content = Buffer()
        val trailers = body.writeTo(
            object : ResponseSink {
                override suspend fun write(source: Buffer) {
                    content.writeAll(source)
                }

                override suspend fun flush() {}
            },
        )
        response = HTTPResponse(status, headers, content, trailers.lowercaseKeys())
    }
}

private class StreamExchange(
    request: HTTPRequest,
    private val onResult: suspend (StreamResult<Buffer>) -> Unit,
) : InMemoryExchange(request, HTTPMethod.POST) {
    // One element per Stream.send; closed normally by sendClose.
    val requestBody = Channel<Buffer>(Channel.BUFFERED)
    private val pending = Buffer()
    private var responded = false

    // Set when respond or respondStreaming ended with an exception: the response is incomplete.
    private var aborted = false

    @Volatile
    var completed = false
        private set

    override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
        while (pending.exhausted()) {
            pending.writeAll(requestBody.receiveCatching().getOrNull() ?: return -1)
        }
        return pending.read(sink, maxBytes)
    }

    override suspend fun respond(status: Int, headers: Headers, body: ByteArray) {
        responded = true
        if (status != 200) {
            // As ConnectOkHttpClient: the status decides the code and the headers become the metadata.
            return complete(ConnectException(Code.fromHTTPStatus(status), "unexpected HTTP status: $status", metadata = headers))
        }
        abortOnFailure {
            onResult(StreamResult.Headers(headers))
            val envelopes = Envelopes(onResult)
            envelopes.write(Buffer().write(body))
            complete(envelopes.truncation())
        }
    }

    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) {
        responded = true
        abortOnFailure {
            onResult(StreamResult.Headers(headers))
            val envelopes = Envelopes(onResult)
            val trailers = body.writeTo(
                object : ResponseSink {
                    override suspend fun write(source: Buffer) = envelopes.write(source)

                    override suspend fun flush() {}
                },
            )
            complete(envelopes.truncation(), trailers.lowercaseKeys())
        }
    }

    /** Marks the response aborted when [block] throws (see [HttpExchange], "Aborting"). */
    private suspend fun abortOnFailure(block: suspend () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            aborted = true
            throw e
        }
    }

    /** Delivers the final result once; later calls do nothing. */
    suspend fun complete(cause: ConnectException?, trailers: Headers = emptyMap()) {
        if (completed) return
        completed = true
        val result = when {
            cause != null -> cause

            // As an HTTP/2 client reads the server's RST_STREAM(CANCEL) (PROTOCOL-HTTP2.md "Errors").
            aborted -> ConnectException(Code.CANCELED, "the server aborted the response")

            !responded -> ConnectException(Code.UNKNOWN, "server returned without a response")

            else -> null
        }
        // Also runs once the call is canceled, so the client learns how it ended.
        withContext(NonCancellable) { onResult(StreamResult.Complete(result, trailers)) }
    }
}

/**
 * Splits a response body into envelopes (5-byte prefix and message), the unit
 * the client's protocol implementations expect in each [StreamResult.Message].
 */
private class Envelopes(private val onResult: suspend (StreamResult<Buffer>) -> Unit) {
    private val buffer = Buffer()

    suspend fun write(source: Buffer) {
        buffer.writeAll(source)
        while (buffer.size >= 5) {
            val length = buffer.peek().run {
                skip(1)
                readInt().toLong() and 0xffffffffL
            }
            if (buffer.size < 5 + length) return
            val envelope = Buffer()
            envelope.write(buffer, 5 + length)
            onResult(StreamResult.Message(envelope))
        }
    }

    fun truncation(): ConnectException? = if (buffer.size > 0) {
        ConnectException(Code.INTERNAL_ERROR, "response body ended inside a message")
    } else {
        null
    }
}

private fun Headers.lowercaseKeys(): Headers = entries.groupBy({ it.key.lowercase() }, { it.value }).mapValues { it.value.flatten() }
