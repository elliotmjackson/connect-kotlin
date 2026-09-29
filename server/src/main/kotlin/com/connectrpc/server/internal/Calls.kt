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

package com.connectrpc.server.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.compression.ServerCompressionPool
import com.connectrpc.server.http.HttpExchange
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import okio.Buffer
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import kotlin.time.Duration
import kotlin.time.TimeMark
import kotlin.time.TimeSource

internal val EMPTY = ByteArray(0)

/** Settings and state shared by every call of one server. */
internal class Runtime(val config: ServerConfig) {
    val compressions = Compressions(config.compressionPools)

    // The jobs of the handlers running now.
    private val running: MutableSet<Job> = ConcurrentHashMap.newKeySet()

    // Set once by shutdown; every call from then on ends with it.
    @Volatile
    private var shutdownError: ConnectException? = null

    /** The deadline of a call starting now whose client sent [timeout], limited by [ServerConfig.maxTimeout]. */
    fun deadline(timeout: Duration?): TimeMark? {
        val max = config.maxTimeout
        val effective = if (max != null && (timeout == null || timeout > max)) max else timeout
        return effective?.let { TimeSource.Monotonic.markNow() + it }
    }

    /**
     * Runs [block], the handler or the read of a GET body, until the deadline
     * of [ctx], and returns its result or the error to report as a failed
     * [Result] holding a [ConnectException]. While it runs, [shutdown] can
     * cancel it.
     *
     * - [ConnectException]: sent as is.
     * - Deadline expiry: `deadline_exceeded` (protocol.md "Timeout"), also
     *   when [block] returns or throws a non-cancellation [Exception] after
     *   the deadline. Code that blocks without suspending is not interrupted:
     *   the call ends only when it returns or throws, and its late result or
     *   error is discarded (a late non-[ConnectException] is still logged).
     * - After [shutdown], or when shutdown cancels [block]: the shutdown error,
     *   without running [block] in the first case.
     * - Any other [Exception]: logged, then sent as `unknown` with no message,
     *   so internal details never reach the client (connect-go
     *   `connecthttp/error.go:92-115`).
     * - Cancellation of the call itself (client gone) and [Error]s propagate.
     */
    suspend fun <T> runHandler(ctx: HandlerContext, block: suspend () -> T): Result<T> = try {
        coroutineScope {
            val job = coroutineContext.job
            running += job
            try {
                // Read after registering, so shutdown either cancels this job or this call sees the error.
                shutdownError?.let { throw it }
                val timeout = ctx.timeRemaining()
                val result = try {
                    if (timeout == null) block() else withTimeout(timeout) { block() }
                } catch (e: Exception) {
                    if (e is CancellationException || ctx.deadline?.hasPassedNow() != true) throw e
                    if (e !is ConnectException) LOG.log(Level.WARNING, "handler for ${ctx.procedure} failed after its deadline", e)
                    return@coroutineScope Result.failure(ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
                }
                if (ctx.deadline?.hasPassedNow() == true) {
                    Result.failure(ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
                } else {
                    Result.success(result)
                }
            } finally {
                running -= job
            }
        }
    } catch (e: TimeoutCancellationException) {
        currentCoroutineContext().ensureActive()
        Result.failure(ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
    } catch (e: CancellationException) {
        currentCoroutineContext().ensureActive()
        Result.failure(shutdownError ?: ConnectException(Code.CANCELED, "canceled"))
    } catch (e: ConnectException) {
        Result.failure(e)
    } catch (e: Exception) {
        LOG.log(Level.WARNING, "handler for ${ctx.procedure} failed", e)
        Result.failure(ConnectException(Code.UNKNOWN))
    }

    /** See [com.connectrpc.server.ConnectServer.shutdown]. */
    suspend fun shutdown(gracePeriod: Duration, error: ConnectException) {
        synchronized(this) {
            if (shutdownError == null) shutdownError = error
        }
        withTimeoutOrNull(gracePeriod) { running.toList().joinAll() }
        for (job in running) job.cancel()
    }
}

private fun checkSendLimit(config: ServerConfig, payload: Buffer) {
    if (config.sendMaxBytes > 0 && payload.size > config.sendMaxBytes) {
        throw ConnectException(
            Code.RESOURCE_EXHAUSTED,
            "message size ${payload.size} exceeds sendMaxBytes ${config.sendMaxBytes}",
        )
    }
}

/**
 * `Connect-Protocol-Version` / `connect=` check (protocol.md
 * "Connect-Protocol-Version"): a present value must be the one
 * defined; absence is only an error when [required].
 */
internal fun checkConnectVersion(value: String?, expected: String, name: String, required: Boolean) {
    if (value == null) {
        if (required) throw ConnectException(Code.INVALID_ARGUMENT, "missing required $name: set it to \"$expected\"")
    } else if (value.trim() != expected) {
        throw ConnectException(Code.INVALID_ARGUMENT, "$name must be \"$expected\": got \"${sanitizeValue(value).take(32)}\"")
    }
}

/** Connect unary over POST or GET (protocol.md "Unary-Request", "Unary-Get-Request"). */
internal class ConnectUnaryCall(
    private val runtime: Runtime,
    private val route: Route,
    private val exchange: HttpExchange,
    private val headers: Headers,
    private val codec: Codec,
    private val responseContentType: String,
    private val query: Query?,
) {
    private val config = runtime.config
    private val isGet = query != null

    /** Serves the call; returns the code sent, or null on success. */
    suspend fun run(): Code? {
        val deadline: TimeMark?
        val compressions: Pair<ServerCompressionPool?, ServerCompressionPool?>
        // Checked before the GET body probe, which waits on the client: none of them needs the body.
        try {
            if (query != null) {
                checkConnectVersion(query.first("connect"), "v1", "query parameter connect", config.requireConnectProtocolHeader)
            } else {
                checkConnectVersion(headers.first("connect-protocol-version"), "1", "Connect-Protocol-Version header", config.requireConnectProtocolHeader)
            }
            deadline = runtime.deadline(parseConnectTimeout(headers.first("connect-timeout-ms")))
            val sent = if (query != null) query.first("compression") else headers.first("content-encoding")
            compressions = runtime.compressions.negotiate(sent, headers.first("accept-encoding"))
        } catch (e: ConnectException) {
            return respondError(e, null)
        }
        val (requestCompression, responseCompression) = compressions
        // One context, so that one deadline bounds the GET body probe and the handler.
        val ctx = HandlerContext(route.procedure, headers, exchange.method, deadline, query?.strings())
        if (isGet) rejectGetBody(ctx)?.let { return it }
        var body = runtime.runHandler(ctx) {
            val response = route.callUnary(ctx) { readRequest(requestCompression) }
            route.encode(codec, response).also { checkSendLimit(config, it) }
        }.getOrElse {
            return respondError(it as ConnectException, ctx)
        }
        val out = MetadataBuilder().set("content-type", responseContentType)
        if (responseCompression != null && body.size >= config.compressMinBytes) {
            body = responseCompression.compress(body)
            out.set("content-encoding", responseCompression.name())
        }
        commonHeaders(out)
        out.addApplication(ctx.responseHeaders)
        // Unary trailers travel as `trailer-` headers (protocol.md "Unary-Response").
        out.addApplication(ctx.responseTrailers, prefix = "trailer-")
        exchange.respond(200, out.build(), body.readByteArray())
        return null
    }

    /**
     * Answers a GET that has a body with 415, as connect-go does
     * (`connecthttp/handler.go:81-94`). Reads at most one byte, as a call of
     * its own: within the deadline of [ctx], and cancelled by shutdown.
     * Returns the code of the call when it is over, or null when the body is
     * empty.
     */
    private suspend fun rejectGetBody(ctx: HandlerContext): Code? {
        val announced = headers.first("content-length")?.trim()?.toLongOrNull()?.let { it > 0 } == true
        val reader = RequestReader(exchange, config.readMaxBytes)
        val hasBody = announced ||
            runtime.runHandler(ctx) { reader.hasBody() }.getOrElse {
                val error = it as ConnectException
                if (reader.transportFailed) {
                    // The client has gone away; nobody is left to answer.
                    LOG.log(Level.FINE, "transport failed for ${exchange.path}", error)
                    return error.code
                }
                return respondError(error, ctx)
            }
        if (!hasBody) return null
        exchange.respond(415, emptyMap(), EMPTY)
        return Code.UNKNOWN
    }

    /** Reads and decodes the request message, from the body or the GET query. */
    private suspend fun readRequest(requestCompression: ServerCompressionPool?): Any {
        val wire = if (query != null) getMessage(query) else RequestReader(exchange, config.readMaxBytes).readAll()
        // Zero-length content is never decompressed (protocol.md "Content-Encoding").
        val payload = if (requestCompression != null && wire.size > 0) {
            decompressBounded(requestCompression, wire, config.readMaxBytes)
        } else {
            wire
        }
        return route.decode(codec, payload)
    }

    /**
     * Unary error (protocol.md "Unary-Response"): status from the code table,
     * `application/json` whatever the request codec, never compressed. The
     * error's metadata goes into the response headers, as in connect-es
     * (`protocol-connect/handler-factory.ts:283-288`) and connect-go v1.20.0
     * (`protocol_connect.go:768-771`), so plain HTTP clients see headers such
     * as `WWW-Authenticate` and `Retry-After`. Returns the error's code.
     */
    private suspend fun respondError(error: ConnectException, ctx: HandlerContext?): Code {
        val out = MetadataBuilder().set("content-type", "application/json")
        commonHeaders(out)
        if (ctx != null) {
            out.addApplication(ctx.responseHeaders)
            out.addApplication(ctx.responseTrailers, prefix = "trailer-")
        }
        out.addApplication(error.metadata)
        exchange.respond(error.code.connectHttpStatus(), out.build(), connectErrorJson(error))
        return error.code
    }

    private fun commonHeaders(out: MetadataBuilder) {
        runtime.compressions.advertised?.let { out.set("accept-encoding", it) }
        // GET responses depend on Accept-Encoding (connect-go `connecthttp/protocol_connect.go:799-806`).
        if (isGet) out.set("vary", "Accept-Encoding")
    }

    /**
     * The GET `message` parameter: URL-safe base64 with optional padding when
     * `base64=1`, raw bytes otherwise (protocol.md "Unary-Get-Request").
     */
    private fun getMessage(query: Query): Buffer {
        val raw = query.bytes("message")
            ?: throw ConnectException(Code.INVALID_ARGUMENT, "missing message parameter")
        val bytes = if (query.first("base64") == "1") {
            try {
                java.util.Base64.getUrlDecoder().decode(raw)
            } catch (e: IllegalArgumentException) {
                throw ConnectException(Code.INVALID_ARGUMENT, "message parameter is not URL-safe base64", e)
            }
        } else {
            raw
        }
        if (config.readMaxBytes > 0 && bytes.size > config.readMaxBytes) {
            throw ConnectException(Code.RESOURCE_EXHAUSTED, "message size ${bytes.size} is larger than configured max ${config.readMaxBytes}")
        }
        return Buffer().write(bytes)
    }
}

/**
 * Connect streaming, gRPC and gRPC-Web: enveloped messages in both
 * directions, status at the end of the response (protocol.md
 * "Streaming-Response"; PROTOCOL-HTTP2.md "Responses"; PROTOCOL-WEB.md).
 */
internal class EnvelopedCall(
    private val runtime: Runtime,
    private val route: Route,
    private val exchange: HttpExchange,
    private val headers: Headers,
    private val protocol: Protocol,
    private val codec: Codec,
    private val responseContentType: String,
) {
    private val config = runtime.config

    /** Serves the call; returns the error sent, or null on success. */
    suspend fun run(): ConnectException? {
        val base = MetadataBuilder().set("content-type", responseContentType)
        runtime.compressions.advertised?.let { base.set(protocol.acceptEncodingHeader, it) }
        val compressions: Pair<ServerCompressionPool?, ServerCompressionPool?>
        val timeout: Duration?
        try {
            if (protocol == Protocol.GRPC && !exchange.supportsTrailers) {
                // gRPC carries its status in trailers (PROTOCOL-HTTP2.md "Responses").
                throw ConnectException(Code.UNIMPLEMENTED, "gRPC needs HTTP trailers, which this connection cannot send: use HTTP/2, gRPC-Web or Connect")
            }
            timeout = if (protocol == Protocol.CONNECT_STREAM) {
                checkConnectVersion(headers.first("connect-protocol-version"), "1", "Connect-Protocol-Version header", required = false)
                parseConnectTimeout(headers.first("connect-timeout-ms"))
            } else {
                parseGrpcTimeout(headers.first("grpc-timeout"))
            }
            compressions = runtime.compressions.negotiate(headers.first(protocol.encodingHeader), headers.first(protocol.acceptEncodingHeader))
        } catch (e: ConnectException) {
            respondImmediateError(base, e)
            return e
        }
        val (requestCompression, responseCompression) = compressions
        responseCompression?.let { base.set(protocol.encodingHeader, it.name()) }
        val ctx = HandlerContext(route.procedure, headers, exchange.method, runtime.deadline(timeout))
        return serve(base, ctx, requestCompression, responseCompression)
    }

    private suspend fun serve(
        base: MetadataBuilder,
        ctx: HandlerContext,
        requestCompression: ServerCompressionPool?,
        responseCompression: ServerCompressionPool?,
    ): ConnectException? {
        var error: ConnectException? = null
        var responded = false
        try {
            coroutineScope {
                val reader = RequestReader(exchange, config.readMaxBytes)
                // Rendezvous: send() returns once the response writer has taken the
                // frame, so a client that stops reading stalls the handler instead of
                // growing a queue.
                val outbound = Channel<Buffer>(Channel.RENDEZVOUS)
                // Response headers are committed at the first send or when the handler ends.
                val committed = CompletableDeferred<Headers>()
                val commit = {
                    synchronized(committed) {
                        if (!committed.isCompleted) committed.complete(base.addApplication(ctx.responseHeaders).build())
                    }
                }
                val requests = object : RequestStream {
                    override suspend fun receive(): Any? = reader.nextEnvelope(requestCompression)?.let { route.decode(codec, it) }

                    override suspend fun receiveSingle(): Any {
                        val first = reader.nextEnvelope(requestCompression)
                            ?: throw ConnectException(Code.UNIMPLEMENTED, "unary request has zero messages")
                        if (reader.nextEnvelope(requestCompression) != null) {
                            throw ConnectException(Code.UNIMPLEMENTED, "unary request has multiple messages")
                        }
                        return route.decode(codec, first)
                    }
                }
                val responses = object : ResponseStream {
                    override suspend fun send(message: Any) {
                        val payload = route.encode(codec, message)
                        checkSendLimit(config, payload)
                        val frame = if (responseCompression != null && payload.size >= config.compressMinBytes) {
                            envelope(FLAG_COMPRESSED, responseCompression.compress(payload))
                        } else {
                            envelope(0, payload)
                        }
                        if (!committed.isCompleted) commit()
                        outbound.send(frame)
                    }
                }
                val handlerJob = launch {
                    try {
                        error = runtime.runHandler(ctx) { route.call(requests, responses, ctx) }.exceptionOrNull() as ConnectException?
                    } finally {
                        commit()
                        outbound.close()
                    }
                }
                exchange.respondStreaming(200, committed.await()) { sink ->
                    // Each message is flushed as soon as it is written.
                    for (frame in outbound) {
                        sink.write(frame)
                        sink.flush()
                    }
                    handlerJob.join()
                    finish(error, ctx) {
                        sink.write(it)
                        sink.flush()
                    }
                }
                responded = true
                handlerJob.cancel()
            }
        } catch (e: CancellationException) {
            // The whole response, end of stream included, has been handed to
            // the transport, so the call has ended with the code it sent. The
            // end of the response can itself cancel the call: see
            // [HttpExchange], "Cancellation".
            if (!responded) throw e
        }
        return error
    }

    /** Writes the end of the stream; returns HTTP trailers for gRPC. */
    private suspend fun finish(error: ConnectException?, ctx: HandlerContext, write: suspend (Buffer) -> Unit): Headers {
        val trailers = MetadataBuilder()
        return when (protocol) {
            Protocol.CONNECT_STREAM -> {
                trailers.addApplication(ctx.responseTrailers).addApplication(error?.metadata.orEmpty())
                write(envelope(FLAG_CONNECT_END_STREAM, endStreamJson(error, trailers.build())))
                emptyMap()
            }

            Protocol.GRPC_WEB -> {
                grpcStatusFields(error, trailers)
                trailers.addApplication(ctx.responseTrailers).addApplication(error?.metadata.orEmpty())
                write(envelope(FLAG_GRPC_WEB_TRAILER, grpcWebTrailerBlock(trailers.build())))
                emptyMap()
            }

            Protocol.GRPC -> {
                grpcStatusFields(error, trailers)
                trailers.addApplication(ctx.responseTrailers).addApplication(error?.metadata.orEmpty()).build()
            }

            Protocol.CONNECT_UNARY -> error("Connect unary is not enveloped")
        }
    }

    /**
     * Errors found before the handler runs. Connect streams answer 200 with
     * only an end-stream message; gRPC and gRPC-Web answer Trailers-Only,
     * the status in the response headers and no body (PROTOCOL-HTTP2.md
     * "Trailers-Only"; PROTOCOL-WEB.md).
     */
    private suspend fun respondImmediateError(base: MetadataBuilder, error: ConnectException) {
        if (protocol == Protocol.CONNECT_STREAM) {
            val trailers = MetadataBuilder().addApplication(error.metadata).build()
            exchange.respond(200, base.build(), envelope(FLAG_CONNECT_END_STREAM, endStreamJson(error, trailers)).readByteArray())
            return
        }
        grpcStatusFields(error, base)
        base.addApplication(error.metadata)
        exchange.respond(200, base.build(), EMPTY)
    }
}

/**
 * gRPC-Web trailers: an HTTP/1 header block with lower-case names and no
 * terminating blank line (PROTOCOL-WEB.md "Message framing"). Names and
 * values were validated by [MetadataBuilder], so no line can be injected.
 */
private fun grpcWebTrailerBlock(trailers: Headers): Buffer {
    val out = Buffer()
    for ((name, values) in trailers) {
        for (value in values) out.writeUtf8(name).writeUtf8(": ").writeUtf8(value).writeUtf8("\r\n")
    }
    return out
}
