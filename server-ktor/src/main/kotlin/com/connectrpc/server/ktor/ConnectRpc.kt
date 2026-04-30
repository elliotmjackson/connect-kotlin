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

package com.connectrpc.server.ktor

import com.connectrpc.CODEC_NAME_JSON
import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import com.connectrpc.server.protocol.CONNECT_ERROR_CONTENT_TYPE
import com.connectrpc.server.protocol.CONNECT_STREAM_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.CONNECT_STREAM_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.ENVELOPE_FLAG_END_STREAM
import com.connectrpc.server.protocol.ENVELOPE_FLAG_GRPC_WEB_TRAILER
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_BARE
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.connectErrorJsonBody
import com.connectrpc.server.protocol.connectHttpStatus
import com.connectrpc.server.protocol.decodeNextEnvelope
import com.connectrpc.server.protocol.encodeEnvelope
import com.connectrpc.server.protocol.endStreamJsonPayload
import com.connectrpc.server.protocol.grpcWebTrailerPayload
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.contentType
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondBytesWriter
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.toByteArray
import io.ktor.utils.io.writeFully
import okio.Buffer

private const val CONNECT_TIMEOUT_HEADER = "Connect-Timeout-Ms"
private const val GRPC_TIMEOUT_HEADER = "Grpc-Timeout"
private const val CONNECT_CONTENT_ENCODING_HEADER = "Connect-Content-Encoding"
private const val GRPC_ENCODING_HEADER = "Grpc-Encoding"
private const val IDENTITY_ENCODING = "identity"

/**
 * Mounts a Connect/gRPC-Web server into a Ktor [Application]. gRPC over HTTP/2
 * is a follow-up.
 */
fun Application.connectRpc(registry: HandlerRegistry) {
    routing {
        for (procedure in registry.procedures) {
            post("/$procedure") {
                dispatch(call, registry, procedure)
            }
        }
    }
}

/**
 * How a request frame is wrapped on the wire — picks trailer encoding,
 * compression header naming, and whether unary calls are framed in envelopes.
 */
private data class StreamingProtocol(
    val trailerEnvelopeFlag: Int,
    val buildTrailerPayload: (ConnectException?, Map<String, List<String>>) -> ByteArray,
    val compressionHeader: String,
    /** gRPC-Web wraps unary too. Connect doesn't (unary uses application/proto). */
    val framesUnary: Boolean,
)

private val CONNECT_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_END_STREAM,
    buildTrailerPayload = ::endStreamJsonPayload,
    compressionHeader = CONNECT_CONTENT_ENCODING_HEADER,
    framesUnary = false,
)

private val GRPC_WEB_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_GRPC_WEB_TRAILER,
    buildTrailerPayload = ::grpcWebTrailerPayload,
    compressionHeader = GRPC_ENCODING_HEADER,
    framesUnary = true,
)

private suspend fun dispatch(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
) {
    val contentType = call.request.contentType().withoutParameters().toString()
    val (codecName, protocol) = when (contentType) {
        CONNECT_UNARY_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO to null
        CONNECT_UNARY_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to null
        CONNECT_STREAM_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO to CONNECT_STREAMING
        CONNECT_STREAM_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to CONNECT_STREAMING
        GRPC_WEB_CONTENT_TYPE_PROTO, GRPC_WEB_CONTENT_TYPE_BARE -> CODEC_NAME_PROTO to GRPC_WEB_STREAMING
        GRPC_WEB_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to GRPC_WEB_STREAMING
        else -> {
            call.respondBytes(bytes = ByteArray(0), status = HttpStatusCode.UnsupportedMediaType)
            return
        }
    }
    val codec = registry.codec(codecName)
    if (codec == null) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "codec $codecName is not registered"),
        )
        return
    }
    if (protocol == null) {
        handleConnectUnary(call, registry, procedure, codec, contentType)
    } else {
        handleStreaming(call, registry, procedure, codec, contentType, protocol)
    }
}

private suspend fun handleConnectUnary(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    codec: SerializationStrategy,
    requestContentType: String,
) {
    val handler = registry.find(procedure)
    if (handler == null || handler.methodSpec.streamType != StreamType.UNARY) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
        )
        return
    }
    @Suppress("UNCHECKED_CAST")
    val unary = handler as UnaryHandler<Any, Any>

    val ctx = newHandlerContext(call, procedure)

    val contentEncoding = call.request.headers["Content-Encoding"]
    if (contentEncoding != null && contentEncoding != IDENTITY_ENCODING) {
        respondConnectUnaryError(
            call,
            ctx,
            ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $contentEncoding"),
        )
        return
    }

    val requestBytes = call.receiveChannel().toByteArray()
    val request = try {
        codec.codec(unary.methodSpec.requestClass).deserialize(Buffer().write(requestBytes))
    } catch (ex: Exception) {
        respondConnectUnaryError(
            call,
            ctx,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    val response = try {
        unary.handle(request, ctx)
    } catch (ex: ConnectException) {
        respondConnectUnaryError(call, ctx, ex)
        return
    }

    writeUnaryHeadersAndTrailers(call, ctx)
    val responseBytes = codec.codec(unary.methodSpec.responseClass).serialize(response).readByteArray()
    call.respondBytes(
        bytes = responseBytes,
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    )
}

private suspend fun handleStreaming(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
) {
    val handler = registry.find(procedure)
    if (handler == null) {
        respondStreamError(
            call,
            requestContentType,
            ctx = null,
            protocol,
            ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
        )
        return
    }
    val ctx = newHandlerContext(call, procedure)

    val streamEncoding = call.request.headers[protocol.compressionHeader]
    if (streamEncoding != null && streamEncoding != IDENTITY_ENCODING) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $streamEncoding"),
        )
        return
    }

    when (handler.methodSpec.streamType) {
        StreamType.UNARY -> {
            if (!protocol.framesUnary) {
                respondStreamError(
                    call,
                    requestContentType,
                    ctx,
                    protocol,
                    ConnectException(
                        Code.UNIMPLEMENTED,
                        "Connect streaming content-type cannot be used for unary procedures",
                    ),
                )
                return
            }
            @Suppress("UNCHECKED_CAST")
            val uh = handler as UnaryHandler<Any, Any>
            handleUnaryAsStream(call, uh, ctx, codec, requestContentType, protocol)
        }
        StreamType.SERVER -> {
            @Suppress("UNCHECKED_CAST")
            val sh = handler as ServerStreamHandler<Any, Any>
            handleServerStream(call, sh, ctx, codec, requestContentType, protocol)
        }
        StreamType.CLIENT -> {
            @Suppress("UNCHECKED_CAST")
            val ch = handler as ClientStreamHandler<Any, Any>
            handleClientStream(call, ch, ctx, codec, requestContentType, protocol)
        }
        StreamType.BIDI -> {
            @Suppress("UNCHECKED_CAST")
            val bh = handler as BidiStreamHandler<Any, Any>
            handleBidiStream(call, bh, ctx, codec, requestContentType, protocol)
        }
    }
}

private suspend fun handleUnaryAsStream(
    call: ApplicationCall,
    handler: UnaryHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
) {
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol)
        ?: return
    if (error != null) {
        respondStreamError(call, requestContentType, ctx, protocol, error)
        return
    }
    val request = try {
        codec.codec(handler.methodSpec.requestClass).deserialize(Buffer().write(env.payload))
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    val (response, handlerError) = try {
        handler.handle(request, ctx) to null
    } catch (ex: ConnectException) {
        null to ex
    }

    writeStreamResponseHeaders(call, ctx)
    call.respondBytesWriter(
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    ) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass)
                .serialize(response)
                .readByteArray()
            writeFully(encodeEnvelope(0, bytes))
        }
        writeTrailerEnvelope(this, protocol, handlerError, ctx)
    }
}

private suspend fun handleServerStream(
    call: ApplicationCall,
    handler: ServerStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
) {
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol)
        ?: return
    if (error != null) {
        respondStreamError(call, requestContentType, ctx, protocol, error)
        return
    }
    val request = try {
        codec.codec(handler.methodSpec.requestClass).deserialize(Buffer().write(env.payload))
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    val outbound = mutableListOf<ByteArray>()
    val outStream = object : ServerMessageStream<Any> {
        override suspend fun send(message: Any) {
            outbound += codec.codec(handler.methodSpec.responseClass)
                .serialize(message)
                .readByteArray()
        }
    }

    val handlerError = try {
        handler.handle(request, ctx, outStream)
        null
    } catch (ex: ConnectException) {
        ex
    }

    writeStreamResponseHeaders(call, ctx)
    call.respondBytesWriter(
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    ) {
        for (payload in outbound) {
            writeFully(encodeEnvelope(0, payload))
        }
        writeTrailerEnvelope(this, protocol, handlerError, ctx)
    }
}

private suspend fun handleClientStream(
    call: ApplicationCall,
    handler: ClientStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
) {
    val messages = readAllRequestEnvelopes(call, handler.methodSpec.requestClass, codec, ctx, requestContentType, protocol)
        ?: return

    val inStream = BufferedClientMessageStream(messages, ctx.requestHeaders)

    val (response, handlerError) = try {
        handler.handle(inStream, ctx) to null
    } catch (ex: ConnectException) {
        null to ex
    }

    writeStreamResponseHeaders(call, ctx)
    call.respondBytesWriter(
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    ) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            writeFully(encodeEnvelope(0, bytes))
        }
        writeTrailerEnvelope(this, protocol, handlerError, ctx)
    }
}

private suspend fun handleBidiStream(
    call: ApplicationCall,
    handler: BidiStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
) {
    val messages = readAllRequestEnvelopes(call, handler.methodSpec.requestClass, codec, ctx, requestContentType, protocol)
        ?: return

    val outbound = mutableListOf<ByteArray>()
    val bidi = BufferedBidiStream<Any, Any>(messages, ctx.requestHeaders) { message ->
        outbound += codec.codec(handler.methodSpec.responseClass)
            .serialize(message)
            .readByteArray()
    }

    val handlerError = try {
        handler.handle(bidi, ctx)
        null
    } catch (ex: ConnectException) {
        ex
    }

    writeStreamResponseHeaders(call, ctx)
    call.respondBytesWriter(
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    ) {
        for (payload in outbound) {
            writeFully(encodeEnvelope(0, payload))
        }
        writeTrailerEnvelope(this, protocol, handlerError, ctx)
    }
}

/**
 * Reads exactly one request envelope. Returns (envelope, null) on success,
 * (dummy, error) on protocol violation that the caller should respond with,
 * or null if the response was already sent (e.g., compression rejected).
 */
private suspend fun readSingleStreamEnvelope(
    call: ApplicationCall,
    @Suppress("UNUSED_PARAMETER") requestContentType: String,
    @Suppress("UNUSED_PARAMETER") ctx: HandlerContext,
    @Suppress("UNUSED_PARAMETER") protocol: StreamingProtocol,
): Pair<com.connectrpc.server.protocol.ConnectEnvelope, ConnectException?>? {
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val envelope = try {
        decodeNextEnvelope(buffer)
            ?: return DUMMY_ENVELOPE to ConnectException(
                Code.UNIMPLEMENTED,
                "expects exactly one request envelope, got 0",
            )
    } catch (ex: ConnectException) {
        return DUMMY_ENVELOPE to ex
    }
    if (envelope.isCompressed) {
        return DUMMY_ENVELOPE to ConnectException(
            Code.INTERNAL_ERROR,
            "request envelope marked compressed but no compression negotiated",
        )
    }
    if (decodeNextEnvelope(buffer) != null) {
        return DUMMY_ENVELOPE to ConnectException(
            Code.UNIMPLEMENTED,
            "expects exactly one request envelope, got more",
        )
    }
    return envelope to null
}

private val DUMMY_ENVELOPE = com.connectrpc.server.protocol.ConnectEnvelope(0, ByteArray(0))

private suspend fun readAllRequestEnvelopes(
    call: ApplicationCall,
    requestClass: kotlin.reflect.KClass<Any>,
    codec: SerializationStrategy,
    ctx: HandlerContext,
    requestContentType: String,
    protocol: StreamingProtocol,
): List<Any>? {
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val messages = mutableListOf<Any>()
    val msgCodec = codec.codec(requestClass)
    try {
        while (true) {
            val env = decodeNextEnvelope(buffer) ?: break
            if (env.isCompressed) {
                respondStreamError(
                    call,
                    requestContentType,
                    ctx,
                    protocol,
                    ConnectException(
                        Code.INTERNAL_ERROR,
                        "request envelope marked compressed but no compression negotiated",
                    ),
                )
                return null
            }
            messages += msgCodec.deserialize(Buffer().write(env.payload))
        }
    } catch (ex: ConnectException) {
        respondStreamError(call, requestContentType, ctx, protocol, ex)
        return null
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return null
    }
    return messages
}

private suspend fun writeTrailerEnvelope(
    writer: io.ktor.utils.io.ByteWriteChannel,
    protocol: StreamingProtocol,
    error: ConnectException?,
    ctx: HandlerContext,
) {
    val payload = protocol.buildTrailerPayload(
        error,
        ctx.responseTrailers.mapValues { it.value.toList() },
    )
    writer.writeFully(encodeEnvelope(protocol.trailerEnvelopeFlag, payload))
}

private class BufferedClientMessageStream<Req : Any>(
    messages: List<Req>,
    override val headers: Headers,
) : ClientMessageStream<Req> {
    private val iterator = messages.iterator()
    override suspend fun receive(): Req? = if (iterator.hasNext()) iterator.next() else null
}

private class BufferedBidiStream<Req : Any, Res : Any>(
    messages: List<Req>,
    override val headers: Headers,
    private val onSend: suspend (Res) -> Unit,
) : BidiStream<Req, Res> {
    private val iterator = messages.iterator()
    override suspend fun receive(): Req? = if (iterator.hasNext()) iterator.next() else null
    override suspend fun send(message: Res) = onSend(message)
}

private fun newHandlerContext(call: ApplicationCall, procedure: String) =
    HandlerContext(
        procedure = procedure,
        requestHeaders = call.request.headers.toMap(),
        httpMethod = call.request.httpMethod.value,
        timeoutMs = call.request.headers[CONNECT_TIMEOUT_HEADER]?.toLongOrNull()
            ?: call.request.headers[GRPC_TIMEOUT_HEADER]?.let(::parseGrpcTimeoutMs),
    )

/**
 * gRPC timeouts are encoded as `<digits><unit>` where unit is one of
 * H (hours), M (minutes), S (seconds), m (milliseconds), u (microseconds),
 * or n (nanoseconds). Returns the value in milliseconds, rounding sub-ms
 * units up.
 */
private fun parseGrpcTimeoutMs(value: String): Long? {
    if (value.length < 2) return null
    val unit = value.last()
    val digits = value.dropLast(1).toLongOrNull() ?: return null
    return when (unit) {
        'H' -> digits * 3_600_000L
        'M' -> digits * 60_000L
        'S' -> digits * 1_000L
        'm' -> digits
        'u' -> (digits + 999L) / 1000L
        'n' -> (digits + 999_999L) / 1_000_000L
        else -> null
    }
}

private suspend fun respondConnectUnaryError(
    call: ApplicationCall,
    ctx: HandlerContext?,
    exception: ConnectException,
) {
    if (ctx != null) writeUnaryHeadersAndTrailers(call, ctx)
    call.respondBytes(
        bytes = connectErrorJsonBody(exception),
        contentType = ContentType.parse(CONNECT_ERROR_CONTENT_TYPE),
        status = HttpStatusCode.fromValue(exception.code.connectHttpStatus()),
    )
}

private suspend fun respondStreamError(
    call: ApplicationCall,
    requestContentType: String,
    ctx: HandlerContext?,
    protocol: StreamingProtocol,
    exception: ConnectException,
) {
    if (ctx != null) writeStreamResponseHeaders(call, ctx)
    val trailers = ctx?.responseTrailers?.mapValues { it.value.toList() } ?: emptyMap()
    call.respondBytesWriter(
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    ) {
        writeFully(
            encodeEnvelope(
                protocol.trailerEnvelopeFlag,
                protocol.buildTrailerPayload(exception, trailers),
            ),
        )
    }
}

// Connect unary protocol: response headers go straight into HTTP headers;
// trailers go into HTTP headers prefixed with "trailer-".
private fun writeUnaryHeadersAndTrailers(call: ApplicationCall, ctx: HandlerContext) {
    for ((name, values) in ctx.responseHeaders) {
        for (v in values) call.response.headers.append(name, v, safeOnly = false)
    }
    for ((name, values) in ctx.responseTrailers) {
        for (v in values) call.response.headers.append("trailer-$name", v, safeOnly = false)
    }
}

// Streaming: response headers go into HTTP headers; trailers go into the
// trailer envelope, written by the caller.
private fun writeStreamResponseHeaders(call: ApplicationCall, ctx: HandlerContext) {
    for ((name, values) in ctx.responseHeaders) {
        for (v in values) call.response.headers.append(name, v, safeOnly = false)
    }
}

private fun io.ktor.http.Headers.toMap(): Map<String, List<String>> =
    entries().associate { it.key to it.value }
