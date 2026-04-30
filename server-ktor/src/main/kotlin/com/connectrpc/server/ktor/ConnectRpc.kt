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
import com.connectrpc.Headers as ConnectHeaders
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.compression.CompressionPool
import com.connectrpc.compression.GzipCompressionPool
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
import com.connectrpc.server.protocol.ENVELOPE_FLAG_COMPRESSED
import com.connectrpc.server.protocol.ENVELOPE_FLAG_END_STREAM
import com.connectrpc.server.protocol.ENVELOPE_FLAG_GRPC_WEB_TRAILER
import com.connectrpc.server.protocol.GRPC_CONTENT_TYPE_BARE
import com.connectrpc.server.protocol.GRPC_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.GRPC_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_BARE
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.GRPC_WEB_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.connectErrorJsonBody
import com.connectrpc.server.protocol.connectHttpStatus
import com.connectrpc.server.protocol.decodeNextEnvelope
import com.connectrpc.server.protocol.encodeEnvelope
import com.connectrpc.server.protocol.endStreamJsonPayload
import com.connectrpc.server.protocol.grpcTrailerPairs
import com.connectrpc.server.protocol.grpcWebTrailerPayload
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.contentType
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.toByteArray
import io.ktor.utils.io.writeFully
import okio.Buffer

private const val CONNECT_TIMEOUT_HEADER = "Connect-Timeout-Ms"
private const val GRPC_TIMEOUT_HEADER = "Grpc-Timeout"
private const val CONNECT_CONTENT_ENCODING_HEADER = "Connect-Content-Encoding"
private const val GRPC_ENCODING_HEADER = "Grpc-Encoding"
private const val IDENTITY_ENCODING = "identity"

/** Compression algorithms recognized server-side. Always includes identity. */
private val COMPRESSION_POOLS: Map<String, CompressionPool> = mapOf(
    GzipCompressionPool.name() to GzipCompressionPool,
)

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
 *
 * If [usesHttpTrailers] is true, the protocol's trailers go into real HTTP
 * trailing headers (gRPC over HTTP/2). Otherwise they are emitted as a
 * final envelope on the body (Connect / gRPC-Web).
 */
private data class StreamingProtocol(
    val trailerEnvelopeFlag: Int,
    val buildTrailerPayload: (ConnectException?, Map<String, List<String>>) -> ByteArray,
    val buildHttpTrailers: (ConnectException?, Map<String, List<String>>) -> List<Pair<String, String>>,
    val compressionHeader: String,
    /** gRPC and gRPC-Web frame unary too; Connect doesn't (unary uses application/proto). */
    val framesUnary: Boolean,
    val usesHttpTrailers: Boolean,
)

private val CONNECT_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_END_STREAM,
    buildTrailerPayload = ::endStreamJsonPayload,
    buildHttpTrailers = { _, _ -> emptyList() },
    compressionHeader = CONNECT_CONTENT_ENCODING_HEADER,
    framesUnary = false,
    usesHttpTrailers = false,
)

private val GRPC_WEB_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_GRPC_WEB_TRAILER,
    buildTrailerPayload = ::grpcWebTrailerPayload,
    buildHttpTrailers = { _, _ -> emptyList() },
    compressionHeader = GRPC_ENCODING_HEADER,
    framesUnary = true,
    usesHttpTrailers = false,
)

private val GRPC_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = -1,
    buildTrailerPayload = { _, _ -> ByteArray(0) },
    buildHttpTrailers = ::grpcTrailerPairs,
    compressionHeader = GRPC_ENCODING_HEADER,
    framesUnary = true,
    usesHttpTrailers = true,
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
        GRPC_CONTENT_TYPE_PROTO, GRPC_CONTENT_TYPE_BARE -> CODEC_NAME_PROTO to GRPC_STREAMING
        GRPC_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to GRPC_STREAMING
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
    val requestPool: CompressionPool? = when {
        contentEncoding == null || contentEncoding == IDENTITY_ENCODING -> null
        else -> COMPRESSION_POOLS[contentEncoding] ?: run {
            respondConnectUnaryError(
                call,
                ctx,
                ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $contentEncoding"),
            )
            return
        }
    }

    val rawBytes = call.receiveChannel().toByteArray()
    val requestBytes = if (requestPool != null) {
        try {
            requestPool.decompress(Buffer().write(rawBytes)).readByteArray()
        } catch (ex: Exception) {
            respondConnectUnaryError(
                call,
                ctx,
                ConnectException(Code.INVALID_ARGUMENT, "could not decompress request: ${ex.message}"),
            )
            return
        }
    } else {
        rawBytes
    }
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
    val streamPool: CompressionPool? = when {
        streamEncoding == null || streamEncoding == IDENTITY_ENCODING -> null
        else -> COMPRESSION_POOLS[streamEncoding] ?: run {
            respondStreamError(
                call,
                requestContentType,
                ctx,
                protocol,
                ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $streamEncoding"),
            )
            return
        }
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
            handleUnaryAsStream(call, uh, ctx, codec, requestContentType, protocol, streamPool)
        }
        StreamType.SERVER -> {
            @Suppress("UNCHECKED_CAST")
            val sh = handler as ServerStreamHandler<Any, Any>
            handleServerStream(call, sh, ctx, codec, requestContentType, protocol, streamPool)
        }
        StreamType.CLIENT -> {
            @Suppress("UNCHECKED_CAST")
            val ch = handler as ClientStreamHandler<Any, Any>
            handleClientStream(call, ch, ctx, codec, requestContentType, protocol, streamPool)
        }
        StreamType.BIDI -> {
            @Suppress("UNCHECKED_CAST")
            val bh = handler as BidiStreamHandler<Any, Any>
            handleBidiStream(call, bh, ctx, codec, requestContentType, protocol, streamPool)
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
    pool: CompressionPool?,
) {
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol, pool)
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
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass)
                .serialize(response)
                .readByteArray()
            writeFully(encodeEnvelope(0, bytes))
        }
    }
}

private suspend fun handleServerStream(
    call: ApplicationCall,
    handler: ServerStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
    pool: CompressionPool?,
) {
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol, pool)
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
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        for (payload in outbound) {
            writeFully(encodeEnvelope(0, payload))
        }
    }
}

private suspend fun handleClientStream(
    call: ApplicationCall,
    handler: ClientStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
    pool: CompressionPool?,
) {
    val messages = readAllRequestEnvelopes(call, handler.methodSpec.requestClass, codec, ctx, requestContentType, protocol, pool)
        ?: return

    val inStream = BufferedClientMessageStream(messages, ctx.requestHeaders)

    val (response, handlerError) = try {
        handler.handle(inStream, ctx) to null
    } catch (ex: ConnectException) {
        null to ex
    }

    writeStreamResponseHeaders(call, ctx)
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            writeFully(encodeEnvelope(0, bytes))
        }
    }
}

private suspend fun handleBidiStream(
    call: ApplicationCall,
    handler: BidiStreamHandler<Any, Any>,
    ctx: HandlerContext,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
    pool: CompressionPool?,
) {
    val messages = readAllRequestEnvelopes(call, handler.methodSpec.requestClass, codec, ctx, requestContentType, protocol, pool)
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
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        for (payload in outbound) {
            writeFully(encodeEnvelope(0, payload))
        }
    }
}

/**
 * Writes the streaming response: caller-supplied body messages first, then —
 * for envelope-trailer protocols — a trailer envelope, followed (if the
 * protocol uses real HTTP trailers) by [StreamingProtocol.buildHttpTrailers]
 * being surfaced through the engine.
 */
private suspend fun respondStreaming(
    call: ApplicationCall,
    requestContentType: String,
    protocol: StreamingProtocol,
    ctx: HandlerContext,
    handlerError: ConnectException?,
    writeMessages: suspend ByteWriteChannel.() -> Unit,
) {
    val ct = ContentType.parse(requestContentType)
    val trailers: Headers = if (protocol.usesHttpTrailers) {
        headersFromPairs(
            protocol.buildHttpTrailers(handlerError, ctx.responseTrailers.mapValues { it.value.toList() }),
        )
    } else {
        Headers.Empty
    }
    call.respond(
        FramedStreamingContent(
            ct = ct,
            getTrailers = { trailers },
            writeBody = { channel ->
                channel.writeMessages()
                if (!protocol.usesHttpTrailers) {
                    channel.writeFully(
                        encodeEnvelope(
                            protocol.trailerEnvelopeFlag,
                            protocol.buildTrailerPayload(
                                handlerError,
                                ctx.responseTrailers.mapValues { it.value.toList() },
                            ),
                        ),
                    )
                }
            },
        ),
    )
}

private class FramedStreamingContent(
    private val ct: ContentType,
    private val getTrailers: () -> Headers,
    private val writeBody: suspend (ByteWriteChannel) -> Unit,
) : OutgoingContent.WriteChannelContent() {
    override val contentType get() = ct
    override val status get() = HttpStatusCode.OK
    override fun trailers(): Headers = getTrailers()
    override suspend fun writeTo(channel: ByteWriteChannel) {
        writeBody(channel)
    }
}

private fun headersFromPairs(pairs: List<Pair<String, String>>): Headers {
    if (pairs.isEmpty()) return Headers.Empty
    val builder = io.ktor.http.HeadersBuilder()
    for ((name, value) in pairs) builder.append(name, value)
    return builder.build()
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
    pool: CompressionPool?,
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
    if (decodeNextEnvelope(buffer) != null) {
        return DUMMY_ENVELOPE to ConnectException(
            Code.UNIMPLEMENTED,
            "expects exactly one request envelope, got more",
        )
    }
    val decoded = decompressEnvelopeIfNeeded(envelope, pool)
        ?: return DUMMY_ENVELOPE to ConnectException(
            Code.INTERNAL_ERROR,
            "request envelope marked compressed but no compression negotiated",
        )
    return decoded to null
}

private fun decompressEnvelopeIfNeeded(
    envelope: com.connectrpc.server.protocol.ConnectEnvelope,
    pool: CompressionPool?,
): com.connectrpc.server.protocol.ConnectEnvelope? {
    if (!envelope.isCompressed) return envelope
    if (pool == null) return null
    val decompressed = pool.decompress(Buffer().write(envelope.payload)).readByteArray()
    return com.connectrpc.server.protocol.ConnectEnvelope(
        flags = envelope.flags and ENVELOPE_FLAG_COMPRESSED.inv(),
        payload = decompressed,
    )
}

private val DUMMY_ENVELOPE = com.connectrpc.server.protocol.ConnectEnvelope(0, ByteArray(0))

private suspend fun readAllRequestEnvelopes(
    call: ApplicationCall,
    requestClass: kotlin.reflect.KClass<Any>,
    codec: SerializationStrategy,
    ctx: HandlerContext,
    requestContentType: String,
    protocol: StreamingProtocol,
    pool: CompressionPool?,
): List<Any>? {
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val messages = mutableListOf<Any>()
    val msgCodec = codec.codec(requestClass)
    try {
        while (true) {
            val env = decodeNextEnvelope(buffer) ?: break
            val decoded = decompressEnvelopeIfNeeded(env, pool)
                ?: run {
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
            messages += msgCodec.deserialize(Buffer().write(decoded.payload))
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

private class BufferedClientMessageStream<Req : Any>(
    messages: List<Req>,
    override val headers: ConnectHeaders,
) : ClientMessageStream<Req> {
    private val iterator = messages.iterator()
    override suspend fun receive(): Req? = if (iterator.hasNext()) iterator.next() else null
}

private class BufferedBidiStream<Req : Any, Res : Any>(
    messages: List<Req>,
    override val headers: ConnectHeaders,
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
    val userTrailers = ctx?.responseTrailers?.mapValues { it.value.toList() } ?: emptyMap()
    val trailers: Headers = if (protocol.usesHttpTrailers) {
        headersFromPairs(protocol.buildHttpTrailers(exception, userTrailers))
    } else {
        Headers.Empty
    }
    call.respond(
        FramedStreamingContent(
            ct = ContentType.parse(requestContentType),
            getTrailers = { trailers },
            writeBody = { channel ->
                if (!protocol.usesHttpTrailers) {
                    channel.writeFully(
                        encodeEnvelope(
                            protocol.trailerEnvelopeFlag,
                            protocol.buildTrailerPayload(exception, userTrailers),
                        ),
                    )
                }
            },
        ),
    )
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
