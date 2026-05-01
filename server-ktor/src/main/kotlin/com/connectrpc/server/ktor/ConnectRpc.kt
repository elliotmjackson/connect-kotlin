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
import com.connectrpc.Idempotency
import com.connectrpc.compression.CompressionPool
import com.connectrpc.compression.DeflateCompressionPool
import com.connectrpc.compression.GzipCompressionPool
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerInterceptor
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
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.server.request.contentType
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytes
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.toByteArray
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withTimeout
import okio.Buffer

private const val CONNECT_TIMEOUT_HEADER = "Connect-Timeout-Ms"
private const val GRPC_TIMEOUT_HEADER = "Grpc-Timeout"
private const val CONNECT_CONTENT_ENCODING_HEADER = "Connect-Content-Encoding"
private const val GRPC_ENCODING_HEADER = "Grpc-Encoding"
private const val IDENTITY_ENCODING = "identity"
private const val CONNECT_PROTOCOL_VERSION_HEADER = "Connect-Protocol-Version"
private const val CONNECT_PROTOCOL_VERSION_VALUE = "1"

/** Compression algorithms recognized server-side. Always includes identity. */
private val COMPRESSION_POOLS: Map<String, CompressionPool> = mapOf(
    GzipCompressionPool.name() to GzipCompressionPool,
    DeflateCompressionPool.name() to DeflateCompressionPool,
)

/**
 * Mounts a Connect/gRPC-Web server into a Ktor [Application]. gRPC over HTTP/2
 * is a follow-up.
 */
fun Application.connectRpc(
    registry: HandlerRegistry,
    /**
     * Maximum size in bytes of any single request message after decompression.
     * Zero means unlimited. Mirrors `ServerCompatRequest.message_receive_limit`
     * from the conformance suite.
     */
    maxReceiveMessageSize: Int = 0,
    /**
     * If true, Connect protocol POSTs missing `Connect-Protocol-Version: 1`
     * are rejected with INVALID_ARGUMENT. Off by default per the Connect spec
     * (header is recommended but not required for backwards compatibility).
     * Mirrors connect-go's `RequireConnectProtocolHeader` handler option.
     */
    requireConnectProtocolHeader: Boolean = false,
    /**
     * Responses whose serialized message size meets or exceeds this threshold
     * are eligible for compression when the client advertises a supported
     * encoding via Accept-Encoding (or the protocol's streaming equivalent).
     * Set to [Int.MAX_VALUE] to disable outbound compression.
     */
    compressMinBytes: Int = 1024,
) {
    val opts = ConnectRpcOptions(
        maxReceiveMessageSize = maxReceiveMessageSize,
        requireConnectProtocolHeader = requireConnectProtocolHeader,
        compressMinBytes = compressMinBytes,
    )
    routing {
        for (procedure in registry.procedures) {
            post("/$procedure") {
                withCancellationOnDisconnect(call) {
                    dispatch(call, registry, procedure, opts)
                }
            }
            val handler = registry.find(procedure)
            if (handler != null && handler.methodSpec.idempotency == Idempotency.NO_SIDE_EFFECTS) {
                get("/$procedure") {
                    withCancellationOnDisconnect(call) {
                        dispatchConnectGet(call, registry, procedure, opts)
                    }
                }
            }
        }
    }
}

/**
 * Wires Netty-level connection close to coroutine cancellation. When the
 * underlying channel's close future fires, this routine's Job is cancelled,
 * propagating CancellationException to any in-flight handler suspension.
 *
 * No-op for non-Netty engines (the Ktor call won't be a [NettyApplicationCall]).
 */
private suspend inline fun withCancellationOnDisconnect(
    call: ApplicationCall,
    block: () -> Unit,
) {
    val channel = call.nettyChannelOrNull()
    if (channel == null) {
        block()
        return
    }
    val job = currentCoroutineContext()[Job]!!
    val listener = io.netty.util.concurrent.GenericFutureListener<io.netty.channel.ChannelFuture> {
        job.cancel(kotlinx.coroutines.CancellationException("client disconnected"))
    }
    channel.closeFuture().addListener(listener)
    try {
        block()
    } finally {
        channel.closeFuture().removeListener(listener)
    }
}

/**
 * Unwraps the Ktor routing call layers to find the engine's
 * [NettyApplicationCall] and return its underlying Netty channel.
 * Returns null when the engine isn't Netty (or when Ktor's wrapping
 * layout has changed beneath us).
 */
private fun ApplicationCall.nettyChannelOrNull(): io.netty.channel.Channel? {
    val direct = (this as? NettyApplicationCall)?.context?.channel()
    if (direct != null) return direct
    // Within a routing block the call is a RoutingCall wrapping a
    // RoutingPipelineCall wrapping the engine call. Reach through both.
    val routing = (this as? io.ktor.server.routing.RoutingCall) ?: return null
    val engineCall = routing.pipelineCall.engineCall
    return (engineCall as? NettyApplicationCall)?.context?.channel()
}

private data class ConnectRpcOptions(
    val maxReceiveMessageSize: Int,
    val requireConnectProtocolHeader: Boolean,
    val compressMinBytes: Int,
)

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
    /** Request-side compression header — also names the response-side header by convention. */
    val compressionHeader: String,
    /** Header the client uses to advertise outbound encodings it can decompress. */
    val acceptEncodingHeader: String,
    /** gRPC and gRPC-Web frame unary too; Connect doesn't (unary uses application/proto). */
    val framesUnary: Boolean,
    val usesHttpTrailers: Boolean,
)

private val CONNECT_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_END_STREAM,
    buildTrailerPayload = ::endStreamJsonPayload,
    buildHttpTrailers = { _, _ -> emptyList() },
    compressionHeader = CONNECT_CONTENT_ENCODING_HEADER,
    acceptEncodingHeader = "Connect-Accept-Encoding",
    framesUnary = false,
    usesHttpTrailers = false,
)

private val GRPC_WEB_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = ENVELOPE_FLAG_GRPC_WEB_TRAILER,
    buildTrailerPayload = ::grpcWebTrailerPayload,
    buildHttpTrailers = { _, _ -> emptyList() },
    compressionHeader = GRPC_ENCODING_HEADER,
    acceptEncodingHeader = "Grpc-Accept-Encoding",
    framesUnary = true,
    usesHttpTrailers = false,
)

private val GRPC_STREAMING = StreamingProtocol(
    trailerEnvelopeFlag = -1,
    buildTrailerPayload = { _, _ -> ByteArray(0) },
    buildHttpTrailers = ::grpcTrailerPairs,
    compressionHeader = GRPC_ENCODING_HEADER,
    acceptEncodingHeader = "Grpc-Accept-Encoding",
    framesUnary = true,
    usesHttpTrailers = true,
)

private suspend fun dispatch(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    opts: ConnectRpcOptions,
) {
    val maxReceiveMessageSize = opts.maxReceiveMessageSize
    val requireConnectProtocolHeader = opts.requireConnectProtocolHeader
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

    val isConnectProtocol = protocol == null || protocol === CONNECT_STREAMING
    val isGrpcProtocol = protocol === GRPC_STREAMING

    if (isConnectProtocol && requireConnectProtocolHeader) {
        // Connect spec: Connect-Protocol-Version is recommended but not
        // required. Servers may opt in to enforcement via the
        // requireConnectProtocolHeader flag — useful when the server only
        // talks to known-recent clients. (GET requests use ?connect=v1 and
        // are routed through dispatchConnectGet.)
        val version = call.request.headers[CONNECT_PROTOCOL_VERSION_HEADER]
        if (version != CONNECT_PROTOCOL_VERSION_VALUE) {
            val msg = if (version == null) {
                "missing required header: $CONNECT_PROTOCOL_VERSION_HEADER"
            } else {
                "unsupported $CONNECT_PROTOCOL_VERSION_HEADER: $version"
            }
            if (protocol == null) {
                respondConnectUnaryError(call, ctx = null, ConnectException(Code.INVALID_ARGUMENT, msg))
            } else {
                respondStreamError(call, contentType, ctx = null, protocol, ConnectException(Code.INVALID_ARGUMENT, msg))
            }
            return
        }
    }
    if (isGrpcProtocol) {
        // gRPC requires TE: trailers per the spec — reject otherwise so
        // misconfigured clients fail fast.
        val teValues = call.request.headers.getAll("TE").orEmpty()
        val hasTrailers = teValues.any { it.split(',').any { v -> v.trim().equals("trailers", ignoreCase = true) } }
        if (!hasTrailers) {
            respondStreamError(
                call,
                contentType,
                ctx = null,
                protocol!!,
                ConnectException(Code.INTERNAL_ERROR, "missing required header: TE: trailers"),
            )
            return
        }
    }

    if (protocol == null) {
        handleConnectUnary(call, registry, procedure, codec, contentType, opts)
    } else {
        handleStreaming(call, registry, procedure, codec, contentType, protocol, opts)
    }
}

private suspend fun handleConnectUnary(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    codec: SerializationStrategy,
    requestContentType: String,
    opts: ConnectRpcOptions,
) {
    val maxReceiveMessageSize = opts.maxReceiveMessageSize
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
    val unary = (handler as UnaryHandler<Any, Any>).wrapUnary(registry.interceptors + registry.interceptorsFor(procedure))

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
    if (maxReceiveMessageSize > 0 && requestBytes.size > maxReceiveMessageSize) {
        respondConnectUnaryError(
            call,
            ctx,
            ConnectException(
                Code.RESOURCE_EXHAUSTED,
                "message size ${requestBytes.size} exceeds limit of $maxReceiveMessageSize",
            ),
        )
        return
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
        invokeWithTimeout(ctx.timeoutMs) { unary.handle(request, ctx) }
    } catch (ex: TimeoutCancellationException) {
        respondConnectUnaryError(call, ctx, ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
        return
    } catch (ex: ConnectException) {
        respondConnectUnaryError(call, ctx, ex)
        return
    } catch (ex: kotlinx.coroutines.CancellationException) {
        throw ex
    } catch (ex: Throwable) {
        respondConnectUnaryError(call, ctx, ex.toUnknownConnectException())
        return
    }

    writeUnaryHeadersAndTrailers(call, ctx)
    val rawResponseBytes = codec.codec(unary.methodSpec.responseClass).serialize(response).readByteArray()
    val (finalBytes, encoding) = maybeCompressOutbound(
        rawResponseBytes,
        acceptEncoding = call.request.headers["Accept-Encoding"],
        compressMinBytes = opts.compressMinBytes,
    )
    if (encoding != null) {
        call.response.headers.append("Content-Encoding", encoding, safeOnly = false)
    }
    call.respondBytes(
        bytes = finalBytes,
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    )
}

/**
 * Picks an outbound encoding from [acceptEncoding] (a comma-separated list)
 * and compresses [bytes] if the chosen pool reduces size meaningfully and
 * the input meets the [compressMinBytes] threshold. Returns the final body
 * bytes plus the encoding name (or null for identity).
 */
private fun maybeCompressOutbound(
    bytes: ByteArray,
    acceptEncoding: String?,
    compressMinBytes: Int,
): Pair<ByteArray, String?> {
    if (acceptEncoding == null || bytes.size < compressMinBytes) return bytes to null
    val pool = pickPool(acceptEncoding) ?: return bytes to null
    val compressed = pool.compress(Buffer().write(bytes)).readByteArray()
    return compressed to pool.name()
}

private fun pickPool(acceptEncoding: String?): CompressionPool? {
    if (acceptEncoding == null) return null
    val accepted = acceptEncoding.split(',')
        .map { it.substringBefore(';').trim().lowercase() }
        .filter { it.isNotEmpty() }
    return accepted.firstNotNullOfOrNull { COMPRESSION_POOLS[it] }
}

/**
 * Encodes a streaming envelope, optionally gzipping the payload when [pool]
 * is non-null and the payload meets the [compressMinBytes] threshold. The
 * compressed flag (0x01) is OR'd into the envelope flags when applicable.
 */
private fun encodeOutboundEnvelope(
    flags: Int,
    payload: ByteArray,
    pool: CompressionPool?,
    compressMinBytes: Int,
): ByteArray {
    if (pool == null || payload.size < compressMinBytes) {
        return encodeEnvelope(flags, payload)
    }
    val compressed = pool.compress(Buffer().write(payload)).readByteArray()
    return encodeEnvelope(flags or ENVELOPE_FLAG_COMPRESSED, compressed)
}

private suspend fun handleStreaming(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    codec: SerializationStrategy,
    requestContentType: String,
    protocol: StreamingProtocol,
    opts: ConnectRpcOptions,
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
            val combined = registry.interceptors + registry.interceptorsFor(procedure)
            @Suppress("UNCHECKED_CAST")
            val uh = (handler as UnaryHandler<Any, Any>).wrapUnary(combined)
            handleUnaryAsStream(call, uh, ctx, codec, requestContentType, protocol, streamPool, opts)
        }
        StreamType.SERVER -> {
            val combined = registry.interceptors + registry.interceptorsFor(procedure)
            @Suppress("UNCHECKED_CAST")
            val sh = (handler as ServerStreamHandler<Any, Any>).wrapServerStream(combined)
            handleServerStream(call, sh, ctx, codec, requestContentType, protocol, streamPool, opts)
        }
        StreamType.CLIENT -> {
            val combined = registry.interceptors + registry.interceptorsFor(procedure)
            @Suppress("UNCHECKED_CAST")
            val ch = (handler as ClientStreamHandler<Any, Any>).wrapClientStream(combined)
            handleClientStream(call, ch, ctx, codec, requestContentType, protocol, streamPool, opts)
        }
        StreamType.BIDI -> {
            val combined = registry.interceptors + registry.interceptorsFor(procedure)
            @Suppress("UNCHECKED_CAST")
            val bh = (handler as BidiStreamHandler<Any, Any>).wrapBidi(combined)
            handleBidiStream(call, bh, ctx, codec, requestContentType, protocol, streamPool, opts)
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
    opts: ConnectRpcOptions,
) {
    val maxReceiveMessageSize = opts.maxReceiveMessageSize
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol, pool)
        ?: return
    if (error != null) {
        respondStreamError(call, requestContentType, ctx, protocol, error)
        return
    }
    if (maxReceiveMessageSize > 0 && env.payload.size > maxReceiveMessageSize) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(
                Code.RESOURCE_EXHAUSTED,
                "message size ${env.payload.size} exceeds limit of $maxReceiveMessageSize",
            ),
        )
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
        invokeWithTimeout(ctx.timeoutMs) { handler.handle(request, ctx) } to null
    } catch (ex: TimeoutCancellationException) {
        null to ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded")
    } catch (ex: ConnectException) {
        null to ex
    } catch (ex: kotlinx.coroutines.CancellationException) {
        throw ex
    } catch (ex: Throwable) {
        null to ex.toUnknownConnectException()
    }

    val outPool = pickPool(call.request.headers[protocol.acceptEncodingHeader])
    writeStreamResponseHeaders(call, ctx)
    if (outPool != null) {
        call.response.headers.append(protocol.compressionHeader, outPool.name(), safeOnly = false)
    }
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass)
                .serialize(response)
                .readByteArray()
            writeFully(encodeOutboundEnvelope(0, bytes, outPool, opts.compressMinBytes))
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
    opts: ConnectRpcOptions,
) {
    val maxReceiveMessageSize = opts.maxReceiveMessageSize
    val (env, error) = readSingleStreamEnvelope(call, requestContentType, ctx, protocol, pool)
        ?: return
    if (error != null) {
        respondStreamError(call, requestContentType, ctx, protocol, error)
        return
    }
    if (maxReceiveMessageSize > 0 && env.payload.size > maxReceiveMessageSize) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            protocol,
            ConnectException(
                Code.RESOURCE_EXHAUSTED,
                "message size ${env.payload.size} exceeds limit of $maxReceiveMessageSize",
            ),
        )
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

    // Real-time streaming: handler runs concurrently with response writer.
    // The handler's send() pushes encoded envelopes into a channel; the
    // response writer drains them as they arrive. Headers are committed at
    // the moment the handler either calls send() for the first time or
    // returns/throws — so the conformance pattern of setting headers from
    // the request's response_definition before the first send works.
    val responseCodec = codec.codec(handler.methodSpec.responseClass)
    val outboundQueue = Channel<ByteArray>(Channel.UNLIMITED)
    val readyToCommit = CompletableDeferred<Unit>()
    val handlerErrorRef = java.util.concurrent.atomic.AtomicReference<ConnectException?>(null)
    val outPool = pickPool(call.request.headers[protocol.acceptEncodingHeader])

    coroutineScope {
        val handlerJob = async {
            val outStream = object : ServerMessageStream<Any> {
                override suspend fun send(message: Any) {
                    readyToCommit.complete(Unit)
                    outboundQueue.send(responseCodec.serialize(message).readByteArray())
                }
            }
            try {
                invokeWithTimeout(ctx.timeoutMs) { handler.handle(request, ctx, outStream) }
            } catch (ex: TimeoutCancellationException) {
                handlerErrorRef.set(ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
            } catch (ex: ConnectException) {
                handlerErrorRef.set(ex)
            } catch (ex: kotlinx.coroutines.CancellationException) {
                throw ex
            } catch (ex: Throwable) {
                handlerErrorRef.set(ex.toUnknownConnectException())
            } finally {
                readyToCommit.complete(Unit)
                outboundQueue.close()
            }
        }

        readyToCommit.await()
        writeStreamResponseHeaders(call, ctx)
        if (outPool != null) {
            call.response.headers.append(protocol.compressionHeader, outPool.name(), safeOnly = false)
        }

        val ct = ContentType.parse(requestContentType)
        val trailerHeadersRef = java.util.concurrent.atomic.AtomicReference<Headers>(Headers.Empty)
        call.respond(
            FramedStreamingContent(
                ct = ct,
                getTrailers = { trailerHeadersRef.get() },
                writeBody = { responseChannel ->
                    for (payload in outboundQueue) {
                        responseChannel.writeFully(
                            encodeOutboundEnvelope(0, payload, outPool, opts.compressMinBytes),
                        )
                        responseChannel.flush()
                    }
                    handlerJob.await()
                    val userTrailers = ctx.responseTrailers.mapValues { it.value.toList() }
                    val err = handlerErrorRef.get()
                    if (protocol.usesHttpTrailers) {
                        trailerHeadersRef.set(headersFromPairs(protocol.buildHttpTrailers(err, userTrailers)))
                    } else {
                        responseChannel.writeFully(
                            encodeEnvelope(
                                protocol.trailerEnvelopeFlag,
                                protocol.buildTrailerPayload(err, userTrailers),
                            ),
                        )
                    }
                },
            ),
        )
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
    opts: ConnectRpcOptions,
) {
    val messages = readAllRequestEnvelopes(call, handler.methodSpec.requestClass, codec, ctx, requestContentType, protocol, pool, opts.maxReceiveMessageSize)
        ?: return

    val inStream = BufferedClientMessageStream(messages, ctx.requestHeaders)

    val (response, handlerError) = try {
        invokeWithTimeout(ctx.timeoutMs) { handler.handle(inStream, ctx) } to null
    } catch (ex: TimeoutCancellationException) {
        null to ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded")
    } catch (ex: ConnectException) {
        null to ex
    } catch (ex: kotlinx.coroutines.CancellationException) {
        throw ex
    } catch (ex: Throwable) {
        null to ex.toUnknownConnectException()
    }

    val outPool = pickPool(call.request.headers[protocol.acceptEncodingHeader])
    writeStreamResponseHeaders(call, ctx)
    if (outPool != null) {
        call.response.headers.append(protocol.compressionHeader, outPool.name(), safeOnly = false)
    }
    respondStreaming(call, requestContentType, protocol, ctx, handlerError) {
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            writeFully(encodeOutboundEnvelope(0, bytes, outPool, opts.compressMinBytes))
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
    opts: ConnectRpcOptions,
) {
    // Real-time bidi: handler reads request envelopes lazily via the
    // request channel and writes response envelopes through an outbound
    // queue that the response writer drains. Same first-send-or-finish
    // gate as server-stream so response headers commit at the right time.
    val requestChannel = call.receiveChannel()
    val msgRequestCodec = codec.codec(handler.methodSpec.requestClass)
    val msgResponseCodec = codec.codec(handler.methodSpec.responseClass)
    val outPool = pickPool(call.request.headers[protocol.acceptEncodingHeader])

    val outboundQueue = Channel<ByteArray>(Channel.UNLIMITED)
    val readyToCommit = CompletableDeferred<Unit>()
    val handlerErrorRef = java.util.concurrent.atomic.AtomicReference<ConnectException?>(null)

    coroutineScope {
        val handlerJob = async {
            val bidi = StreamingBidiStream<Any, Any>(
                headers = ctx.requestHeaders,
                requestChannel = requestChannel,
                onSend = { message ->
                    readyToCommit.complete(Unit)
                    outboundQueue.send(msgResponseCodec.serialize(message).readByteArray())
                },
                requestCodec = msgRequestCodec,
                pool = pool,
                maxReceiveMessageSize = opts.maxReceiveMessageSize,
            )
            try {
                invokeWithTimeout(ctx.timeoutMs) { handler.handle(bidi, ctx) }
            } catch (ex: TimeoutCancellationException) {
                handlerErrorRef.set(ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
            } catch (ex: ConnectException) {
                handlerErrorRef.set(ex)
            } catch (ex: kotlinx.coroutines.CancellationException) {
                throw ex
            } catch (ex: Throwable) {
                handlerErrorRef.set(ex.toUnknownConnectException())
            } finally {
                readyToCommit.complete(Unit)
                outboundQueue.close()
            }
        }

        readyToCommit.await()
        writeStreamResponseHeaders(call, ctx)
        if (outPool != null) {
            call.response.headers.append(protocol.compressionHeader, outPool.name(), safeOnly = false)
        }

        val ct = ContentType.parse(requestContentType)
        val trailerHeadersRef = java.util.concurrent.atomic.AtomicReference<Headers>(Headers.Empty)
        call.respond(
            FramedStreamingContent(
                ct = ct,
                getTrailers = { trailerHeadersRef.get() },
                writeBody = { responseChannel ->
                    for (payload in outboundQueue) {
                        responseChannel.writeFully(
                            encodeOutboundEnvelope(0, payload, outPool, opts.compressMinBytes),
                        )
                        responseChannel.flush()
                    }
                    handlerJob.await()
                    val userTrailers = ctx.responseTrailers.mapValues { it.value.toList() }
                    val err = handlerErrorRef.get()
                    if (protocol.usesHttpTrailers) {
                        trailerHeadersRef.set(headersFromPairs(protocol.buildHttpTrailers(err, userTrailers)))
                    } else {
                        responseChannel.writeFully(
                            encodeEnvelope(
                                protocol.trailerEnvelopeFlag,
                                protocol.buildTrailerPayload(err, userTrailers),
                            ),
                        )
                    }
                },
            ),
        )
    }
}

private class StreamingBidiStream<Req : Any, Res : Any>(
    override val headers: ConnectHeaders,
    private val requestChannel: ByteReadChannel,
    private val onSend: suspend (Res) -> Unit,
    private val requestCodec: com.connectrpc.Codec<Req>,
    private val pool: CompressionPool?,
    private val maxReceiveMessageSize: Int,
) : BidiStream<Req, Res> {
    override suspend fun receive(): Req? {
        val env = readEnvelopeFromChannel(requestChannel) ?: return null
        val decoded = decompressEnvelopeIfNeeded(env, pool)
            ?: throw ConnectException(
                Code.INTERNAL_ERROR,
                "request envelope marked compressed but no compression negotiated",
            )
        if (maxReceiveMessageSize > 0 && decoded.payload.size > maxReceiveMessageSize) {
            throw ConnectException(
                Code.RESOURCE_EXHAUSTED,
                "message size ${decoded.payload.size} exceeds limit of $maxReceiveMessageSize",
            )
        }
        return requestCodec.deserialize(Buffer().write(decoded.payload))
    }

    override suspend fun send(message: Res) = onSend(message)
}

private suspend fun readEnvelopeFromChannel(
    channel: ByteReadChannel,
): com.connectrpc.server.protocol.ConnectEnvelope? =
    com.connectrpc.server.protocol.readEnvelopeFromBytes(
        readByte = {
            try {
                channel.readByte().toInt() and 0xff
            } catch (_: Exception) {
                -1
            }
        },
        readBytes = { n -> channel.readByteArray(n) },
    )


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
    maxReceiveMessageSize: Int,
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
            if (maxReceiveMessageSize > 0 && decoded.payload.size > maxReceiveMessageSize) {
                respondStreamError(
                    call,
                    requestContentType,
                    ctx,
                    protocol,
                    ConnectException(
                        Code.RESOURCE_EXHAUSTED,
                        "message size ${decoded.payload.size} exceeds limit of $maxReceiveMessageSize",
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

/**
 * Runs [block] under a deadline. If [timeoutMs] is null or non-positive,
 * runs without a timeout. Throws [TimeoutCancellationException] if the
 * deadline elapses; the block's coroutine (and any descendants) are
 * cancelled at that point.
 */
private suspend inline fun <T> invokeWithTimeout(timeoutMs: Long?, crossinline block: suspend () -> T): T =
    if (timeoutMs == null || timeoutMs <= 0) {
        block()
    } else {
        withTimeout(timeoutMs) { block() }
    }

/**
 * Wraps an arbitrary handler exception (NPE, RuntimeException, etc.) into
 * a Connect-protocol-compliant ConnectException with code: unknown. Mirrors
 * connect-go's behavior of mapping un-typed errors to UNKNOWN rather than
 * letting them crash the connection.
 */
private fun Throwable.toUnknownConnectException(): ConnectException {
    val msg = message ?: this::class.qualifiedName ?: "unknown error"
    return ConnectException(code = Code.UNKNOWN, message = msg, exception = this)
}

private fun newHandlerContext(
    call: ApplicationCall,
    procedure: String,
    queryParams: Map<String, List<String>>? = null,
) = HandlerContext(
    procedure = procedure,
    requestHeaders = call.request.headers.toMap(),
    httpMethod = call.request.httpMethod.value,
    timeoutMs = call.request.headers[CONNECT_TIMEOUT_HEADER]?.toLongOrNull()
        ?: call.request.headers[GRPC_TIMEOUT_HEADER]?.let(::parseGrpcTimeoutMs),
    queryParams = queryParams,
)

/**
 * Connect-GET dispatch for idempotent unary procedures. Parses
 * `?connect=v1&encoding=...&message=...&base64=1&compression=...` query params.
 */
private suspend fun dispatchConnectGet(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    opts: ConnectRpcOptions,
) {
    val maxReceiveMessageSize = opts.maxReceiveMessageSize
    val handler = registry.find(procedure) as? UnaryHandler<*, *>
    if (handler == null) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
        )
        return
    }

    val params = call.request.queryParameters
    if (params["connect"] != "v1") {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.INVALID_ARGUMENT, "missing or invalid connect query param"),
        )
        return
    }
    val encoding = params["encoding"]
    val codecName = when (encoding) {
        CODEC_NAME_PROTO -> CODEC_NAME_PROTO
        CODEC_NAME_JSON -> CODEC_NAME_JSON
        else -> {
            respondConnectUnaryError(
                call,
                ctx = null,
                ConnectException(Code.INVALID_ARGUMENT, "missing or invalid encoding query param"),
            )
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

    val message = params["message"]
    if (message == null) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.INVALID_ARGUMENT, "missing message query param"),
        )
        return
    }
    val isBase64 = params["base64"] == "1"
    val rawBytes = try {
        if (isBase64) {
            // Connect spec uses URL-safe, unpadded base64 for the message.
            java.util.Base64.getUrlDecoder().decode(message.padBase64())
        } else {
            message.toByteArray(Charsets.UTF_8)
        }
    } catch (ex: Exception) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode message: ${ex.message}"),
        )
        return
    }

    val compression = params["compression"]
    val pool = when {
        compression == null || compression == IDENTITY_ENCODING -> null
        else -> COMPRESSION_POOLS[compression] ?: run {
            respondConnectUnaryError(
                call,
                ctx = null,
                ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $compression"),
            )
            return
        }
    }
    val decompressed = if (pool != null) {
        try {
            pool.decompress(Buffer().write(rawBytes)).readByteArray()
        } catch (ex: Exception) {
            respondConnectUnaryError(
                call,
                ctx = null,
                ConnectException(Code.INVALID_ARGUMENT, "could not decompress message: ${ex.message}"),
            )
            return
        }
    } else {
        rawBytes
    }

    if (maxReceiveMessageSize > 0 && decompressed.size > maxReceiveMessageSize) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(
                Code.RESOURCE_EXHAUSTED,
                "message size ${decompressed.size} exceeds limit of $maxReceiveMessageSize",
            ),
        )
        return
    }
    @Suppress("UNCHECKED_CAST")
    val unary = (handler as UnaryHandler<Any, Any>).wrapUnary(registry.interceptors + registry.interceptorsFor(procedure))
    val request = try {
        codec.codec(unary.methodSpec.requestClass).deserialize(Buffer().write(decompressed))
    } catch (ex: Exception) {
        respondConnectUnaryError(
            call,
            ctx = null,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    val ctx = newHandlerContext(call, procedure, queryParamsAsMap(call))

    val response = try {
        invokeWithTimeout(ctx.timeoutMs) { unary.handle(request, ctx) }
    } catch (ex: TimeoutCancellationException) {
        respondConnectUnaryError(call, ctx, ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
        return
    } catch (ex: ConnectException) {
        respondConnectUnaryError(call, ctx, ex)
        return
    } catch (ex: kotlinx.coroutines.CancellationException) {
        throw ex
    } catch (ex: Throwable) {
        respondConnectUnaryError(call, ctx, ex.toUnknownConnectException())
        return
    }

    writeUnaryHeadersAndTrailers(call, ctx)
    val responseBytes = codec.codec(unary.methodSpec.responseClass)
        .serialize(response)
        .readByteArray()
    val responseContentType = "application/$codecName"
    call.respondBytes(
        bytes = responseBytes,
        contentType = ContentType.parse(responseContentType),
        status = HttpStatusCode.OK,
    )
}

private fun queryParamsAsMap(call: ApplicationCall): Map<String, List<String>> {
    val out = mutableMapOf<String, MutableList<String>>()
    for (name in call.request.queryParameters.names()) {
        out[name] = call.request.queryParameters.getAll(name).orEmpty().toMutableList()
    }
    return out
}

/** Pads URL-safe base64 to a multiple of 4 by appending '=' chars. */
private fun String.padBase64(): String =
    when (length % 4) {
        2 -> "$this=="
        3 -> "$this="
        else -> this
    }

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
    if (protocol.usesHttpTrailers) {
        // gRPC trailers-only response: there's no body to write, so HTTP/1.1
        // chunked-trailer framing won't fire reliably. Mirror connect-go's
        // approach by promoting the grpc-* trailer pairs to regular response
        // headers — they reach the client either way.
        for ((name, value) in protocol.buildHttpTrailers(exception, userTrailers)) {
            call.response.headers.append(name, value, safeOnly = false)
        }
        call.respondBytes(
            bytes = ByteArray(0),
            contentType = ContentType.parse(requestContentType),
            status = HttpStatusCode.OK,
        )
        return
    }
    call.respond(
        FramedStreamingContent(
            ct = ContentType.parse(requestContentType),
            getTrailers = { Headers.Empty },
            writeBody = { channel ->
                channel.writeFully(
                    encodeEnvelope(
                        protocol.trailerEnvelopeFlag,
                        protocol.buildTrailerPayload(exception, userTrailers),
                    ),
                )
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

/**
 * Composes the interceptor chain around a handler. The first interceptor in
 * the list runs outermost (sees the request first, response last).
 */
private fun <Req : Any, Res : Any> UnaryHandler<Req, Res>.wrapUnary(
    interceptors: List<ServerInterceptor>,
): UnaryHandler<Req, Res> {
    var current: UnaryHandler<Req, Res> = this
    for (interceptor in interceptors.asReversed()) {
        current = interceptor.wrapUnary(current)
    }
    return current
}

private fun <Req : Any, Res : Any> ServerStreamHandler<Req, Res>.wrapServerStream(
    interceptors: List<ServerInterceptor>,
): ServerStreamHandler<Req, Res> {
    var current: ServerStreamHandler<Req, Res> = this
    for (interceptor in interceptors.asReversed()) {
        current = interceptor.wrapServerStream(current)
    }
    return current
}

private fun <Req : Any, Res : Any> ClientStreamHandler<Req, Res>.wrapClientStream(
    interceptors: List<ServerInterceptor>,
): ClientStreamHandler<Req, Res> {
    var current: ClientStreamHandler<Req, Res> = this
    for (interceptor in interceptors.asReversed()) {
        current = interceptor.wrapClientStream(current)
    }
    return current
}

private fun <Req : Any, Res : Any> BidiStreamHandler<Req, Res>.wrapBidi(
    interceptors: List<ServerInterceptor>,
): BidiStreamHandler<Req, Res> {
    var current: BidiStreamHandler<Req, Res> = this
    for (interceptor in interceptors.asReversed()) {
        current = interceptor.wrapBidi(current)
    }
    return current
}
