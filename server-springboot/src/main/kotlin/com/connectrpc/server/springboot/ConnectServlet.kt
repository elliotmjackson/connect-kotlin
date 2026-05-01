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

package com.connectrpc.server.springboot

import com.connectrpc.CODEC_NAME_JSON
import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.compression.CompressionPool
import com.connectrpc.Headers as ConnectHeaders
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
import jakarta.servlet.http.HttpServlet
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import okio.Buffer

private const val CONNECT_TIMEOUT_HEADER = "Connect-Timeout-Ms"
private const val GRPC_TIMEOUT_HEADER = "Grpc-Timeout"
private const val CONNECT_CONTENT_ENCODING_HEADER = "Connect-Content-Encoding"
private const val GRPC_ENCODING_HEADER = "Grpc-Encoding"
private const val IDENTITY_ENCODING = "identity"
private const val CONNECT_PROTOCOL_VERSION_HEADER = "Connect-Protocol-Version"
private const val CONNECT_PROTOCOL_VERSION_VALUE = "1"

/**
 * Per-protocol wire wrap: how to frame/encode trailers, which header names to
 * read for compression, whether unary should still be enveloped (gRPC and
 * gRPC-Web do, Connect doesn't), and whether end-of-RPC trailers go into
 * HTTP trailing headers (gRPC) or onto the body envelope (Connect / gRPC-Web).
 */
private data class StreamingProtocol(
    val trailerEnvelopeFlag: Int,
    val buildTrailerPayload: (ConnectException?, Map<String, List<String>>) -> ByteArray,
    val buildHttpTrailers: (ConnectException?, Map<String, List<String>>) -> List<Pair<String, String>>,
    val compressionHeader: String,
    val acceptEncodingHeader: String,
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

/**
 * Dispatches Connect / gRPC / gRPC-Web requests by [com.connectrpc.server.HandlerRegistry] lookup.
 * Mounted at the root path by [ConnectRpcAutoConfiguration]; routes the request URI to the
 * registered procedure of the same fully-qualified name.
 *
 * Currently implements unary Connect requests. Streaming and gRPC/gRPC-Web
 * are added incrementally.
 */
class ConnectServlet(
    private val registry: HandlerRegistry,
    private val options: ConnectRpcOptions = ConnectRpcOptions(),
) : HttpServlet() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    override fun service(req: HttpServletRequest, resp: HttpServletResponse) {
        val procedure = req.requestURI.removePrefix("/")
        val method = req.method
        // Connect/gRPC procedures are POST; idempotent ones accept GET via
        // Connect-GET (handled later). Reject all other verbs so misbehaving
        // clients fail loudly instead of getting a 200 echo.
        if (method != "POST" && method != "GET") {
            resp.status = HttpServletResponse.SC_METHOD_NOT_ALLOWED
            return
        }
        if (registry.find(procedure) == null) {
            resp.status = HttpServletResponse.SC_NOT_FOUND
            return
        }
        val async = req.startAsync()
        async.timeout = 0L
        scope.launch {
            try {
                dispatch(req, resp, procedure)
            } catch (ex: Throwable) {
                writeUnaryConnectError(resp, ex.toUnknownConnectException())
            } finally {
                async.complete()
            }
        }
    }

    private suspend fun dispatch(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        procedure: String,
    ) {
        val contentType = req.contentType?.substringBefore(';')?.trim() ?: ""
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
                resp.status = HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE
                return
            }
        }
        val codec = registry.codec(codecName)
        if (codec == null) {
            writeUnaryConnectError(
                resp,
                ConnectException(Code.UNIMPLEMENTED, "codec $codecName is not registered"),
            )
            return
        }

        val isConnectProtocol = protocol == null || protocol === CONNECT_STREAMING
        if (isConnectProtocol && options.requireConnectProtocolHeader) {
            val v = req.getHeader(CONNECT_PROTOCOL_VERSION_HEADER)
            if (v != CONNECT_PROTOCOL_VERSION_VALUE) {
                val msg = if (v == null) {
                    "missing required header: $CONNECT_PROTOCOL_VERSION_HEADER"
                } else {
                    "unsupported $CONNECT_PROTOCOL_VERSION_HEADER: $v"
                }
                if (protocol == null) {
                    writeUnaryConnectError(resp, ConnectException(Code.INVALID_ARGUMENT, msg))
                } else {
                    respondStreamError(resp, contentType, ctx = null, protocol, ConnectException(Code.INVALID_ARGUMENT, msg))
                }
                return
            }
        }
        if (protocol === GRPC_STREAMING) {
            // gRPC requires TE: trailers — reject early so misconfigured clients see it.
            val te = req.getHeaders("TE")?.toList().orEmpty()
            val hasTrailers = te.any { it.split(',').any { v -> v.trim().equals("trailers", ignoreCase = true) } }
            if (!hasTrailers) {
                respondStreamError(
                    resp,
                    contentType,
                    ctx = null,
                    protocol,
                    ConnectException(Code.INTERNAL_ERROR, "missing required header: TE: trailers"),
                )
                return
            }
        }

        if (protocol == null) {
            handleConnectUnary(req, resp, procedure, codec, codecName)
        } else {
            handleStreaming(req, resp, procedure, codec, contentType, protocol)
        }
    }

    private suspend fun handleStreaming(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        procedure: String,
        codec: SerializationStrategy,
        requestContentType: String,
        protocol: StreamingProtocol,
    ) {
        val handler = registry.find(procedure)
        if (handler == null) {
            respondStreamError(
                resp,
                requestContentType,
                ctx = null,
                protocol,
                ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
            )
            return
        }
        val ctx = newHandlerContext(req, procedure)

        val streamEncoding = req.getHeader(protocol.compressionHeader)
        val streamPool: CompressionPool? = when {
            streamEncoding == null || streamEncoding == IDENTITY_ENCODING -> null
            else -> registry.compressionPool(streamEncoding) ?: run {
                respondStreamError(
                    resp,
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
                        resp,
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
                handleUnaryAsStream(req, resp, uh, ctx, codec, requestContentType, protocol, streamPool)
            }
            StreamType.SERVER -> {
                val combined = registry.interceptors + registry.interceptorsFor(procedure)
                @Suppress("UNCHECKED_CAST")
                val sh = (handler as ServerStreamHandler<Any, Any>).wrapServerStream(combined)
                handleServerStream(req, resp, sh, ctx, codec, requestContentType, protocol, streamPool)
            }
            StreamType.CLIENT -> {
                val combined = registry.interceptors + registry.interceptorsFor(procedure)
                @Suppress("UNCHECKED_CAST")
                val ch = (handler as ClientStreamHandler<Any, Any>).wrapClientStream(combined)
                handleClientStream(req, resp, ch, ctx, codec, requestContentType, protocol, streamPool)
            }
            StreamType.BIDI -> {
                val combined = registry.interceptors + registry.interceptorsFor(procedure)
                @Suppress("UNCHECKED_CAST")
                val bh = (handler as BidiStreamHandler<Any, Any>).wrapBidi(combined)
                handleBidiStream(req, resp, bh, ctx, codec, requestContentType, protocol, streamPool)
            }
        }
    }

    private suspend fun handleUnaryAsStream(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        handler: UnaryHandler<Any, Any>,
        ctx: HandlerContext,
        codec: SerializationStrategy,
        requestContentType: String,
        protocol: StreamingProtocol,
        pool: CompressionPool?,
    ) {
        val rawBytes = req.inputStream.readAllBytes()
        val (env, error) = readSingleStreamEnvelope(rawBytes, pool)
        if (error != null) {
            respondStreamError(resp, requestContentType, ctx, protocol, error)
            return
        }
        env!!
        if (options.maxReceiveMessageSize > 0 && env.payload.size > options.maxReceiveMessageSize) {
            respondStreamError(
                resp,
                requestContentType,
                ctx,
                protocol,
                ConnectException(
                    Code.RESOURCE_EXHAUSTED,
                    "message size ${env.payload.size} exceeds limit of ${options.maxReceiveMessageSize}",
                ),
            )
            return
        }
        val request = try {
            codec.codec(handler.methodSpec.requestClass).deserialize(Buffer().write(env.payload))
        } catch (ex: Exception) {
            respondStreamError(
                resp,
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

        val outPool = pickPool(req.getHeader(protocol.acceptEncodingHeader))
        beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            resp.outputStream.write(encodeOutboundEnvelope(0, bytes, outPool, options.compressMinBytes))
        }
        finishStreamResponse(resp, ctx, protocol, handlerError)
    }

    private suspend fun handleServerStream(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        handler: ServerStreamHandler<Any, Any>,
        ctx: HandlerContext,
        codec: SerializationStrategy,
        requestContentType: String,
        protocol: StreamingProtocol,
        pool: CompressionPool?,
    ) {
        val rawBytes = req.inputStream.readAllBytes()
        val (env, error) = readSingleStreamEnvelope(rawBytes, pool)
        if (error != null) {
            respondStreamError(resp, requestContentType, ctx, protocol, error)
            return
        }
        env!!
        if (options.maxReceiveMessageSize > 0 && env.payload.size > options.maxReceiveMessageSize) {
            respondStreamError(
                resp,
                requestContentType,
                ctx,
                protocol,
                ConnectException(
                    Code.RESOURCE_EXHAUSTED,
                    "message size ${env.payload.size} exceeds limit of ${options.maxReceiveMessageSize}",
                ),
            )
            return
        }
        val request = try {
            codec.codec(handler.methodSpec.requestClass).deserialize(Buffer().write(env.payload))
        } catch (ex: Exception) {
            respondStreamError(
                resp,
                requestContentType,
                ctx,
                protocol,
                ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
            )
            return
        }

        val outPool = pickPool(req.getHeader(protocol.acceptEncodingHeader))
        val responseCodec = codec.codec(handler.methodSpec.responseClass)
        // Headers/trailer-supplier must be set before any byte hits the wire.
        // Set them upfront, then flip a "started" guard on first send so user
        // code can mutate ctx.responseHeaders before then.
        var started = false
        val outStream = object : ServerMessageStream<Any> {
            override suspend fun send(message: Any) {
                if (!started) {
                    started = true
                    beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
                }
                val bytes = responseCodec.serialize(message).readByteArray()
                resp.outputStream.write(encodeOutboundEnvelope(0, bytes, outPool, options.compressMinBytes))
                resp.outputStream.flush()
            }
        }

        val handlerError: ConnectException? = try {
            invokeWithTimeout(ctx.timeoutMs) { handler.handle(request, ctx, outStream) }
            null
        } catch (ex: TimeoutCancellationException) {
            ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded")
        } catch (ex: ConnectException) {
            ex
        } catch (ex: kotlinx.coroutines.CancellationException) {
            throw ex
        } catch (ex: Throwable) {
            ex.toUnknownConnectException()
        }

        if (!started) {
            beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
        }
        finishStreamResponse(resp, ctx, protocol, handlerError)
    }

    private suspend fun handleClientStream(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        handler: ClientStreamHandler<Any, Any>,
        ctx: HandlerContext,
        codec: SerializationStrategy,
        requestContentType: String,
        protocol: StreamingProtocol,
        pool: CompressionPool?,
    ) {
        val messages = readAllRequestEnvelopes(
            req.inputStream.readAllBytes(),
            handler.methodSpec.requestClass,
            codec,
            pool,
        ) ?: run {
            respondStreamError(
                resp,
                requestContentType,
                ctx,
                protocol,
                ConnectException(Code.INTERNAL_ERROR, "request envelope marked compressed but no compression negotiated"),
            )
            return
        }

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

        val outPool = pickPool(req.getHeader(protocol.acceptEncodingHeader))
        beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
        if (response != null) {
            val bytes = codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            resp.outputStream.write(encodeOutboundEnvelope(0, bytes, outPool, options.compressMinBytes))
        }
        finishStreamResponse(resp, ctx, protocol, handlerError)
    }

    /**
     * Half-duplex bidi: drain all inbound envelopes first, then run the
     * handler, which can interleave [com.connectrpc.server.BidiStream.receive]
     * and [com.connectrpc.server.BidiStream.send] freely. Full-duplex over
     * HTTP/2 needs async ReadListener/WriteListener and is a follow-up.
     */
    private suspend fun handleBidiStream(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        handler: BidiStreamHandler<Any, Any>,
        ctx: HandlerContext,
        codec: SerializationStrategy,
        requestContentType: String,
        protocol: StreamingProtocol,
        pool: CompressionPool?,
    ) {
        val messages = readAllRequestEnvelopes(
            req.inputStream.readAllBytes(),
            handler.methodSpec.requestClass,
            codec,
            pool,
        ) ?: run {
            respondStreamError(
                resp,
                requestContentType,
                ctx,
                protocol,
                ConnectException(Code.INTERNAL_ERROR, "request envelope marked compressed but no compression negotiated"),
            )
            return
        }

        val outPool = pickPool(req.getHeader(protocol.acceptEncodingHeader))
        val responseCodec = codec.codec(handler.methodSpec.responseClass)
        var started = false
        val bidi = BufferedBidiStream<Any, Any>(
            messages = messages,
            headers = ctx.requestHeaders,
            onSend = { message ->
                if (!started) {
                    started = true
                    beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
                }
                val bytes = responseCodec.serialize(message).readByteArray()
                resp.outputStream.write(encodeOutboundEnvelope(0, bytes, outPool, options.compressMinBytes))
                resp.outputStream.flush()
            },
        )

        val handlerError: ConnectException? = try {
            invokeWithTimeout(ctx.timeoutMs) { handler.handle(bidi, ctx) }
            null
        } catch (ex: TimeoutCancellationException) {
            ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded")
        } catch (ex: ConnectException) {
            ex
        } catch (ex: kotlinx.coroutines.CancellationException) {
            throw ex
        } catch (ex: Throwable) {
            ex.toUnknownConnectException()
        }

        if (!started) {
            beginStreamResponse(resp, ctx, requestContentType, protocol, outPool)
        }
        finishStreamResponse(resp, ctx, protocol, handlerError)
    }

    /**
     * Reads every envelope from the buffered request body, applying inbound
     * compression and message-size enforcement. Returns the decoded list, or
     * null if a compressed envelope arrived without negotiated compression.
     */
    private fun readAllRequestEnvelopes(
        rawBytes: ByteArray,
        requestClass: kotlin.reflect.KClass<Any>,
        codec: SerializationStrategy,
        pool: CompressionPool?,
    ): List<Any>? {
        val buffer = Buffer().write(rawBytes)
        val msgCodec = codec.codec(requestClass)
        val messages = mutableListOf<Any>()
        while (true) {
            val env = decodeNextEnvelope(buffer) ?: break
            val decoded = decompressEnvelopeIfNeeded(env, pool) ?: return null
            if (options.maxReceiveMessageSize > 0 && decoded.payload.size > options.maxReceiveMessageSize) {
                throw ConnectException(
                    Code.RESOURCE_EXHAUSTED,
                    "message size ${decoded.payload.size} exceeds limit of ${options.maxReceiveMessageSize}",
                )
            }
            messages += msgCodec.deserialize(Buffer().write(decoded.payload))
        }
        return messages
    }

    /**
     * Set status, headers, content-type, content-encoding, and (for gRPC)
     * trailer supplier — everything must land before the first byte hits the
     * output stream.
     */
    private fun beginStreamResponse(
        resp: HttpServletResponse,
        ctx: HandlerContext,
        requestContentType: String,
        protocol: StreamingProtocol,
        outPool: CompressionPool?,
    ) {
        resp.status = HttpServletResponse.SC_OK
        resp.contentType = requestContentType
        for ((name, values) in ctx.responseHeaders) {
            for (v in values) resp.addHeader(name, v)
        }
        if (outPool != null) {
            resp.setHeader(protocol.compressionHeader, outPool.name())
        }
        if (protocol.usesHttpTrailers) {
            // gRPC over HTTP/2 — declare the trailer-fields supplier upfront,
            // mutate the holder later. Servlet 4+ wires this into the HTTP/2
            // trailers frame at end-of-response.
            resp.setTrailerFields {
                val pairs = protocol.buildHttpTrailers(
                    /* exception */ null,
                    ctx.responseTrailers.mapValues { it.value.toList() },
                )
                joinMultiValuePairs(pairs)
            }
        }
    }

    /**
     * For envelope-trailer protocols (Connect / gRPC-Web), write the trailer
     * envelope. For HTTP-trailer protocols (gRPC), swap in a final supplier
     * that captures the handler's outcome.
     */
    private fun finishStreamResponse(
        resp: HttpServletResponse,
        ctx: HandlerContext,
        protocol: StreamingProtocol,
        handlerError: ConnectException?,
    ) {
        val userTrailers = ctx.responseTrailers.mapValues { it.value.toList() }
        if (protocol.usesHttpTrailers) {
            resp.setTrailerFields {
                joinMultiValuePairs(protocol.buildHttpTrailers(handlerError, userTrailers))
            }
        } else {
            resp.outputStream.write(
                encodeEnvelope(
                    protocol.trailerEnvelopeFlag,
                    protocol.buildTrailerPayload(handlerError, userTrailers),
                ),
            )
        }
        resp.outputStream.flush()
    }

    private fun readSingleStreamEnvelope(
        rawBytes: ByteArray,
        pool: CompressionPool?,
    ): Pair<com.connectrpc.server.protocol.ConnectEnvelope?, ConnectException?> {
        val buffer = Buffer().write(rawBytes)
        val envelope = try {
            decodeNextEnvelope(buffer)
                ?: return null to ConnectException(
                    Code.UNIMPLEMENTED,
                    "expects exactly one request envelope, got 0",
                )
        } catch (ex: ConnectException) {
            return null to ex
        }
        if (decodeNextEnvelope(buffer) != null) {
            return null to ConnectException(
                Code.UNIMPLEMENTED,
                "expects exactly one request envelope, got more",
            )
        }
        val decoded = decompressEnvelopeIfNeeded(envelope, pool)
            ?: return null to ConnectException(
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

    private fun respondStreamError(
        resp: HttpServletResponse,
        requestContentType: String,
        ctx: HandlerContext?,
        protocol: StreamingProtocol,
        exception: ConnectException,
    ) {
        resp.status = HttpServletResponse.SC_OK
        resp.contentType = requestContentType
        if (ctx != null) {
            for ((name, values) in ctx.responseHeaders) {
                for (v in values) resp.addHeader(name, v)
            }
        }
        val userTrailers = ctx?.responseTrailers?.mapValues { it.value.toList() } ?: emptyMap()
        if (protocol.usesHttpTrailers) {
            // Tomcat sends an empty DATA frame instead of a true trailer-only
            // response, which strict gRPC clients reject — promote grpc-* to
            // regular response headers so they reach the client either way.
            for ((name, value) in protocol.buildHttpTrailers(exception, userTrailers)) {
                resp.addHeader(name, value)
            }
            resp.outputStream.flush()
            return
        }
        resp.outputStream.write(
            encodeEnvelope(
                protocol.trailerEnvelopeFlag,
                protocol.buildTrailerPayload(exception, userTrailers),
            ),
        )
        resp.outputStream.flush()
    }

    private suspend fun handleConnectUnary(
        req: HttpServletRequest,
        resp: HttpServletResponse,
        procedure: String,
        codec: SerializationStrategy,
        codecName: String,
    ) {
        val handler = registry.find(procedure)
        if (handler == null || handler.methodSpec.streamType != StreamType.UNARY) {
            writeUnaryConnectError(resp, ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"))
            return
        }
        @Suppress("UNCHECKED_CAST")
        val unary = (handler as UnaryHandler<Any, Any>)
            .wrapUnary(registry.interceptors + registry.interceptorsFor(procedure))
        val ctx = newHandlerContext(req, procedure)

        val contentEncoding = req.getHeader("Content-Encoding")
        val requestPool: CompressionPool? = when {
            contentEncoding == null || contentEncoding == IDENTITY_ENCODING -> null
            else -> registry.compressionPool(contentEncoding) ?: run {
                writeUnaryConnectError(
                    resp,
                    ConnectException(Code.UNIMPLEMENTED, "unsupported compression: $contentEncoding"),
                )
                return
            }
        }

        val rawBytes = req.inputStream.readAllBytes()
        val requestBytes = if (requestPool != null) {
            try {
                requestPool.decompress(Buffer().write(rawBytes)).readByteArray()
            } catch (ex: Exception) {
                writeUnaryConnectError(
                    resp,
                    ConnectException(Code.INVALID_ARGUMENT, "could not decompress request: ${ex.message}"),
                )
                return
            }
        } else {
            rawBytes
        }
        if (options.maxReceiveMessageSize > 0 && requestBytes.size > options.maxReceiveMessageSize) {
            writeUnaryConnectError(
                resp,
                ConnectException(
                    Code.RESOURCE_EXHAUSTED,
                    "message size ${requestBytes.size} exceeds limit of ${options.maxReceiveMessageSize}",
                ),
            )
            return
        }

        val request = try {
            codec.codec(unary.methodSpec.requestClass).deserialize(Buffer().write(requestBytes))
        } catch (ex: Exception) {
            writeUnaryConnectError(
                resp,
                ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
            )
            return
        }

        val response = try {
            invokeWithTimeout(ctx.timeoutMs) { unary.handle(request, ctx) }
        } catch (ex: TimeoutCancellationException) {
            writeUnaryConnectError(resp, ctx, ConnectException(Code.DEADLINE_EXCEEDED, "deadline exceeded"))
            return
        } catch (ex: ConnectException) {
            writeUnaryConnectError(resp, ctx, ex)
            return
        } catch (ex: kotlinx.coroutines.CancellationException) {
            throw ex
        } catch (ex: Throwable) {
            writeUnaryConnectError(resp, ctx, ex.toUnknownConnectException())
            return
        }

        applyUnaryHeaders(resp, ctx)
        val rawResponseBytes = codec.codec(unary.methodSpec.responseClass)
            .serialize(response)
            .readByteArray()
        val (finalBytes, encoding) = maybeCompressOutbound(
            rawResponseBytes,
            req.getHeader("Accept-Encoding"),
            options.compressMinBytes,
        )
        if (encoding != null) resp.setHeader("Content-Encoding", encoding)
        resp.contentType = "application/$codecName"
        resp.status = HttpServletResponse.SC_OK
        resp.outputStream.write(finalBytes)
        resp.outputStream.flush()
    }

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
        return accepted.firstNotNullOfOrNull { registry.compressionPool(it) }
    }

    private fun newHandlerContext(req: HttpServletRequest, procedure: String) =
        HandlerContext(
            procedure = procedure,
            requestHeaders = headersAsMap(req),
            httpMethod = req.method,
            timeoutMs = req.getHeader(CONNECT_TIMEOUT_HEADER)?.toLongOrNull()
                ?: req.getHeader(GRPC_TIMEOUT_HEADER)?.let(::parseGrpcTimeoutMs),
        )

    private fun headersAsMap(req: HttpServletRequest): Map<String, List<String>> {
        val out = mutableMapOf<String, MutableList<String>>()
        val names = req.headerNames
        while (names.hasMoreElements()) {
            val name = names.nextElement()
            val values = req.getHeaders(name).toList()
            out[name] = values.toMutableList()
        }
        return out
    }

    private fun applyUnaryHeaders(resp: HttpServletResponse, ctx: HandlerContext) {
        for ((name, values) in ctx.responseHeaders) {
            for (v in values) resp.addHeader(name, v)
        }
        // Connect unary surfaces trailers as `trailer-<name>` HTTP headers.
        for ((name, values) in ctx.responseTrailers) {
            for (v in values) resp.addHeader("trailer-$name", v)
        }
    }

    private fun writeUnaryConnectError(resp: HttpServletResponse, exception: ConnectException) {
        writeUnaryConnectError(resp, ctx = null, exception)
    }

    private fun writeUnaryConnectError(
        resp: HttpServletResponse,
        ctx: HandlerContext?,
        exception: ConnectException,
    ) {
        if (ctx != null) applyUnaryHeaders(resp, ctx)
        resp.contentType = CONNECT_ERROR_CONTENT_TYPE
        resp.status = exception.code.connectHttpStatus()
        val body = connectErrorJsonBody(exception)
        resp.outputStream.write(body)
        resp.outputStream.flush()
    }
}

/** Tunables for [ConnectServlet]. Mirrors the Ktor adapter's settings. */
data class ConnectRpcOptions(
    /** Cap on a single request message size after decompression. Zero = unlimited. */
    val maxReceiveMessageSize: Int = 0,
    /** If true, reject Connect POSTs missing `Connect-Protocol-Version: 1`. */
    val requireConnectProtocolHeader: Boolean = false,
    /** Smallest response message eligible for outbound compression. */
    val compressMinBytes: Int = 1024,
)

private suspend inline fun <T> invokeWithTimeout(timeoutMs: Long?, crossinline block: suspend () -> T): T =
    if (timeoutMs == null || timeoutMs <= 0) block() else withTimeout(timeoutMs) { block() }

private fun Throwable.toUnknownConnectException(): ConnectException {
    if (this is ConnectException) return this
    val msg = message ?: this::class.qualifiedName ?: "unknown error"
    return ConnectException(code = Code.UNKNOWN, message = msg, exception = this)
}

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

/**
 * Servlet's [jakarta.servlet.http.HttpServletResponse.setTrailerFields] takes
 * a `Supplier<Map<String, String>>` — single value per name. gRPC metadata can
 * carry multi-value entries; HTTP semantics permit comma-joined values for
 * repeated headers, and the gRPC-java reference client splits them back out.
 */
private fun joinMultiValuePairs(pairs: List<Pair<String, String>>): Map<String, String> {
    val grouped = LinkedHashMap<String, MutableList<String>>()
    for ((k, v) in pairs) grouped.getOrPut(k) { mutableListOf() }.add(v)
    return grouped.mapValues { it.value.joinToString(",") }
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
