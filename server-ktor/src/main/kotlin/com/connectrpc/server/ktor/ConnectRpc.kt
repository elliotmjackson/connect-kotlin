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
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.Headers
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
import com.connectrpc.server.protocol.connectErrorJsonBody
import com.connectrpc.server.protocol.connectHttpStatus
import com.connectrpc.server.protocol.decodeNextEnvelope
import com.connectrpc.server.protocol.encodeEnvelope
import com.connectrpc.server.protocol.endStreamJsonPayload
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

/**
 * Mounts a Connect/gRPC/gRPC-Web server into a Ktor [Application].
 *
 * Milestone-3.1 scope: Connect unary + Connect server-stream. Other stream types
 * and gRPC/gRPC-Web protocols return UNIMPLEMENTED or 415.
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

private suspend fun dispatch(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
) {
    val contentType = call.request.contentType().withoutParameters().toString()
    val (codecName, isStream) = when (contentType) {
        CONNECT_UNARY_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO to false
        CONNECT_UNARY_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to false
        CONNECT_STREAM_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO to true
        CONNECT_STREAM_CONTENT_TYPE_JSON -> CODEC_NAME_JSON to true
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
    if (isStream) {
        handleConnectStream(call, registry, procedure, codec, contentType)
    } else {
        handleConnectUnary(call, registry, procedure, codec, contentType)
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

private suspend fun handleConnectStream(
    call: ApplicationCall,
    registry: HandlerRegistry,
    procedure: String,
    codec: SerializationStrategy,
    requestContentType: String,
) {
    val handler = registry.find(procedure)
    if (handler == null) {
        respondStreamError(
            call,
            requestContentType,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
        )
        return
    }
    when (handler.methodSpec.streamType) {
        StreamType.SERVER -> {
            @Suppress("UNCHECKED_CAST")
            val sh = handler as ServerStreamHandler<Any, Any>
            handleServerStream(call, sh, codec, requestContentType, procedure)
        }
        StreamType.CLIENT -> {
            @Suppress("UNCHECKED_CAST")
            val ch = handler as ClientStreamHandler<Any, Any>
            handleClientStream(call, ch, codec, requestContentType, procedure)
        }
        StreamType.BIDI -> {
            @Suppress("UNCHECKED_CAST")
            val bh = handler as BidiStreamHandler<Any, Any>
            handleBidiStream(call, bh, codec, requestContentType, procedure)
        }
        else -> respondStreamError(
            call,
            requestContentType,
            ctx = null,
            ConnectException(
                Code.UNIMPLEMENTED,
                "stream type ${handler.methodSpec.streamType} not yet implemented",
            ),
        )
    }
}

private suspend fun handleClientStream(
    call: ApplicationCall,
    handler: ClientStreamHandler<Any, Any>,
    codec: SerializationStrategy,
    requestContentType: String,
    procedure: String,
) {
    val ctx = newHandlerContext(call, procedure)
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val messages = mutableListOf<Any>()
    val msgCodec = codec.codec(handler.methodSpec.requestClass)
    try {
        while (true) {
            val env = decodeNextEnvelope(buffer) ?: break
            messages += msgCodec.deserialize(Buffer().write(env.payload))
        }
    } catch (ex: ConnectException) {
        respondStreamError(call, requestContentType, ctx, ex)
        return
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

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
            val responseBytes =
                codec.codec(handler.methodSpec.responseClass).serialize(response).readByteArray()
            writeFully(encodeEnvelope(0, responseBytes))
        }
        writeFully(
            encodeEnvelope(
                ENVELOPE_FLAG_END_STREAM,
                endStreamJsonPayload(handlerError, ctx.responseTrailers.mapValues { it.value.toList() }),
            ),
        )
    }
}

private class BufferedClientMessageStream<Req : Any>(
    messages: List<Req>,
    override val headers: Headers,
) : ClientMessageStream<Req> {
    private val iterator = messages.iterator()
    override suspend fun receive(): Req? = if (iterator.hasNext()) iterator.next() else null
}

private suspend fun handleBidiStream(
    call: ApplicationCall,
    handler: BidiStreamHandler<Any, Any>,
    codec: SerializationStrategy,
    requestContentType: String,
    procedure: String,
) {
    val ctx = newHandlerContext(call, procedure)
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val messages = mutableListOf<Any>()
    val msgCodec = codec.codec(handler.methodSpec.requestClass)
    try {
        while (true) {
            val env = decodeNextEnvelope(buffer) ?: break
            messages += msgCodec.deserialize(Buffer().write(env.payload))
        }
    } catch (ex: ConnectException) {
        respondStreamError(call, requestContentType, ctx, ex)
        return
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    // Half-duplex over HTTP/1: the handler reads all requests, then the
    // adapter writes all buffered responses. Full-duplex requires HTTP/2.
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
        writeFully(
            encodeEnvelope(
                ENVELOPE_FLAG_END_STREAM,
                endStreamJsonPayload(handlerError, ctx.responseTrailers.mapValues { it.value.toList() }),
            ),
        )
    }
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

private suspend fun handleServerStream(
    call: ApplicationCall,
    handler: ServerStreamHandler<Any, Any>,
    codec: SerializationStrategy,
    requestContentType: String,
    procedure: String,
) {
    val ctx = newHandlerContext(call, procedure)
    val requestBytes = call.receiveChannel().toByteArray()
    val buffer = Buffer().write(requestBytes)
    val envelope = try {
        decodeNextEnvelope(buffer)
            ?: throw ConnectException(Code.INVALID_ARGUMENT, "no request envelope")
    } catch (ex: ConnectException) {
        respondStreamError(call, requestContentType, ctx, ex)
        return
    }
    val request = try {
        codec.codec(handler.methodSpec.requestClass)
            .deserialize(Buffer().write(envelope.payload))
    } catch (ex: Exception) {
        respondStreamError(
            call,
            requestContentType,
            ctx,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    // Buffer outbound messages so we can set HTTP headers from ctx before
    // committing the response. Real-time streaming is a follow-up.
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
        writeFully(
            encodeEnvelope(
                ENVELOPE_FLAG_END_STREAM,
                endStreamJsonPayload(handlerError, ctx.responseTrailers.mapValues { it.value.toList() }),
            ),
        )
    }
}

private fun newHandlerContext(call: ApplicationCall, procedure: String) =
    HandlerContext(
        procedure = procedure,
        requestHeaders = call.request.headers.toMap(),
        httpMethod = call.request.httpMethod.value,
        timeoutMs = call.request.headers[CONNECT_TIMEOUT_HEADER]?.toLongOrNull(),
    )

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
                ENVELOPE_FLAG_END_STREAM,
                endStreamJsonPayload(exception, trailers),
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

// Connect streaming: response headers go into HTTP headers; trailers go into
// the EndStream envelope's metadata field, written by the caller.
private fun writeStreamResponseHeaders(call: ApplicationCall, ctx: HandlerContext) {
    for ((name, values) in ctx.responseHeaders) {
        for (v in values) call.response.headers.append(name, v, safeOnly = false)
    }
}

private fun io.ktor.http.Headers.toMap(): Map<String, List<String>> =
    entries().associate { it.key to it.value }
