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

import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Idempotency
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.Handler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.ServerInterceptor
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import okio.Buffer
import java.util.logging.Level
import java.util.logging.Logger

internal val LOG: Logger = Logger.getLogger("com.connectrpc.server")

/** Wire protocol of a request, chosen from its Content-Type (multi-protocol.md). */
internal enum class Protocol(
    /** Request and response message compression header. */
    val encodingHeader: String,
    /** Header in which the client lists the encodings it accepts, and the server advertises its own. */
    val acceptEncodingHeader: String,
    /** Name reported to [com.connectrpc.server.ServerObserver], as connect-go names protocols (v1.20.0 `protocol.go:32-34`). */
    val protocolName: String,
) {
    CONNECT_UNARY("content-encoding", "accept-encoding", "connect"),
    CONNECT_STREAM("connect-content-encoding", "connect-accept-encoding", "connect"),
    GRPC("grpc-encoding", "grpc-accept-encoding", "grpc"),
    GRPC_WEB("grpc-encoding", "grpc-accept-encoding", "grpcweb"),
}

internal class Codec(val name: String, val strategy: SerializationStrategy)

/** One procedure: its interceptor-wrapped handler and the requests it accepts. */
internal class Route(
    val procedure: String,
    val handler: Handler<Any, Any>,
    codecs: Collection<SerializationStrategy>,
    // In registration order; the first is outermost.
    private val interceptors: List<ServerInterceptor>,
) {
    val streamType: StreamType = handler.methodSpec.streamType

    /** GET is only for unary procedures marked `NO_SIDE_EFFECTS` (protocol.md "Unary-Get-Request"). */
    val allowsGet: Boolean = streamType == StreamType.UNARY &&
        handler.methodSpec.idempotency == Idempotency.NO_SIDE_EFFECTS

    /** `Allow` value for 405 responses (connect-go `connecthttp/handler.go:58-63`). */
    val allow: String = if (allowsGet) "GET, POST" else "POST"

    /**
     * Canonical request content types this procedure accepts. Connect unary
     * types are only accepted by unary procedures and `connect+` types only
     * by streaming ones (protocol.md "Unary-Content-Type",
     * "Streaming-Content-Type").
     */
    val contentTypes: Map<String, Pair<Protocol, Codec>>

    /** Codecs by name, for Connect GET's `encoding` parameter. */
    val getCodecs: Map<String, Codec>

    /** `Accept-Post` value for 415 responses (connect-go `connecthttp/handler.go:75-78`). */
    val acceptPost: String

    /** The handler inside every interceptor's `wrap*`, outermost first. */
    private val wrapped: Handler<Any, Any> = wrap(handler, interceptors)

    init {
        val types = LinkedHashMap<String, Pair<Protocol, Codec>>()
        val kind = when (handler) {
            is UnaryHandler -> StreamType.UNARY
            is ServerStreamHandler -> StreamType.SERVER
            is ClientStreamHandler -> StreamType.CLIENT
            is BidiStreamHandler -> StreamType.BIDI
        }
        require(kind == streamType) { "$procedure: handler is $kind but its MethodSpec says $streamType" }
        val byName = LinkedHashMap<String, Codec>()
        for (strategy in codecs) {
            val codec = Codec(strategy.serializationName(), strategy)
            byName[codec.name] = codec
            if (streamType == StreamType.UNARY) {
                types["application/${codec.name}"] = Protocol.CONNECT_UNARY to codec
            } else {
                types["application/connect+${codec.name}"] = Protocol.CONNECT_STREAM to codec
            }
            types["application/grpc+${codec.name}"] = Protocol.GRPC to codec
            types["application/grpc-web+${codec.name}"] = Protocol.GRPC_WEB to codec
            // The bare gRPC types mean protobuf (PROTOCOL-HTTP2.md "Content-Type"; PROTOCOL-WEB.md).
            if (codec.name == CODEC_NAME_PROTO) {
                types["application/grpc"] = Protocol.GRPC to codec
                types["application/grpc-web"] = Protocol.GRPC_WEB to codec
            }
        }
        contentTypes = types
        getCodecs = byName
        acceptPost = types.keys.sorted().joinToString(", ")
    }

    fun decode(codec: Codec, payload: Buffer): Any = try {
        codec.strategy.codec(handler.methodSpec.requestClass).deserialize(payload)
    } catch (e: Exception) {
        LOG.log(Level.FINE, "could not unmarshal request for $procedure", e)
        throw ConnectException(Code.INVALID_ARGUMENT, "could not unmarshal ${codec.name} request message")
    }

    fun encode(codec: Codec, message: Any): Buffer = try {
        codec.strategy.codec(handler.methodSpec.responseClass).serialize(message)
    } catch (e: Exception) {
        LOG.log(Level.WARNING, "could not marshal response for $procedure", e)
        throw ConnectException(Code.INTERNAL_ERROR, "could not marshal response message")
    }

    /**
     * Runs a call of a unary procedure, the only kind Connect unary serves:
     * [request] reads and decodes the request message.
     */
    suspend fun callUnary(ctx: HandlerContext, request: suspend () -> Any): Any = intercept(ctx, 0) {
        (wrapped as UnaryHandler<Any, Any>).handle(request(), ctx)
    }

    /** Runs a call of any kind over enveloped message streams. */
    suspend fun call(requests: RequestStream, responses: ResponseStream, ctx: HandlerContext) = intercept(ctx, 0) {
        when (val h = wrapped) {
            is UnaryHandler<Any, Any> -> responses.send(h.handle(requests.receiveSingle(), ctx))
            is ServerStreamHandler<Any, Any> -> h.handle(requests.receiveSingle(), ctx, responses)
            is ClientStreamHandler<Any, Any> -> responses.send(h.handle(requests, ctx))
            is BidiStreamHandler<Any, Any> -> h.handle(Bidi(requests, responses), ctx)
        }
    }

    /** Runs [block] inside [ServerInterceptor.interceptCall] of the interceptors from index [from] on. */
    private suspend fun <T> intercept(ctx: HandlerContext, from: Int, block: suspend () -> T): T = if (from == interceptors.size) {
        block()
    } else {
        interceptors[from].interceptCall(ctx) { intercept(ctx, from + 1, block) }
    }
}

/**
 * Canonicalises a request Content-Type: type and subtype are lower-cased
 * (RFC 9110 §8.3.1: case-insensitive) and `charset=utf-8` is dropped. Any other
 * parameter makes the type unmatchable, so it is answered with 415
 * (connect-go `connecthttp/protocol.go:370-411`).
 */
internal fun canonicalContentType(value: String): String? {
    val parts = value.split(';')
    for (i in 1 until parts.size) {
        val param = parts[i].trim()
        if (param.isEmpty()) continue
        val key = param.substringBefore('=').trim()
        val paramValue = param.substringAfter('=', "").trim().removeSurrounding("\"")
        if (!key.equals("charset", ignoreCase = true) || !paramValue.equals("utf-8", ignoreCase = true)) return null
    }
    return parts[0].trim().lowercase()
}

internal interface RequestStream : ClientMessageStream<Any> {
    /** Exactly one request message; zero or several is `unimplemented` (protocol.md "Unary-Request"). */
    suspend fun receiveSingle(): Any
}

internal interface ResponseStream : ServerMessageStream<Any>

private class Bidi(
    requests: RequestStream,
    responses: ResponseStream,
) : BidiStream<Any, Any>,
    RequestStream by requests,
    ResponseStream by responses

private fun wrap(handler: Handler<Any, Any>, interceptors: List<ServerInterceptor>): Handler<Any, Any> {
    val outermostFirst = interceptors.asReversed()
    return when (handler) {
        is UnaryHandler<Any, Any> -> outermostFirst.fold<ServerInterceptor, UnaryHandler<Any, Any>>(handler) { next, i -> i.wrapUnary(next) }
        is ServerStreamHandler<Any, Any> -> outermostFirst.fold<ServerInterceptor, ServerStreamHandler<Any, Any>>(handler) { next, i -> i.wrapServerStream(next) }
        is ClientStreamHandler<Any, Any> -> outermostFirst.fold<ServerInterceptor, ClientStreamHandler<Any, Any>>(handler) { next, i -> i.wrapClientStream(next) }
        is BidiStreamHandler<Any, Any> -> outermostFirst.fold<ServerInterceptor, BidiStreamHandler<Any, Any>>(handler) { next, i -> i.wrapBidi(next) }
    }
}
