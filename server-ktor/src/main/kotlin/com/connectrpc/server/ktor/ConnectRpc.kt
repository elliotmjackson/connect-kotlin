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
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import com.connectrpc.server.protocol.CONNECT_ERROR_CONTENT_TYPE
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.connectErrorJsonBody
import com.connectrpc.server.protocol.connectHttpStatus
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.contentType
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respondBytes
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.toByteArray
import okio.Buffer

private const val CONNECT_TIMEOUT_HEADER = "Connect-Timeout-Ms"

/**
 * Mounts a Connect/gRPC/gRPC-Web server into a Ktor [Application].
 *
 * Milestone-2.1 scope: Connect unary success + error paths with response
 * headers, trailers, and timeout echo. Streaming and gRPC/gRPC-Web still 415.
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
    val codecName = connectUnaryCodecForContentType(contentType)
    if (codecName == null) {
        call.respondBytes(bytes = ByteArray(0), status = HttpStatusCode.UnsupportedMediaType)
        return
    }
    val codec = registry.codec(codecName)
    if (codec == null) {
        respondConnectError(
            call,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "codec $codecName is not registered"),
        )
        return
    }
    handleConnectUnary(call, registry, procedure, codec, contentType)
}

private fun connectUnaryCodecForContentType(contentType: String): String? = when (contentType) {
    CONNECT_UNARY_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO
    CONNECT_UNARY_CONTENT_TYPE_JSON -> CODEC_NAME_JSON
    else -> null
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
        respondConnectError(
            call,
            ctx = null,
            ConnectException(Code.UNIMPLEMENTED, "$procedure is not implemented"),
        )
        return
    }
    @Suppress("UNCHECKED_CAST")
    val unary = handler as UnaryHandler<Any, Any>

    val ctx = HandlerContext(
        procedure = procedure,
        requestHeaders = call.request.headers.toMap(),
        httpMethod = call.request.httpMethod.value,
        timeoutMs = call.request.headers[CONNECT_TIMEOUT_HEADER]?.toLongOrNull(),
    )

    val requestBytes = call.receiveChannel().toByteArray()
    val request = try {
        codec.codec(unary.methodSpec.requestClass).deserialize(Buffer().write(requestBytes))
    } catch (ex: Exception) {
        respondConnectError(
            call,
            ctx,
            ConnectException(Code.INVALID_ARGUMENT, "could not decode request: ${ex.message}"),
        )
        return
    }

    val response = try {
        unary.handle(request, ctx)
    } catch (ex: ConnectException) {
        respondConnectError(call, ctx, ex)
        return
    }

    writeContextHeaders(call, ctx)
    val responseBytes = codec.codec(unary.methodSpec.responseClass).serialize(response).readByteArray()
    call.respondBytes(
        bytes = responseBytes,
        contentType = ContentType.parse(requestContentType),
        status = HttpStatusCode.OK,
    )
}

private suspend fun respondConnectError(
    call: ApplicationCall,
    ctx: HandlerContext?,
    exception: ConnectException,
) {
    if (ctx != null) writeContextHeaders(call, ctx)
    call.respondBytes(
        bytes = connectErrorJsonBody(exception),
        contentType = ContentType.parse(CONNECT_ERROR_CONTENT_TYPE),
        status = HttpStatusCode.fromValue(exception.code.connectHttpStatus()),
    )
}

// Connect unary protocol: response headers go straight into HTTP headers;
// trailers go into HTTP headers prefixed with "trailer-".
private fun writeContextHeaders(call: ApplicationCall, ctx: HandlerContext) {
    for ((name, values) in ctx.responseHeaders) {
        for (v in values) call.response.headers.append(name, v, safeOnly = false)
    }
    for ((name, values) in ctx.responseTrailers) {
        for (v in values) call.response.headers.append("trailer-$name", v, safeOnly = false)
    }
}

private fun io.ktor.http.Headers.toMap(): Map<String, List<String>> =
    entries().associate { it.key to it.value }
