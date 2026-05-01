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
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerInterceptor
import com.connectrpc.server.UnaryHandler
import com.connectrpc.server.protocol.CONNECT_ERROR_CONTENT_TYPE
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_JSON
import com.connectrpc.server.protocol.CONNECT_UNARY_CONTENT_TYPE_PROTO
import com.connectrpc.server.protocol.connectErrorJsonBody
import com.connectrpc.server.protocol.connectHttpStatus
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
private const val IDENTITY_ENCODING = "identity"
private const val CONNECT_PROTOCOL_VERSION_HEADER = "Connect-Protocol-Version"
private const val CONNECT_PROTOCOL_VERSION_VALUE = "1"

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
        if (registry.find(procedure) == null) {
            // Let the container's default 404 handling kick in for unknown routes.
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
        val codecName = when (contentType) {
            CONNECT_UNARY_CONTENT_TYPE_PROTO -> CODEC_NAME_PROTO
            CONNECT_UNARY_CONTENT_TYPE_JSON -> CODEC_NAME_JSON
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

        if (options.requireConnectProtocolHeader) {
            val v = req.getHeader(CONNECT_PROTOCOL_VERSION_HEADER)
            if (v != CONNECT_PROTOCOL_VERSION_VALUE) {
                writeUnaryConnectError(
                    resp,
                    ConnectException(
                        Code.INVALID_ARGUMENT,
                        if (v == null) "missing required header: $CONNECT_PROTOCOL_VERSION_HEADER"
                        else "unsupported $CONNECT_PROTOCOL_VERSION_HEADER: $v",
                    ),
                )
                return
            }
        }

        handleConnectUnary(req, resp, procedure, codec, codecName)
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

    private fun pickPool(acceptEncoding: String): CompressionPool? {
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

private fun <Req : Any, Res : Any> UnaryHandler<Req, Res>.wrapUnary(
    interceptors: List<ServerInterceptor>,
): UnaryHandler<Req, Res> {
    var current: UnaryHandler<Req, Res> = this
    for (interceptor in interceptors.asReversed()) {
        current = interceptor.wrapUnary(current)
    }
    return current
}
