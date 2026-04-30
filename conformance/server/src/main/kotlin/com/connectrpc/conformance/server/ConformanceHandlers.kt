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

package com.connectrpc.conformance.server

import com.connectrpc.AnyError
import com.connectrpc.Code
import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ConnectException
import com.connectrpc.ErrorDetailParser
import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.conformance.v1.ConformancePayload
import com.connectrpc.conformance.v1.Header
import com.connectrpc.conformance.v1.IdempotentUnaryRequest
import com.connectrpc.conformance.v1.IdempotentUnaryResponse
import com.connectrpc.conformance.v1.UnaryRequest
import com.connectrpc.conformance.v1.UnaryResponse
import com.connectrpc.conformance.v1.UnaryResponseDefinition
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.UnaryHandler
import com.google.protobuf.Any
import com.google.protobuf.Message
import kotlinx.coroutines.delay
import okio.ByteString.Companion.toByteString

internal class ConformanceUnaryHandler : UnaryHandler<UnaryRequest, UnaryResponse> {
    override val methodSpec = MethodSpec(
        "$SERVICE_PATH/Unary",
        UnaryRequest::class,
        UnaryResponse::class,
        StreamType.UNARY,
    )

    override suspend fun handle(request: UnaryRequest, ctx: HandlerContext): UnaryResponse {
        val def = if (request.hasResponseDefinition()) request.responseDefinition else null
        val payload = handleUnary(def, ctx, listOf(packAny(request)))
        return UnaryResponse.newBuilder().setPayload(payload).build()
    }
}

internal class ConformanceIdempotentUnaryHandler :
    UnaryHandler<IdempotentUnaryRequest, IdempotentUnaryResponse> {
    override val methodSpec = MethodSpec(
        "$SERVICE_PATH/IdempotentUnary",
        IdempotentUnaryRequest::class,
        IdempotentUnaryResponse::class,
        StreamType.UNARY,
        Idempotency.NO_SIDE_EFFECTS,
    )

    override suspend fun handle(
        request: IdempotentUnaryRequest,
        ctx: HandlerContext,
    ): IdempotentUnaryResponse {
        val def = if (request.hasResponseDefinition()) request.responseDefinition else null
        val payload = handleUnary(def, ctx, listOf(packAny(request)))
        return IdempotentUnaryResponse.newBuilder().setPayload(payload).build()
    }
}

private suspend fun handleUnary(
    def: UnaryResponseDefinition?,
    ctx: HandlerContext,
    requests: List<Any>,
): ConformancePayload {
    val requestInfo = buildRequestInfo(ctx, requests)
    val payloadBuilder = ConformancePayload.newBuilder().setRequestInfo(requestInfo)
    // Capture for use inside the error branch below; Kotlin can't reference
    // payloadBuilder.requestInfo here without rebuilding it.
    val capturedRequestInfo = requestInfo

    if (def == null) return payloadBuilder.build()

    for (h in def.responseHeadersList) {
        ctx.responseHeaders.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
    }
    for (h in def.responseTrailersList) {
        ctx.responseTrailers.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
    }

    if (def.responseDelayMs > 0) {
        delay(def.responseDelayMs.toLong())
    }

    if (def.hasError()) {
        val err = def.error
        val code = Code.fromValue(err.code.number) ?: Code.UNKNOWN
        val message = if (err.hasMessage()) err.message else null
        // Per the Connect spec, when the server is asked to fail before producing
        // a response, it should include a RequestInfo detail with what it observed
        // about the request. Plus any details defined in the request itself.
        val details = mutableListOf<ConnectErrorDetail>()
        details += packAny(capturedRequestInfo).toConnectErrorDetail()
        for (d in err.detailsList) {
            details += d.toConnectErrorDetail()
        }
        throw ConnectException(code = code, message = message)
            .withErrorDetails(NoopErrorDetailParser, details)
    }

    return payloadBuilder.setData(def.responseData).build()
}

private fun buildRequestInfo(
    ctx: HandlerContext,
    requests: List<Any>,
): ConformancePayload.RequestInfo {
    val builder = ConformancePayload.RequestInfo.newBuilder().addAllRequests(requests)
    for ((name, values) in ctx.requestHeaders) {
        builder.addRequestHeaders(Header.newBuilder().setName(name).addAllValue(values))
    }
    val t = ctx.timeoutMs
    if (t != null) {
        builder.timeoutMs = t
    }
    return builder.build()
}

private fun packAny(message: Message): Any = Any.pack(message)

private fun Any.toConnectErrorDetail(): ConnectErrorDetail =
    ConnectErrorDetail(
        type = typeUrl,
        payload = value.toByteArray().toByteString(),
    )

// Server-side never unpacks details, only writes them. This stub satisfies
// ConnectException.withErrorDetails(parser, ...) without dragging the real
// JavaErrorParser into module visibility.
private object NoopErrorDetailParser : ErrorDetailParser {
    override fun <E : kotlin.Any> unpack(any: AnyError, clazz: kotlin.reflect.KClass<E>): E? = null
    override fun parseDetails(bytes: ByteArray): List<ConnectErrorDetail> = emptyList()
}
