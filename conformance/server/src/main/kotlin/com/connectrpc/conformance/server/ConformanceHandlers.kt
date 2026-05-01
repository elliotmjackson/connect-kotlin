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
import com.connectrpc.conformance.v1.BidiStreamRequest
import com.connectrpc.conformance.v1.BidiStreamResponse
import com.connectrpc.conformance.v1.ClientStreamRequest
import com.connectrpc.conformance.v1.ClientStreamResponse
import com.connectrpc.conformance.v1.ConformancePayload
import com.connectrpc.conformance.v1.ConformanceServiceHandler
import com.connectrpc.conformance.v1.Header
import com.connectrpc.conformance.v1.IdempotentUnaryRequest
import com.connectrpc.conformance.v1.IdempotentUnaryResponse
import com.connectrpc.conformance.v1.ServerStreamRequest
import com.connectrpc.conformance.v1.ServerStreamResponse
import com.connectrpc.conformance.v1.StreamResponseDefinition
import com.connectrpc.conformance.v1.UnaryRequest
import com.connectrpc.conformance.v1.UnaryResponse
import com.connectrpc.conformance.v1.UnaryResponseDefinition
import com.connectrpc.conformance.v1.UnimplementedRequest
import com.connectrpc.conformance.v1.UnimplementedResponse
import com.connectrpc.server.BidiStream
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.ServerMessageStream
import com.google.protobuf.Any
import com.google.protobuf.Message
import kotlinx.coroutines.delay
import okio.ByteString.Companion.toByteString

/**
 * Conformance service implementation, written against the generated
 * [ConformanceServiceHandler] abstract base from protoc-gen-connect-kotlin.
 *
 * Override one method per RPC; the framework wires each into the appropriate
 * `Handler` via the inherited `handlers()` helper.
 */
class ConformanceServiceImpl : ConformanceServiceHandler() {
    override suspend fun unary(request: UnaryRequest, ctx: HandlerContext): UnaryResponse {
        val def = if (request.hasResponseDefinition()) request.responseDefinition else null
        val payload = handleUnary(def, ctx, listOf(packAny(request)))
        return UnaryResponse.newBuilder().setPayload(payload).build()
    }

    override suspend fun idempotentUnary(
        request: IdempotentUnaryRequest,
        ctx: HandlerContext,
    ): IdempotentUnaryResponse {
        val def = if (request.hasResponseDefinition()) request.responseDefinition else null
        val payload = handleUnary(def, ctx, listOf(packAny(request)))
        return IdempotentUnaryResponse.newBuilder().setPayload(payload).build()
    }

    override suspend fun unimplemented(
        request: UnimplementedRequest,
        ctx: HandlerContext,
    ): UnimplementedResponse {
        throw ConnectException(Code.UNIMPLEMENTED, "Unimplemented is not implemented")
    }

    override suspend fun serverStream(
        request: ServerStreamRequest,
        ctx: HandlerContext,
        stream: ServerMessageStream<ServerStreamResponse>,
    ) {
        val def = if (request.hasResponseDefinition()) request.responseDefinition else null
        val requestInfo = buildRequestInfo(ctx, listOf(packAny(request)))

        if (def == null) return

        for (h in def.responseHeadersList) {
            ctx.responseHeaders.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
        }
        for (h in def.responseTrailersList) {
            ctx.responseTrailers.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
        }

        for ((index, data) in def.responseDataList.withIndex()) {
            if (def.responseDelayMs > 0) {
                delay(def.responseDelayMs.toLong())
            }
            val payloadBuilder = ConformancePayload.newBuilder().setData(data)
            if (index == 0) {
                payloadBuilder.setRequestInfo(requestInfo)
            }
            stream.send(
                ServerStreamResponse.newBuilder().setPayload(payloadBuilder.build()).build(),
            )
        }

        if (def.hasError()) {
            val err = def.error
            val code = Code.fromValue(err.code.number) ?: Code.UNKNOWN
            val message = if (err.hasMessage()) err.message else null
            val details = mutableListOf<ConnectErrorDetail>()
            if (def.responseDataList.isEmpty()) {
                details += packAny(requestInfo).toConnectErrorDetail()
            }
            for (d in err.detailsList) {
                details += d.toConnectErrorDetail()
            }
            throw ConnectException(code = code, message = message)
                .withErrorDetails(NoopErrorDetailParser, details)
        }
    }

    override suspend fun clientStream(
        stream: ClientMessageStream<ClientStreamRequest>,
        ctx: HandlerContext,
    ): ClientStreamResponse {
        val received = mutableListOf<ClientStreamRequest>()
        var def: UnaryResponseDefinition? = null
        while (true) {
            val msg = stream.receive() ?: break
            if (received.isEmpty() && msg.hasResponseDefinition()) {
                def = msg.responseDefinition
            }
            received += msg
        }

        val requestInfo = buildRequestInfo(ctx, received.map { packAny(it) })
        val payloadBuilder = ConformancePayload.newBuilder().setRequestInfo(requestInfo)

        if (def == null) {
            return ClientStreamResponse.newBuilder().setPayload(payloadBuilder.build()).build()
        }

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
            val details = mutableListOf<ConnectErrorDetail>()
            details += packAny(requestInfo).toConnectErrorDetail()
            for (d in err.detailsList) {
                details += d.toConnectErrorDetail()
            }
            throw ConnectException(code = code, message = message)
                .withErrorDetails(NoopErrorDetailParser, details)
        }

        return ClientStreamResponse.newBuilder()
            .setPayload(payloadBuilder.setData(def.responseData).build())
            .build()
    }

    override suspend fun bidiStream(
        stream: BidiStream<BidiStreamRequest, BidiStreamResponse>,
        ctx: HandlerContext,
    ) {
        val first = stream.receive() ?: return
        val def: StreamResponseDefinition? =
            if (first.hasResponseDefinition()) first.responseDefinition else null
        val fullDuplex = first.fullDuplex

        if (def != null) {
            for (h in def.responseHeadersList) {
                ctx.responseHeaders.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
            }
            for (h in def.responseTrailersList) {
                ctx.responseTrailers.getOrPut(h.name) { mutableListOf() }.addAll(h.valueList)
            }
        }

        if (fullDuplex) {
            handleBidiFullDuplex(stream, ctx, def, first)
        } else {
            handleBidiHalfDuplex(stream, ctx, def, first)
        }
    }

    private suspend fun handleBidiHalfDuplex(
        stream: BidiStream<BidiStreamRequest, BidiStreamResponse>,
        ctx: HandlerContext,
        def: StreamResponseDefinition?,
        first: BidiStreamRequest,
    ) {
        val received = mutableListOf(first)
        while (true) {
            val msg = stream.receive() ?: break
            received += msg
        }
        val requestInfo = buildRequestInfo(ctx, received.map { packAny(it) })

        if (def == null) return

        for ((index, data) in def.responseDataList.withIndex()) {
            if (def.responseDelayMs > 0) {
                delay(def.responseDelayMs.toLong())
            }
            val payloadBuilder = ConformancePayload.newBuilder().setData(data)
            if (index == 0) payloadBuilder.setRequestInfo(requestInfo)
            stream.send(
                BidiStreamResponse.newBuilder().setPayload(payloadBuilder.build()).build(),
            )
        }

        if (def.hasError()) {
            throwResponseDefError(def, requestInfo, hadResponses = def.responseDataList.isNotEmpty())
        }
    }

    private suspend fun handleBidiFullDuplex(
        stream: BidiStream<BidiStreamRequest, BidiStreamResponse>,
        ctx: HandlerContext,
        def: StreamResponseDefinition?,
        first: BidiStreamRequest,
    ) {
        val pending = mutableListOf<BidiStreamRequest>(first)
        var respIdx = 0
        var firstResponse = true
        var lastRequestInfo: ConformancePayload.RequestInfo? = null
        while (true) {
            if (def == null || respIdx >= def.responseDataList.size) {
                if (def?.hasError() == true) {
                    val info = lastRequestInfo
                        ?: buildRequestInfo(ctx, pending.map { packAny(it) })
                    throwResponseDefError(def, info, hadResponses = respIdx > 0)
                }
                while (stream.receive() != null) {
                    // discard
                }
                break
            }
            if (def.responseDelayMs > 0) {
                delay(def.responseDelayMs.toLong())
            }
            val info = buildRequestInfo(ctx, pending.map { packAny(it) })
            lastRequestInfo = info
            val payloadBuilder = ConformancePayload.newBuilder()
                .setData(def.responseDataList[respIdx])
            if (firstResponse) {
                payloadBuilder.setRequestInfo(info)
                firstResponse = false
            } else {
                payloadBuilder.setRequestInfo(
                    ConformancePayload.RequestInfo.newBuilder()
                        .addAllRequests(pending.map { packAny(it) })
                        .build(),
                )
            }
            stream.send(
                BidiStreamResponse.newBuilder().setPayload(payloadBuilder.build()).build(),
            )
            respIdx++
            pending.clear()
            val next = stream.receive() ?: break
            pending += next
        }

        if (def?.hasError() == true) {
            val info = lastRequestInfo
                ?: buildRequestInfo(ctx, pending.map { packAny(it) })
            throwResponseDefError(def, info, hadResponses = respIdx > 0)
        }
    }

    private fun throwResponseDefError(
        def: StreamResponseDefinition,
        requestInfo: ConformancePayload.RequestInfo,
        hadResponses: Boolean,
    ) {
        val err = def.error
        val code = Code.fromValue(err.code.number) ?: Code.UNKNOWN
        val message = if (err.hasMessage()) err.message else null
        val details = mutableListOf<ConnectErrorDetail>()
        if (!hadResponses) {
            details += packAny(requestInfo).toConnectErrorDetail()
        }
        for (d in err.detailsList) {
            details += d.toConnectErrorDetail()
        }
        throw ConnectException(code = code, message = message)
            .withErrorDetails(NoopErrorDetailParser, details)
    }
}

private suspend fun handleUnary(
    def: UnaryResponseDefinition?,
    ctx: HandlerContext,
    requests: List<Any>,
): ConformancePayload {
    val requestInfo = buildRequestInfo(ctx, requests)
    val payloadBuilder = ConformancePayload.newBuilder().setRequestInfo(requestInfo)
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
    val qp = ctx.queryParams
    if (qp != null) {
        val getInfo = ConformancePayload.ConnectGetInfo.newBuilder()
        for ((name, values) in qp) {
            getInfo.addQueryParams(Header.newBuilder().setName(name).addAllValue(values))
        }
        builder.connectGetInfo = getInfo.build()
    }
    return builder.build()
}

private fun packAny(message: Message): Any = Any.pack(message)

private fun Any.toConnectErrorDetail(): ConnectErrorDetail =
    ConnectErrorDetail(
        type = typeUrl,
        payload = value.toByteArray().toByteString(),
    )

internal object NoopErrorDetailParser : ErrorDetailParser {
    override fun <E : kotlin.Any> unpack(any: AnyError, clazz: kotlin.reflect.KClass<E>): E? = null
    override fun parseDetails(bytes: ByteArray): List<ConnectErrorDetail> = emptyList()
}
