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

package com.connectrpc.server

import com.connectrpc.AnyError
import com.connectrpc.CODEC_NAME_JSON
import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.Codec
import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ErrorDetailParser
import com.connectrpc.Headers
import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.server.compression.GzipServerCompressionPool
import com.connectrpc.server.http.HttpExchange
import com.connectrpc.server.http.ResponseSink
import com.connectrpc.server.http.StreamingBody
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import okio.Buffer
import okio.BufferedSource
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KClass

/** Opaque test message: the codec writes and reads its bytes verbatim, and fails on text starting with `!`. */
data class Msg(val text: String)

class BytesStrategy(private val name: String) : SerializationStrategy {
    override fun serializationName(): String = name

    @Suppress("UNCHECKED_CAST")
    override fun <E : Any> codec(clazz: KClass<E>): Codec<E> = object : Codec<Msg> {
        override fun encodingName(): String = name
        override fun serialize(message: Msg): Buffer {
            require(!message.text.startsWith("!")) { "bad message" }
            return Buffer().writeUtf8(message.text)
        }
        override fun deterministicSerialize(message: Msg): Buffer = serialize(message)
        override fun deserialize(source: BufferedSource): Msg {
            val text = source.readUtf8()
            require(!text.startsWith("!")) { "bad message" }
            return Msg(text)
        }
    } as Codec<E>

    override fun errorDetailParser(): ErrorDetailParser = object : ErrorDetailParser {
        override fun <E : Any> unpack(any: AnyError, clazz: KClass<E>): E? = null
        override fun parseDetails(bytes: ByteArray): List<ConnectErrorDetail> = emptyList()
    }
}

const val SVC = "test.v1.TestService"

fun spec(method: String, type: StreamType, idempotency: Idempotency = Idempotency.UNKNOWN) = MethodSpec("$SVC/$method", Msg::class, Msg::class, type, idempotency)

fun unary(method: String = "Unary", idempotency: Idempotency = Idempotency.UNKNOWN, body: suspend (Msg, HandlerContext) -> Msg) = object : UnaryHandler<Msg, Msg> {
    override val methodSpec = spec(method, StreamType.UNARY, idempotency)
    override suspend fun handle(request: Msg, ctx: HandlerContext): Msg = body(request, ctx)
}

fun serverStream(method: String = "ServerStream", body: suspend (Msg, HandlerContext, ServerMessageStream<Msg>) -> Unit) = object : ServerStreamHandler<Msg, Msg> {
    override val methodSpec = spec(method, StreamType.SERVER)
    override suspend fun handle(request: Msg, ctx: HandlerContext, stream: ServerMessageStream<Msg>) = body(request, ctx, stream)
}

fun clientStream(method: String = "ClientStream", body: suspend (ClientMessageStream<Msg>, HandlerContext) -> Msg) = object : ClientStreamHandler<Msg, Msg> {
    override val methodSpec = spec(method, StreamType.CLIENT)
    override suspend fun handle(stream: ClientMessageStream<Msg>, ctx: HandlerContext): Msg = body(stream, ctx)
}

fun bidi(method: String = "Bidi", body: suspend (BidiStream<Msg, Msg>, HandlerContext) -> Unit) = object : BidiStreamHandler<Msg, Msg> {
    override val methodSpec = spec(method, StreamType.BIDI)
    override suspend fun handle(stream: BidiStream<Msg, Msg>, ctx: HandlerContext) = body(stream, ctx)
}

fun server(vararg handlers: Handler<*, *>, config: ServerConfig = ServerConfig(), configure: HandlerRegistry.Builder.() -> Unit = {}): ConnectServer {
    val builder = HandlerRegistry.builder().codec(BytesStrategy(CODEC_NAME_PROTO)).codec(BytesStrategy(CODEC_NAME_JSON))
    handlers.forEach { builder.register(it) }
    builder.configure()
    return ConnectServer(builder.build(), config)
}

/** Connect/gRPC envelope. */
fun env(flags: Int, payload: ByteArray): ByteArray = Buffer().writeByte(flags).writeInt(payload.size).write(payload).readByteArray()

fun env(flags: Int, text: String): ByteArray = env(flags, text.toByteArray())

fun gzip(bytes: ByteArray): ByteArray = GzipServerCompressionPool.compress(Buffer().write(bytes)).readByteArray()

fun gunzip(bytes: ByteArray): String = Buffer().also { out ->
    GzipServerCompressionPool.decompress(Buffer().write(bytes)).use { src -> while (src.read(out, 8192) != -1L) continue }
}.readUtf8()

class Frame(val flags: Int, val payload: ByteArray) {
    val text: String get() = payload.toString(Charsets.UTF_8)
}

fun frames(body: ByteArray): List<Frame> {
    val buffer = Buffer().write(body)
    val out = mutableListOf<Frame>()
    while (!buffer.exhausted()) {
        val flags = buffer.readByte().toInt() and 0xff
        val size = buffer.readInt().toLong()
        out += Frame(flags, buffer.readByteArray(size))
    }
    return out
}

/**
 * In-memory [HttpExchange]. The request body comes from [bodyChunks] (closed =
 * end of body); [writeGate], when set, must hand out one permit per
 * [ResponseSink.write] so tests can stall the "client".
 */
class FakeExchange(
    override val method: String = "POST",
    override val path: String,
    override val rawQuery: String? = null,
    override val requestHeaders: Headers = emptyMap(),
    override val supportsTrailers: Boolean = true,
    val bodyChunks: Channel<ByteArray> = Channel(Channel.UNLIMITED),
    val writeGate: Channel<Unit>? = null,
) : HttpExchange {
    var status: Int? = null
    var headers: Headers = emptyMap()
    val body = Buffer()
    var trailers: Headers = emptyMap()
    val bytesRead = AtomicLong()
    val writes = AtomicLong()
    private var pending = Buffer()

    val bodyBytes: ByteArray get() = body.snapshot().toByteArray()

    fun header(name: String): String? = headers[name]?.single()

    override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
        if (pending.exhausted()) {
            val next = bodyChunks.receiveCatching().getOrNull() ?: return -1
            pending.write(next)
        }
        val n = minOf(maxBytes, pending.size)
        sink.write(pending, n)
        bytesRead.addAndGet(n)
        return n
    }

    override suspend fun respond(status: Int, headers: Headers, body: ByteArray) {
        check(this.status == null) { "responded twice" }
        this.status = status
        this.headers = headers
        this.body.write(body)
    }

    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) {
        check(this.status == null) { "responded twice" }
        this.status = status
        this.headers = headers
        trailers = body.writeTo(
            object : ResponseSink {
                override suspend fun write(source: Buffer) {
                    writeGate?.receive()
                    writes.incrementAndGet()
                    this@FakeExchange.body.writeAll(source)
                }

                override suspend fun flush() {}
            },
        )
    }
}

/** Builds an exchange whose whole body is [body], serves it, returns the exchange. */
fun ConnectServer.call(
    path: String,
    contentType: String?,
    body: ByteArray = ByteArray(0),
    headers: Map<String, String> = emptyMap(),
    method: String = "POST",
    query: String? = null,
    supportsTrailers: Boolean = true,
): FakeExchange {
    val all = LinkedHashMap<String, List<String>>()
    if (contentType != null) all["Content-Type"] = listOf(contentType)
    headers.forEach { (k, v) -> all[k] = listOf(v) }
    val exchange = FakeExchange(method, "/$SVC/$path", query, all, supportsTrailers)
    if (body.isNotEmpty()) exchange.bodyChunks.trySend(body)
    exchange.bodyChunks.close()
    runBlocking { serve(exchange) }
    return exchange
}
