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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.conformance.v1.BidiStreamRequest
import com.connectrpc.conformance.v1.BidiStreamResponse
import com.connectrpc.conformance.v1.ClientStreamRequest
import com.connectrpc.conformance.v1.ClientStreamResponse
import com.connectrpc.conformance.v1.HTTPVersion
import com.connectrpc.conformance.v1.IdempotentUnaryRequest
import com.connectrpc.conformance.v1.IdempotentUnaryResponse
import com.connectrpc.conformance.v1.ServerCompatRequest
import com.connectrpc.conformance.v1.ServerCompatResponse
import com.connectrpc.conformance.v1.ServerStreamRequest
import com.connectrpc.conformance.v1.ServerStreamResponse
import com.connectrpc.conformance.v1.UnaryRequest
import com.connectrpc.conformance.v1.UnaryResponse
import com.connectrpc.conformance.v1.UnimplementedRequest
import com.connectrpc.conformance.v1.UnimplementedResponse
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.server.ktor.connectRpc
import com.google.protobuf.ByteString
import io.ktor.server.engine.applicationEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import kotlin.system.exitProcess

internal const val SERVICE_PATH = "connectrpc.conformance.v1.ConformanceService"

fun main(args: Array<String>) {
    runBlockingMain(args, System.`in`, System.out)
}

internal fun runBlockingMain(
    @Suppress("UNUSED_PARAMETER") args: Array<String>,
    input: InputStream,
    output: OutputStream,
) {
    val request = readServerCompatRequest(input)
    val wantHttp2 = when (request.httpVersion) {
        HTTPVersion.HTTP_VERSION_1 -> false
        HTTPVersion.HTTP_VERSION_2 -> true
        else -> {
            System.err.println("server only supports HTTP/1.1 and HTTP/2, got ${request.httpVersion}")
            exitProcess(1)
        }
    }

    val tlsCertPem: ByteArray? = if (request.useTls) {
        val creds = request.serverCreds
        if (creds.cert.isEmpty || creds.key.isEmpty) {
            System.err.println("use_tls=true but server_creds is missing cert/key")
            exitProcess(1)
        }
        creds.cert.toByteArray()
    } else {
        null
    }

    val registry = buildConformanceRegistry()

    val server = if (request.useTls) {
        val keyStore = buildServerKeyStore(
            request.serverCreds.cert.toByteArray(),
            request.serverCreds.key.toByteArray(),
        )
        embeddedServer(
            factory = Netty,
            environment = applicationEnvironment { },
            configure = {
                sslConnector(
                    keyStore = keyStore,
                    keyAlias = TLS_KEY_ALIAS,
                    keyStorePassword = { TLS_KEY_PASSWORD },
                    privateKeyPassword = { TLS_KEY_PASSWORD },
                ) {
                    host = "127.0.0.1"
                    port = 0
                }
                enableHttp2 = wantHttp2
                enableH2c = false
            },
            module = {
                connectRpc(registry)
            },
        )
    } else {
        embeddedServer(Netty, port = 0, host = "127.0.0.1") {
            connectRpc(registry)
        }
    }
    server.start(wait = false)

    val port = kotlinx.coroutines.runBlocking {
        server.engine.resolvedConnectors().first().port
    }

    val responseBuilder = ServerCompatResponse.newBuilder()
        .setHost("127.0.0.1")
        .setPort(port)
    if (tlsCertPem != null) {
        responseBuilder.pemCert = ByteString.copyFrom(tlsCertPem)
    }
    writeServerCompatResponse(output, responseBuilder.build())

    Runtime.getRuntime().addShutdownHook(
        Thread {
            server.stop(500, 1000)
        },
    )

    // Block forever; harness will SIGTERM us.
    Thread.currentThread().join()
}

private fun buildConformanceRegistry(): HandlerRegistry =
    HandlerRegistry.builder()
        .codec(GoogleJavaProtobufStrategy())
        .codec(GoogleJavaJSONStrategy())
        .register(ConformanceUnaryHandler())
        .register(ConformanceServerStreamHandler())
        .register(ConformanceClientStreamHandler())
        .register(ConformanceBidiStreamHandler())
        .register(
            unimplemented(
                "$SERVICE_PATH/Unimplemented",
                StreamType.UNARY,
                UnimplementedRequest::class,
                UnimplementedResponse::class,
            ),
        )
        .register(ConformanceIdempotentUnaryHandler())
        .build()

private fun <Req : Any, Res : Any> unimplemented(
    path: String,
    streamType: StreamType,
    reqClass: kotlin.reflect.KClass<Req>,
    resClass: kotlin.reflect.KClass<Res>,
): UnaryHandler<Req, Res> = object : UnaryHandler<Req, Res> {
    override val methodSpec = MethodSpec(path, reqClass, resClass, streamType)
    override suspend fun handle(request: Req, ctx: HandlerContext): Res =
        throw ConnectException(Code.UNIMPLEMENTED, "$path is not implemented")
}

private fun <Req : Any, Res : Any> unimplementedServerStream(
    path: String,
    reqClass: kotlin.reflect.KClass<Req>,
    resClass: kotlin.reflect.KClass<Res>,
): ServerStreamHandler<Req, Res> = object : ServerStreamHandler<Req, Res> {
    override val methodSpec = MethodSpec(path, reqClass, resClass, StreamType.SERVER)
    override suspend fun handle(
        request: Req,
        ctx: HandlerContext,
        stream: ServerMessageStream<Res>,
    ) = throw ConnectException(Code.UNIMPLEMENTED, "$path is not implemented")
}

private fun <Req : Any, Res : Any> unimplementedClientStream(
    path: String,
    reqClass: kotlin.reflect.KClass<Req>,
    resClass: kotlin.reflect.KClass<Res>,
): ClientStreamHandler<Req, Res> = object : ClientStreamHandler<Req, Res> {
    override val methodSpec = MethodSpec(path, reqClass, resClass, StreamType.CLIENT)
    override suspend fun handle(
        stream: ClientMessageStream<Req>,
        ctx: HandlerContext,
    ): Res = throw ConnectException(Code.UNIMPLEMENTED, "$path is not implemented")
}

private fun <Req : Any, Res : Any> unimplementedBidi(
    path: String,
    reqClass: kotlin.reflect.KClass<Req>,
    resClass: kotlin.reflect.KClass<Res>,
): BidiStreamHandler<Req, Res> = object : BidiStreamHandler<Req, Res> {
    override val methodSpec = MethodSpec(path, reqClass, resClass, StreamType.BIDI)
    override suspend fun handle(
        stream: BidiStream<Req, Res>,
        ctx: HandlerContext,
    ) = throw ConnectException(Code.UNIMPLEMENTED, "$path is not implemented")
}

private fun readServerCompatRequest(input: InputStream): ServerCompatRequest {
    val len = input.readBigEndianInt()
        ?: throw EOFException("EOF before reading ServerCompatRequest length prefix")
    val bytes = input.readN(len)
        ?: throw EOFException("EOF after $len-byte length prefix")
    return ServerCompatRequest.parseFrom(bytes)
}

private fun writeServerCompatResponse(output: OutputStream, response: ServerCompatResponse) {
    val bytes = response.toByteArray()
    output.write(intToBigEndianBytes(bytes.size))
    output.write(bytes)
    output.flush()
}

private fun InputStream.readN(len: Int): ByteArray? {
    val bytes = ByteArray(len)
    var off = 0
    var remain = len
    while (remain > 0) {
        val n = read(bytes, off, remain)
        if (n <= 0) {
            return if (off == 0) null else throw EOFException("read $off of $len bytes")
        }
        off += n
        remain -= n
    }
    return bytes
}

private fun InputStream.readBigEndianInt(): Int? {
    val bytes = readN(4) ?: return null
    return (bytes[0].toInt() and 0xff shl 24) or
        (bytes[1].toInt() and 0xff shl 16) or
        (bytes[2].toInt() and 0xff shl 8) or
        (bytes[3].toInt() and 0xff)
}

private fun intToBigEndianBytes(value: Int): ByteArray = byteArrayOf(
    (value ushr 24).toByte(),
    (value ushr 16).toByte(),
    (value ushr 8).toByte(),
    value.toByte(),
)
