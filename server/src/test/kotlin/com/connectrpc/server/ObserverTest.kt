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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.Idempotency
import com.connectrpc.server.http.HttpExchange
import com.connectrpc.server.http.StreamingBody
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.IOException
import java.util.Collections

/** The observer sees every request once, including those that fail before a handler runs. */
class ObserverTest {
    private data class Observed(val procedure: String?, val protocol: String?, val trace: String?, val code: Code?)

    private val observed: MutableList<Observed> = Collections.synchronizedList(mutableListOf())

    private val observer = ServerObserver { procedure, protocol, headers ->
        ServerObserver.Call { code -> observed += Observed(procedure, protocol, headers["x-trace"]?.single(), code) }
    }

    private val server = server(
        unary { r, _ -> r },
        unary("Get", Idempotency.NO_SIDE_EFFECTS) { r, _ -> r },
        serverStream { r, _, s -> s.send(r) },
        bidi { _, _ -> throw ConnectException(Code.ABORTED) },
        config = ServerConfig(observer = observer),
    )

    @Test
    fun everyClassifiedRequestIsObserved() {
        val unary = "$SVC/Unary"
        data class Case(val name: String, val call: () -> Unit, val expected: Observed)
        val cases = listOf(
            Case("success", { server.call("Unary", "application/proto", "a".toByteArray(), mapOf("X-Trace" to "t1")) }, Observed(unary, "connect", "t1", null)),
            Case("unknown path", { server.call("Nope", "application/proto") }, Observed(null, null, null, Code.UNIMPLEMENTED)),
            Case("method not allowed", { server.call("Unary", null, method = "PUT") }, Observed(unary, null, null, Code.UNKNOWN)),
            Case("unsupported content type", { server.call("Unary", "text/plain") }, Observed(unary, null, null, Code.UNKNOWN)),
            Case("GET with a body", { server.call("Get", null, "a".toByteArray(), method = "GET", query = "encoding=proto&message=a") }, Observed("$SVC/Get", "connect", null, Code.UNKNOWN)),
            Case("bad GET query", { server.call("Get", null, method = "GET", query = "message=%zz") }, Observed("$SVC/Get", "connect", null, Code.INVALID_ARGUMENT)),
            Case("unsupported compression", { server.call("Unary", "application/grpc", env(0, "a"), mapOf("grpc-encoding" to "br")) }, Observed(unary, "grpc", null, Code.UNIMPLEMENTED)),
            Case("undecodable message", { server.call("Unary", "application/proto", "!bad".toByteArray()) }, Observed(unary, "connect", null, Code.INVALID_ARGUMENT)),
            Case("server stream", { server.callEnveloped(Enveloped.GRPC_WEB, "ServerStream", listOf("a")) }, Observed("$SVC/ServerStream", "grpcweb", null, null)),
            Case("handler error", { server.callEnveloped(Enveloped.CONNECT, "Bidi", emptyList()) }, Observed("$SVC/Bidi", "connect", null, Code.ABORTED)),
        )
        for (case in cases) {
            observed.clear()
            case.call()
            assertThat(observed).describedAs(case.name).containsExactly(case.expected)
        }
    }

    /** A response the transport could not send ends as `canceled`. */
    @Test
    fun transportFailureIsCanceled() {
        val fake = FakeExchange(path = "/$SVC/Unary", requestHeaders = mapOf("content-type" to listOf("application/proto")))
        fake.bodyChunks.trySend("a".toByteArray())
        fake.bodyChunks.close()
        val failing = object : HttpExchange by fake {
            override suspend fun respond(status: Int, headers: Headers, body: ByteArray) = throw IOException("broken pipe")
        }
        runBlocking { server.serve(failing) }
        assertThat(observed).containsExactly(Observed("$SVC/Unary", "connect", null, Code.CANCELED))
    }

    /**
     * The end of a response can cancel the call: Ktor on Netty cancels it once
     * the HTTP/2 stream has closed, which the response's END_STREAM does when
     * the request has ended. The response was handed over whole, so the call
     * keeps the code it sent.
     */
    @Test
    fun cancellationOnceTheResponseIsHandedOverKeepsItsCode() {
        val fake = FakeExchange(path = "/$SVC/ServerStream", requestHeaders = mapOf("content-type" to listOf(Enveloped.CONNECT.contentType)))
        fake.bodyChunks.trySend(env(0, "a"))
        fake.bodyChunks.close()
        runBlocking {
            launch {
                val call = coroutineContext.job
                val closing = object : HttpExchange by fake {
                    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) {
                        fake.respondStreaming(status, headers, body)
                        call.cancel()
                    }
                }
                server.serve(closing)
            }.join()
        }
        assertThat(Enveloped.CONNECT.outcome(fake).code).isNull()
        assertThat(observed).containsExactly(Observed("$SVC/ServerStream", "connect", null, null))
    }
}
