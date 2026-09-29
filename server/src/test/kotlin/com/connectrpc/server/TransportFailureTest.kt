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
import com.connectrpc.server.http.ResponseSink
import com.connectrpc.server.http.StreamingBody
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import java.io.IOException

/**
 * A client that goes away mid-call: [ConnectServer.serve] returns normally
 * (HttpExchange documents IOException as the adapter's signal) and the
 * handler does not outlive the call.
 */
class TransportFailureTest {
    /** Delegates to [fake], failing the selected operation with an [IOException]. */
    private class Failing(
        private val fake: FakeExchange,
        private val failRespond: Boolean = false,
        private val failReadAfter: Int = -1,
        private val failWriteAfter: Int = -1,
    ) : HttpExchange by fake {
        private var reads = 0

        override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
            if (reads++ == failReadAfter) throw IOException("connection reset")
            return fake.readRequestBody(sink, maxBytes)
        }

        override suspend fun respond(status: Int, headers: Headers, body: ByteArray) {
            if (failRespond) throw IOException("broken pipe")
            fake.respond(status, headers, body)
        }

        override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) {
            var writes = 0
            fake.respondStreaming(status, headers) { sink ->
                body.writeTo(
                    object : ResponseSink {
                        override suspend fun write(source: Buffer) {
                            if (writes++ == failWriteAfter) throw IOException("broken pipe")
                            sink.write(source)
                        }

                        override suspend fun flush() = sink.flush()
                    },
                )
            }
        }
    }

    private fun exchange(procedure: String, contentType: String, vararg chunks: ByteArray, open: Boolean = false): FakeExchange {
        val ex = FakeExchange(path = "/$SVC/$procedure", requestHeaders = mapOf("content-type" to listOf(contentType)))
        chunks.forEach { ex.bodyChunks.trySend(it) }
        if (!open) ex.bodyChunks.close()
        return ex
    }

    @Test
    fun failedResponseWriteEndsServeNormally() {
        val server = server(unary { req, _ -> req })
        val fake = exchange("Unary", "application/proto", "a".toByteArray())
        runBlocking { server.serve(Failing(fake, failRespond = true)) }
        assertThat(fake.status).isNull()
    }

    /** The GET has-body probe reads the body too; its failure ends the call like any other read failure. */
    @Test
    fun failedGetBodyProbeEndsServeNormally() {
        val server = server(unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req })
        val fake = FakeExchange(method = "GET", path = "/$SVC/Get", rawQuery = "encoding=proto")
        runBlocking { server.serve(Failing(fake, failReadAfter = 0)) }
        assertThat(fake.status).isNull()
    }

    @Test
    fun failedRequestReadIsCanceledForTheHandler() {
        val seen = CompletableDeferred<Throwable>()
        val server = server(
            clientStream { s, _ ->
                try {
                    while (s.receive() != null) continue
                    Msg("done")
                } catch (e: ConnectException) {
                    seen.complete(e)
                    throw e
                }
            },
        )
        val fake = exchange("ClientStream", "application/connect+proto", env(0, "a"), open = true)
        runBlocking { server.serve(Failing(fake, failReadAfter = 1)) }
        assertThat(seen.isCompleted).isTrue()
        assertThat((runBlocking { seen.await() } as ConnectException).code).isEqualTo(Code.CANCELED)
    }

    @Test
    fun failedStreamWriteCancelsTheHandler() {
        for (protocol in Enveloped.values()) {
            val cancelled = CompletableDeferred<Throwable>()
            val server = server(
                serverStream { _, _, s ->
                    try {
                        s.send(Msg("1"))
                        s.send(Msg("2"))
                        awaitCancellation()
                    } catch (e: CancellationException) {
                        cancelled.complete(e)
                        throw e
                    }
                },
            )
            val fake = exchange("ServerStream", protocol.contentType, env(0, "a"))
            runBlocking { withTimeout(5_000) { server.serve(Failing(fake, failWriteAfter = 1)) } }
            assertThat(frames(fake.bodyBytes).map { it.text }).describedAs("$protocol").containsExactly("1")
            assertThat(cancelled.isCompleted).describedAs("$protocol").isTrue()
        }
    }

    /** Only [Exception]s become error responses; an [Error] from the handler reaches the adapter. */
    @Test
    fun handlerErrorsPropagate() {
        val server = server(unary { _, _ -> throw AssertionError("bug") }, serverStream { _, _, _ -> throw AssertionError("bug") })
        assertThatThrownBy { runBlocking { server.serve(exchange("Unary", "application/proto", "a".toByteArray())) } }
            .isInstanceOf(AssertionError::class.java)
        assertThatThrownBy { runBlocking { server.serve(exchange("ServerStream", "application/grpc", env(0, "a"))) } }
            .isInstanceOf(AssertionError::class.java)
    }
}
