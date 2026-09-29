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

package com.connectrpc.okhttp

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.ProtocolClientConfig
import com.connectrpc.eliza.v1.ElizaServiceClient
import com.connectrpc.eliza.v1.introduceRequest
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.impl.ProtocolClient
import com.connectrpc.protocols.NetworkProtocol
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import mockwebserver3.MockResponse
import mockwebserver3.junit4.MockWebServerRule
import okhttp3.OkHttpClient
import okhttp3.Protocol
import org.assertj.core.api.Assertions.assertThat
import org.junit.Rule
import org.junit.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Tests for OkHttp stream handling, specifically connection lifecycle.
 */
class OkHttpStreamTest {

    @get:Rule val mockWebServerRule = MockWebServerRule()

    // Regression test for https://github.com/connectrpc/connect-kotlin/issues/395
    // A non-200 streaming response must close the OkHttp response body.
    // Before the fix, the response body was not closed on non-200 responses,
    // causing OkHttp's connection leak detector to fire.
    @Test
    fun `non-200 streaming response completes and closes response body`() {
        mockWebServerRule.server.enqueue(
            MockResponse.Builder().apply {
                code(503)
                addHeader("content-type", "application/connect+proto")
            }.build(),
        )
        val exception = serverStreamError(NetworkProtocol.CONNECT)
        assertThat(exception).isNotNull()
        assertThat(exception!!.code).isEqualTo(Code.UNAVAILABLE)
    }

    // Verify that a 200 streaming response with an empty body also
    // completes cleanly.
    @Test
    fun `empty streaming response completes and closes response body`() {
        mockWebServerRule.server.enqueue(
            MockResponse.Builder().apply {
                code(200)
                addHeader("content-type", "application/connect+proto")
            }.build(),
        )
        serverStreamError(NetworkProtocol.CONNECT)
    }

    // protocol.md "HTTP to Error Code" and http-grpc-status-mapping.md both
    // map 403 to permission_denied when the response carries no RPC status.
    @Test
    fun `connect non-200 streaming response with foreign content-type infers code from status`() {
        assertForbiddenStreamIsPermissionDenied(NetworkProtocol.CONNECT)
    }

    @Test
    fun `grpc-web non-200 streaming response with foreign content-type infers code from status`() {
        assertForbiddenStreamIsPermissionDenied(NetworkProtocol.GRPC_WEB)
    }

    @Test
    fun `grpc non-200 streaming response with foreign content-type infers code from status`() {
        assertForbiddenStreamIsPermissionDenied(NetworkProtocol.GRPC)
    }

    private fun assertForbiddenStreamIsPermissionDenied(networkProtocol: NetworkProtocol) {
        mockWebServerRule.server.enqueue(
            MockResponse.Builder().apply {
                code(403)
                addHeader("content-type", "application/json")
                addHeader("x-denied-by", "filter")
                body("""{"error":"forbidden"}""")
            }.build(),
        )
        val exception = serverStreamError(networkProtocol)
        assertThat(exception).isNotNull()
        assertThat(exception!!.code).isEqualTo(Code.PERMISSION_DENIED)
        assertThat(exception.metadata["x-denied-by"]).containsExactly("filter")
    }

    /**
     * Runs a server-streaming call against the enqueued response and returns the
     * error it ends with, if any. MockWebServer serves HTTP/1.1 here, which OkHttp
     * cannot use for a duplex (bidi) call.
     */
    private fun serverStreamError(networkProtocol: NetworkProtocol): ConnectException? {
        val client = createStreamingClient(networkProtocol)
        return runBlocking {
            val stream = client.introduce()
            stream.sendAndClose(introduceRequest { name = "test" })
            val exception = withTimeout(10.seconds) {
                try {
                    for (msg in stream.responseChannel()) {
                        // Should not receive any messages.
                    }
                    null
                } catch (e: ConnectException) {
                    e
                }
            }
            stream.receiveClose()
            exception
        }
    }

    private fun createStreamingClient(networkProtocol: NetworkProtocol): ElizaServiceClient {
        val host = mockWebServerRule.server.url("/")
        val okHttpClient = OkHttpClient.Builder()
            .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
            .build()
        val protocolClient = ProtocolClient(
            ConnectOkHttpClient(okHttpClient),
            ProtocolClientConfig(
                host = host.toString(),
                serializationStrategy = GoogleJavaProtobufStrategy(),
                networkProtocol = networkProtocol,
                timeoutOracle = { null },
            ),
        )
        return ElizaServiceClient(protocolClient)
    }
}
