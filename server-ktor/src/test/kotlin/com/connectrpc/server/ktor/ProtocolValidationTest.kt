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

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Ignore
import org.junit.Test

/**
 * TDD targets for protocol-level request validation that the conformance
 * suite (in our current configuration) doesn't exercise. Each test asserts
 * that the server rejects a request that violates a spec-required header
 * convention.
 */
class ProtocolValidationTest {

    /**
     * Connect spec: `Connect-Protocol-Version: 1` is recommended on POST
     * requests but not required for backwards compatibility. Servers can opt
     * in to enforcement via [connectRpc]'s `requireConnectProtocolHeader`
     * flag. With it enabled, a request missing the header is rejected with
     * INVALID_ARGUMENT.
     */
    @Test
    fun connectRejectsMissingProtocolVersion() {
        val handler = unaryHandler { _, _ -> TestMessage("ok") }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry, requireConnectProtocolHeader = true).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/proto")
                // intentionally NO Connect-Protocol-Version header
                .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                // Spec: server should reject. Exact code is implementation-
                // defined but should be a 4xx with a Connect-formatted error.
                assertThat(response.isSuccessful)
                    .describedAs("missing Connect-Protocol-Version should be rejected")
                    .isFalse()
            }
        }
    }

    /**
     * gRPC spec requires `TE: trailers` on every gRPC request. Servers
     * should reject requests missing this header to catch
     * client-misconfiguration early.
     *
     * Currently fails: we accept gRPC requests regardless of the TE header.
     */
    @Test
    fun grpcRejectsMissingTeTrailers() {
        val handler = unaryHandler { _, _ -> TestMessage("ok") }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/grpc+proto")
                // intentionally NO TE: trailers
                .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
                .build()

            newTestClient().newCall(req).execute().use { response ->
                val trailerStatus = response.trailers()["grpc-status"]
                val combinedHeaderStatus = response.header("grpc-status")
                val status = trailerStatus ?: combinedHeaderStatus
                assertThat(status)
                    .describedAs("server must surface a non-zero grpc-status when TE: trailers is missing")
                    .isNotNull()
                    .isNotEqualTo("0")
            }
        }
    }

    /**
     * HTTP/2 cleartext (h2c) prior knowledge should be supported, since
     * gRPC commonly runs over h2c on private networks. Ktor Netty's
     * `enableH2c = true` only handles the HTTP/1.1 Upgrade dance, not
     * prior-knowledge h2c (which is what OkHttp's H2_PRIOR_KNOWLEDGE and
     * most gRPC clients send). To make this pass we need to customize
     * Ktor's Netty channel pipeline to install Netty's
     * `CleartextHttp2ServerUpgradeHandler`, or switch to a different HTTP
     * server library entirely.
     */
    @Test
    @Ignore("TDD target: Ktor enableH2c doesn't support prior-knowledge; needs custom Netty pipeline")
    fun h2cSupported() {
        val handler = unaryHandler { _, _ -> TestMessage("ok") }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry, withH2c = true).use { server ->
            val req = Request.Builder()
                .url("${server.baseUrl}/test.v1.TestService/Unary")
                .header("Content-Type", "application/grpc+proto")
                .header("TE", "trailers")
                .post(envelope(0, ByteArray(0)).toRequestBody("application/grpc+proto".toMediaType()))
                .build()

            newTestClient(h2cPriorKnowledge = true).newCall(req).execute().use { response ->
                assertThat(response.protocol.toString())
                    .describedAs("connection must be HTTP/2")
                    .contains("h2")
                assertThat(response.isSuccessful).isTrue()
            }
        }
    }
}

private fun unaryHandler(
    body: suspend (TestMessage, HandlerContext) -> TestMessage,
): UnaryHandler<TestMessage, TestMessage> = object : UnaryHandler<TestMessage, TestMessage> {
    override val methodSpec = MethodSpec(
        "test.v1.TestService/Unary",
        TestMessage::class,
        TestMessage::class,
        StreamType.UNARY,
    )

    override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage =
        body(request, ctx)
}
