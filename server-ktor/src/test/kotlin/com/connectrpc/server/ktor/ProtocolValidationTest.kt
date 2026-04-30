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
     * Connect spec requires `Connect-Protocol-Version: 1` on POST requests.
     * Servers should reject requests missing this header (or with a different
     * version) with a Connect error.
     *
     * Currently fails: we accept requests regardless of the version header.
     */
    @Test
    @Ignore("TDD target: Connect-Protocol-Version header is not validated")
    fun connectRejectsMissingProtocolVersion() {
        val handler = unaryHandler { _, _ -> TestMessage("ok") }
        val registry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(handler)
            .build()

        TestServer.start(registry).use { server ->
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
    @Ignore("TDD target: gRPC TE: trailers requirement is not validated")
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
                // Should produce an envelope-encoded error or a refusal.
                // Per connect-go behavior, this is an INTERNAL or UNKNOWN
                // error reported via the trailer.
                val body = response.body!!.string()
                assertThat(body)
                    .describedAs("server should signal an error for missing TE: trailers")
                    .satisfiesAnyOf(
                        { assertThat(it).contains("\"code\"") },
                        { assertThat(it).contains("grpc-status") },
                    )
                // grpc-status MUST not be 0 (success) when no TE header was sent.
                assertThat(body).doesNotContain("grpc-status: 0")
            }
        }
    }

    /**
     * HTTP/2 cleartext (h2c) prior knowledge should be supported, since
     * gRPC commonly runs over h2c on private networks. Currently we set
     * `enableH2c = false` because we never validated Ktor's h2c wiring.
     */
    @Test
    @Ignore("TDD target: h2c (HTTP/2 cleartext) not enabled in Ktor config")
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
