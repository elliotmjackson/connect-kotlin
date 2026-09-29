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

import com.connectrpc.CODEC_NAME_JSON
import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.StreamType
import com.connectrpc.server.compression.GzipServerCompressionPool
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import kotlin.time.Duration

/** Configuration errors are reported when the server is built, not per request. */
class RegistryTest {
    private val proto = BytesStrategy(CODEC_NAME_PROTO)

    @Test
    fun registryRejectsInvalidConfiguration() {
        assertThatThrownBy { HandlerRegistry.builder().register(unary { r, _ -> r }).build() }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("codec")
        assertThatThrownBy { HandlerRegistry.builder().register(unary { r, _ -> r }).register(unary { r, _ -> r }) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("$SVC/Unary")
        assertThatThrownBy { HandlerRegistry.builder().codec(proto).codec(BytesStrategy(CODEC_NAME_PROTO)) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("proto")
    }

    /** A caught duplicate registration leaves the original handler in place, even inside registerAll. */
    @Test
    fun rejectedHandlerLeavesTheBuilderUnchanged() {
        val builder = HandlerRegistry.builder().codec(proto).register(unary { _, _ -> Msg("original") })
        assertThatThrownBy { builder.register(unary { _, _ -> Msg("replacement") }) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { builder.registerAll(listOf(unary("Other") { r, _ -> r }, unary { _, _ -> Msg("replacement") })) }
            .isInstanceOf(IllegalArgumentException::class.java)
        val server = ConnectServer(builder.build())
        assertThat(server.procedures).containsExactly("$SVC/Unary")
        assertThat(server.call("Unary", "application/proto", "a".toByteArray()).body.readUtf8()).isEqualTo("original")
    }

    @Test
    fun rejectedCodecLeavesTheBuilderUnchanged() {
        val builder = HandlerRegistry.builder().codec(proto)
        assertThatThrownBy { builder.codec(BytesStrategy(CODEC_NAME_PROTO)) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThat(builder.build().codecs).containsExactly(proto)
    }

    @Test
    fun handlerKindMustMatchItsMethodSpec() {
        val mislabelled = object : UnaryHandler<Msg, Msg> {
            override val methodSpec = spec("Unary", StreamType.BIDI)
            override suspend fun handle(request: Msg, ctx: HandlerContext): Msg = request
        }
        assertThatThrownBy { ConnectServer(HandlerRegistry.builder().codec(proto).register(mislabelled).build()) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("$SVC/Unary")
    }

    @Test
    fun optionsRejectInvalidValues() {
        for (build in listOf<() -> ServerConfig>(
            { ServerConfig(readMaxBytes = -1) },
            { ServerConfig(sendMaxBytes = -1) },
            { ServerConfig(compressMinBytes = -1) },
            { ServerConfig(maxTimeout = Duration.ZERO) },
            { ServerConfig(compressionPools = listOf(GzipServerCompressionPool, GzipServerCompressionPool)) },
        )) {
            assertThatThrownBy { build() }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun servesProceduresFromEveryRegistrationPath() {
        val registry = HandlerRegistry.builder().codec(proto).codec(BytesStrategy(CODEC_NAME_JSON))
            .registerAll(
                listOf(
                    unaryHandler(spec("U", StreamType.UNARY)) { req, _ -> Msg("u:${req.text}") },
                    serverStreamHandler(spec("S", StreamType.SERVER)) { req, _, s -> s.send(Msg("s:${req.text}")) },
                    clientStreamHandler(spec("C", StreamType.CLIENT)) { s, _ -> Msg("c:${s.receive()?.text}") },
                ),
            )
            .register(bidiStreamHandler(spec("B", StreamType.BIDI)) { s, _ -> s.send(Msg("b:${s.receive()?.text}")) })
            // An interceptor that overrides nothing leaves every stream kind unchanged.
            .interceptor(object : ServerInterceptor {})
            .build()
        val server = ConnectServer(registry)
        assertThat(server.procedures).containsExactlyInAnyOrder("$SVC/U", "$SVC/S", "$SVC/C", "$SVC/B")
        for ((procedure, expected) in mapOf("U" to "u:x", "S" to "s:x", "C" to "c:x", "B" to "b:x")) {
            val ex = server.callEnveloped(Enveloped.GRPC, procedure, listOf("x"))
            assertThat(Enveloped.GRPC.outcome(ex)).describedAs(procedure).isEqualTo(Outcome(listOf(expected), null, null, emptyMap()))
        }
    }
}
