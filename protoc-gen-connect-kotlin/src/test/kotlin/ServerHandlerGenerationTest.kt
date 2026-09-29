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

import buf.deprecation.v1.MethodDeprecatedServiceHandler
import buf.servergen.`in`.v1.Payload
import buf.servergen.shapes.v1.NoMethodsServiceHandler
import buf.servergen.shapes.v1.WellKnownServiceHandler
import com.connectrpc.Idempotency
import com.connectrpc.StreamType
import com.connectrpc.server.BidiStream
import com.connectrpc.server.BidiStreamHandler
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ClientStreamHandler
import com.connectrpc.server.Handler
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.ServerStreamHandler
import com.connectrpc.server.UnaryHandler
import com.connectrpc.servergen.`fun`.v1.KeywordServiceHandler
import com.connectrpc.servergen.`fun`.v1.ServerGenProto.Outer.Inner
import com.google.protobuf.Empty
import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

/**
 * Exercises the `<Service>Handler` interfaces that buf.gen.yaml generates from
 * proto/buf/servergen and proto/buf/deprecation into the test source set.
 * Implementing each interface here is the check on the generated method
 * signatures: an override with the wrong parameter types, return type or
 * missing `suspend` does not compile.
 */
class ServerHandlerGenerationTest {
    private val service = "buf.servergen.v1.KeywordService"

    private fun inner(value: String): Inner = Inner.newBuilder().setValue(value).build()

    private class KeywordServiceImpl : KeywordServiceHandler {
        override suspend fun `object`(request: Payload, ctx: HandlerContext): Inner = Inner.newBuilder().setValue("object:${request.value}").build()

        override suspend fun `fun`(request: Inner, ctx: HandlerContext): Payload.Nested = Payload.Nested.newBuilder().setCount(request.value.length).build()

        override suspend fun `in`(request: Inner, ctx: HandlerContext, stream: ServerMessageStream<Inner>) {
            for (c in request.value) stream.send(Inner.newBuilder().setValue(c.toString()).build())
        }

        override suspend fun `is`(stream: ClientMessageStream<Inner>, ctx: HandlerContext): Inner {
            val sb = StringBuilder()
            while (true) sb.append((stream.receive() ?: break).value)
            return Inner.newBuilder().setValue(sb.toString()).build()
        }

        override suspend fun `when`(stream: BidiStream<Inner, Inner>, ctx: HandlerContext) {
            while (true) {
                val msg = stream.receive() ?: break
                stream.send(Inner.newBuilder().setValue("when:${msg.value}").build())
            }
        }

        @Deprecated("The method is deprecated in the Protobuf source file.")
        override suspend fun handle(request: Inner, ctx: HandlerContext): Inner = Inner.newBuilder().setValue("handle:${request.value}").build()
    }

    private class WellKnownServiceImpl : WellKnownServiceHandler {
        override suspend fun ping(request: Empty, ctx: HandlerContext): Empty = request

        override suspend fun handlers(request: StringValue, ctx: HandlerContext): Int32Value = Int32Value.of(request.value.length)
    }

    private class Messages<T : Any>(values: List<T>) : BidiStream<T, T> {
        private val pending = ArrayDeque(values)
        val sent = mutableListOf<T>()
        override suspend fun receive(): T? = pending.removeFirstOrNull()
        override suspend fun send(message: T) {
            sent += message
        }
    }

    private val handlers = KeywordServiceImpl().handlers().byPath()

    private fun List<Handler<*, *>>.byPath() = associateBy { it.methodSpec.path }

    private fun ctx(procedure: String) = HandlerContext(
        procedure = procedure,
        requestHeaders = emptyMap(),
        httpMethod = "POST",
        deadline = null,
    )

    @Suppress("UNCHECKED_CAST")
    private fun <H> handler(method: String): H = handlers.getValue("$service/$method") as H

    @Test
    fun registersEveryProcedureWithStreamTypeAndIdempotency() {
        assertThat(handlers.keys).containsExactlyInAnyOrder(
            *listOf("Object", "Fun", "In", "Is", "When", "Handle").map { "$service/$it" }.toTypedArray(),
        )
        val specs = handlers.mapKeys { it.key.substringAfter('/') }.mapValues { it.value.methodSpec }
        assertThat(specs.mapValues { it.value.streamType }).containsExactlyInAnyOrderEntriesOf(
            mapOf(
                "Object" to StreamType.UNARY,
                "Fun" to StreamType.UNARY,
                "In" to StreamType.SERVER,
                "Is" to StreamType.CLIENT,
                "When" to StreamType.BIDI,
                "Handle" to StreamType.UNARY,
            ),
        )
        // Connect GET is only permitted for NO_SIDE_EFFECTS procedures
        // (https://connectrpc.com/docs/protocol#unary-get-request).
        assertThat(specs.mapValues { it.value.idempotency }).containsExactlyInAnyOrderEntriesOf(
            mapOf(
                "Object" to Idempotency.UNKNOWN,
                "Fun" to Idempotency.NO_SIDE_EFFECTS,
                "In" to Idempotency.UNKNOWN,
                "Is" to Idempotency.IDEMPOTENT,
                "When" to Idempotency.UNKNOWN,
                "Handle" to Idempotency.UNKNOWN,
            ),
        )
        // Payload lives in another proto package with java_multiple_files;
        // Inner is nested in the ServerGenProto outer class.
        assertThat(specs.getValue("Object").requestClass).isEqualTo(Payload::class)
        assertThat(specs.getValue("Object").responseClass).isEqualTo(Inner::class)
        assertThat(specs.getValue("Fun").responseClass).isEqualTo(Payload.Nested::class)
    }

    @Test
    fun dispatchesKeywordNamedRpcsToImplementation() = runBlocking<Unit> {
        val obj = handler<UnaryHandler<Payload, Inner>>("Object")
        assertThat(obj.handle(Payload.newBuilder().setValue("x").build(), ctx("$service/Object")).value).isEqualTo("object:x")

        val fn = handler<UnaryHandler<Inner, Payload.Nested>>("Fun")
        assertThat(fn.handle(inner("abcd"), ctx("$service/Fun")).count).isEqualTo(4)

        val serverStream = Messages<Inner>(emptyList())
        handler<ServerStreamHandler<Inner, Inner>>("In").handle(inner("ab"), ctx("$service/In"), serverStream)
        assertThat(serverStream.sent.map { it.value }).containsExactly("a", "b")

        val clientStream = Messages(listOf(inner("a"), inner("b")))
        assertThat(handler<ClientStreamHandler<Inner, Inner>>("Is").handle(clientStream, ctx("$service/Is")).value)
            .isEqualTo("ab")

        val bidi = Messages(listOf(inner("1"), inner("2")))
        handler<BidiStreamHandler<Inner, Inner>>("When").handle(bidi, ctx("$service/When"))
        assertThat(bidi.sent.map { it.value }).containsExactly("when:1", "when:2")
    }

    @Test
    fun rpcNamedHandleReachesImplementationInsteadOfRecursing() = runBlocking<Unit> {
        val h = handler<UnaryHandler<Inner, Inner>>("Handle")
        assertThat(h.handle(inner("x"), ctx("$service/Handle")).value).isEqualTo("handle:x")
    }

    @Test
    fun serviceWithoutMethodsRegistersNothing() {
        assertThat(object : NoMethodsServiceHandler {}.handlers()).isEmpty()
    }

    @Test
    fun rpcNamedHandlersCoexistsWithTheFactoryAndUsesWellKnownTypes() = runBlocking<Unit> {
        val wellKnown = "buf.servergen.shapes.v1.WellKnownService"
        val registered = WellKnownServiceImpl().handlers().byPath()
        assertThat(registered.keys).containsExactlyInAnyOrder("$wellKnown/Ping", "$wellKnown/Handlers")

        val ping = registered.getValue("$wellKnown/Ping").methodSpec
        assertThat(ping.requestClass).isEqualTo(Empty::class)
        assertThat(ping.responseClass).isEqualTo(Empty::class)
        // An explicit IDEMPOTENCY_UNKNOWN maps to the same value as no option.
        assertThat(ping.idempotency).isEqualTo(Idempotency.UNKNOWN)

        @Suppress("UNCHECKED_CAST")
        val rpc = registered.getValue("$wellKnown/Handlers") as UnaryHandler<StringValue, Int32Value>
        assertThat(rpc.handle(StringValue.of("abc"), ctx("$wellKnown/Handlers")).value).isEqualTo(3)
    }

    @Test
    @Suppress("DEPRECATION")
    fun deprecatedOptionAnnotatesInterfaceOrMethod() {
        val interfaces = mapOf(
            // Deprecated types are referenced by qualified name: import directives
            // are outside the reach of @Suppress.
            buf.deprecation.v1.ServiceDeprecatedServiceHandler::class.java to true,
            buf.deprecation.v1.FileDeprecatedServiceHandler::class.java to true,
            MethodDeprecatedServiceHandler::class.java to false,
            KeywordServiceHandler::class.java to false,
        )
        assertThat(interfaces.mapValues { it.key.isAnnotationPresent(Deprecated::class.java) })
            .containsExactlyInAnyOrderEntriesOf(interfaces)

        val methods = mapOf(
            MethodDeprecatedServiceHandler::class.java.method("deprecatedMethod") to true,
            MethodDeprecatedServiceHandler::class.java.method("handlers") to false,
            KeywordServiceHandler::class.java.method("handle") to true,
            KeywordServiceHandler::class.java.method("object") to false,
            // File-level deprecation marks the interface, not each method.
            buf.deprecation.v1.FileDeprecatedServiceHandler::class.java.method("deprecatedByFile") to false,
        )
        assertThat(methods.mapValues { it.key.isAnnotationPresent(Deprecated::class.java) })
            .containsExactlyInAnyOrderEntriesOf(methods)
    }

    private fun Class<*>.method(name: String) = declaredMethods.single { it.name == name }
}
