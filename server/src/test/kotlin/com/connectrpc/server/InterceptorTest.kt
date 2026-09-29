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
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.Collections
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

class InterceptorTest {
    /**
     * Logs entry and exit of [ServerInterceptor.interceptCall] as `[name` / `]name`, and of
     * every `wrap*` handler as `>name` / `<name`; `!` marks an exit by exception.
     */
    private class Recording(private val name: String, private val log: MutableList<String>) : ServerInterceptor {
        private suspend fun <T> around(open: String = ">", close: String = "<", block: suspend () -> T): T {
            log += "$open$name"
            val result = try {
                block()
            } catch (e: Exception) {
                log += "$close$name!"
                throw e
            }
            log += "$close$name"
            return result
        }

        override suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T = around("[", "]") { next() }

        override fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>) = object : UnaryHandler<Req, Res> {
            override val methodSpec = next.methodSpec
            override suspend fun handle(request: Req, ctx: HandlerContext): Res = around { next.handle(request, ctx) }
        }

        override fun <Req : Any, Res : Any> wrapServerStream(next: ServerStreamHandler<Req, Res>) = object : ServerStreamHandler<Req, Res> {
            override val methodSpec = next.methodSpec
            override suspend fun handle(request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>) = around { next.handle(request, ctx, stream) }
        }

        override fun <Req : Any, Res : Any> wrapClientStream(next: ClientStreamHandler<Req, Res>) = object : ClientStreamHandler<Req, Res> {
            override val methodSpec = next.methodSpec
            override suspend fun handle(stream: ClientMessageStream<Req>, ctx: HandlerContext): Res = around { next.handle(stream, ctx) }
        }

        override fun <Req : Any, Res : Any> wrapBidi(next: BidiStreamHandler<Req, Res>) = object : BidiStreamHandler<Req, Res> {
            override val methodSpec = next.methodSpec
            override suspend fun handle(stream: BidiStream<Req, Res>, ctx: HandlerContext) = around { next.handle(stream, ctx) }
        }
    }

    private val log: MutableList<String> = Collections.synchronizedList(mutableListOf())

    private fun handlers(fail: Boolean): List<Handler<*, *>> {
        fun run(): Msg {
            log += "handler"
            if (fail) throw ConnectException(Code.ABORTED, "no")
            return Msg("ok")
        }
        return listOf(
            unary { _, _ -> run() },
            serverStream { _, _, s -> s.send(run()) },
            clientStream { s, _ ->
                while (s.receive() != null) continue
                run()
            },
            bidi { s, _ -> s.send(run()) },
        )
    }

    private fun serverWith(fail: Boolean = false, perProcedure: String? = null): ConnectServer {
        val builder = HandlerRegistry.builder().codec(BytesStrategy("proto"))
            .interceptor(Recording("a", log))
            .interceptor(Recording("b", log))
        for (h in handlers(fail)) {
            val procedure = h.methodSpec.path.substringAfter('/')
            builder.register(h, if (perProcedure == null || perProcedure == procedure) listOf(Recording("p", log)) else emptyList())
        }
        return ConnectServer(builder.build())
    }

    private val procedures = listOf("Unary" to "application/grpc", "ServerStream" to "application/grpc", "ClientStream" to "application/grpc", "Bidi" to "application/grpc")

    @Test
    fun firstRegisteredIsOutermostForEveryStreamKind() {
        val server = serverWith()
        for ((procedure, contentType) in procedures) {
            log.clear()
            server.call(procedure, contentType, env(0, "x"))
            assertThat(log).describedAs(procedure).containsExactly("[a", "[b", "[p", ">a", ">b", ">p", "handler", "<p", "<b", "<a", "]p", "]b", "]a")
        }
    }

    @Test
    fun handlerErrorsUnwindThroughEveryInterceptor() {
        val server = serverWith(fail = true)
        for ((procedure, contentType) in procedures) {
            log.clear()
            val ex = server.call(procedure, contentType, env(0, "x"))
            assertThat(log).describedAs(procedure).containsExactly("[a", "[b", "[p", ">a", ">b", ">p", "handler", "<p!", "<b!", "<a!", "]p!", "]b!", "]a!")
            assertThat(Enveloped.GRPC.outcome(ex).code).isEqualTo(Code.ABORTED)
        }
    }

    @Test
    fun perProcedureInterceptorsApplyOnlyToTheirProcedure() {
        val server = serverWith(perProcedure = "ServerStream")
        for ((procedure, contentType) in procedures) {
            log.clear()
            server.call(procedure, contentType, env(0, "x"))
            val expected = if (procedure == "ServerStream") {
                listOf("[a", "[b", "[p", ">a", ">b", ">p", "handler", "<p", "<b", "<a", "]p", "]b", "]a")
            } else {
                listOf("[a", "[b", ">a", ">b", "handler", "<b", "<a", "]b", "]a")
            }
            assertThat(log).describedAs(procedure).containsExactlyElementsOf(expected)
        }
    }

    @Test
    fun registerAllInterceptorsApplyOnlyToThoseHandlers() {
        val (first, second) = handlers(fail = false).chunked(2)
        val server = ConnectServer(
            HandlerRegistry.builder().codec(BytesStrategy("proto"))
                .registerAll(first, listOf(Recording("s", log)))
                .registerAll(second)
                .build(),
        )
        for ((procedure, contentType) in procedures) {
            log.clear()
            server.call(procedure, contentType, env(0, "x"))
            val expected = if (procedure in setOf("Unary", "ServerStream")) listOf("[s", ">s", "handler", "<s", "]s") else listOf("handler")
            assertThat(log).describedAs(procedure).containsExactlyElementsOf(expected)
        }
    }

    /** An interceptor that rejects a call before the handler, as an auth check does. */
    @Test
    fun interceptorCanRejectWithoutRunningTheHandler() {
        val auth = object : ServerInterceptor {
            override fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>) = object : UnaryHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext): Res {
                    if (ctx.requestHeaders["authorization"]?.single() != "Bearer ok") {
                        throw ConnectException(Code.UNAUTHENTICATED, "who are you").withMetadata(mapOf("www-authenticate" to listOf("Bearer")))
                    }
                    return next.handle(request, ctx)
                }
            }

            override fun <Req : Any, Res : Any> wrapServerStream(next: ServerStreamHandler<Req, Res>) = object : ServerStreamHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext, stream: ServerMessageStream<Res>) {
                    ctx.responseHeaders["x-checked"] = mutableListOf("1")
                    if (ctx.requestHeaders["authorization"] == null) throw ConnectException(Code.UNAUTHENTICATED)
                    next.handle(request, ctx, stream)
                }
            }
        }
        val server = server(unary { _, _ -> Msg("secret").also { log += "handler" } }, serverStream { _, _, s -> s.send(Msg("secret").also { log += "handler" }) }) {
            interceptor(auth)
        }
        val unary = server.call("Unary", "application/proto", "a".toByteArray())
        assertThat(unary.status).isEqualTo(401)
        assertThat(unary.headers["www-authenticate"]).containsExactly("Bearer")
        assertThat(parseJson(unary.body.readUtf8())).isEqualTo(mapOf("code" to "unauthenticated", "message" to "who are you"))
        for (protocol in Enveloped.values()) {
            val ex = server.callEnveloped(protocol, "ServerStream", listOf("a"))
            assertThat(protocol.outcome(ex)).describedAs("$protocol").isEqualTo(Outcome(emptyList(), Code.UNAUTHENTICATED, null, emptyMap()))
            assertThat(ex.headers["x-checked"]).containsExactly("1")
        }
        assertThat(log).isEmpty()
        assertThat(server.call("Unary", "application/proto", "a".toByteArray(), mapOf("Authorization" to "Bearer ok")).body.readUtf8()).isEqualTo("secret")
    }

    /** Interceptors see the handler's own exception and may translate it. */
    @Test
    fun interceptorCanTranslateHandlerExceptions() {
        val seen = mutableListOf<String>()
        val translate = object : ServerInterceptor {
            override fun <Req : Any, Res : Any> wrapUnary(next: UnaryHandler<Req, Res>) = object : UnaryHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(request: Req, ctx: HandlerContext): Res = try {
                    next.handle(request, ctx)
                } catch (e: NoSuchElementException) {
                    seen += e.message!!
                    throw ConnectException(Code.NOT_FOUND, "no such thing")
                }
            }
        }
        val server = server(unary { req, _ -> throw NoSuchElementException("row ${req.text}") }) { interceptor(translate) }
        val ex = server.call("Unary", "application/proto", "7".toByteArray())
        assertThat(ex.status).isEqualTo(404)
        assertThat(parseJson(ex.body.readUtf8())).isEqualTo(mapOf("code" to "not_found", "message" to "no such thing"))
        assertThat(seen).containsExactly("row 7")
    }

    /** Stream interceptors can wrap the message streams they pass on. */
    @Test
    fun interceptorCanWrapMessageStreams() {
        val upper = object : ServerInterceptor {
            @Suppress("UNCHECKED_CAST")
            override fun <Req : Any, Res : Any> wrapBidi(next: BidiStreamHandler<Req, Res>) = object : BidiStreamHandler<Req, Res> {
                override val methodSpec = next.methodSpec
                override suspend fun handle(stream: BidiStream<Req, Res>, ctx: HandlerContext) = next.handle(
                    object : BidiStream<Req, Res> {
                        override suspend fun receive(): Req? = (stream.receive() as Msg?)?.let { Msg("in:${it.text}") as Req }
                        override suspend fun send(message: Res) = stream.send(Msg("out:${(message as Msg).text}") as Res)
                    },
                    ctx,
                )
            }
        }
        val server = server(
            bidi { s, _ ->
                while (true) s.send(s.receive() ?: break)
            },
        ) { interceptor(upper) }
        for (protocol in Enveloped.values()) {
            assertThat(protocol.outcome(server.callEnveloped(protocol, "Bidi", listOf("1", "2"))).messages).describedAs("$protocol")
                .containsExactly("out:in:1", "out:in:2")
        }
    }

    /**
     * An auth check in interceptCall ends every stream kind before any byte of
     * the request is read, so bodies that would fail to decode or exceed
     * readMaxBytes still get `unauthenticated`.
     */
    @Test
    fun interceptCallRejectsBeforeTheRequestIsRead() {
        val auth = object : ServerInterceptor {
            override suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T {
                if (ctx.requestHeaders["authorization"]?.single() != "Bearer ok") throw ConnectException(Code.UNAUTHENTICATED, "who are you")
                return next()
            }
        }
        val server = server(*handlers(fail = false).toTypedArray()) { interceptor(auth) }
        val huge = ByteArray(ServerConfig.DEFAULT_READ_MAX_BYTES.toInt() + 1)
        // The codec rejects text starting with `!`; the last enveloped body is a truncated prefix.
        for (body in listOf("!bad".toByteArray(), huge)) {
            val ex = server.call("Unary", "application/proto", body)
            assertThat(ex.status).isEqualTo(401)
            assertThat(parseJson(ex.body.readUtf8())).isEqualTo(mapOf("code" to "unauthenticated", "message" to "who are you"))
            assertThat(ex.bytesRead.get()).isZero()
        }
        for (procedure in listOf("Unary", "ServerStream", "ClientStream", "Bidi")) {
            // Unary procedures take Connect unary, not Connect streaming (protocol.md "Unary-Content-Type").
            for (protocol in Enveloped.values().filter { procedure != "Unary" || it != Enveloped.CONNECT }) {
                for (body in listOf(env(0, "!bad"), env(0, huge), byteArrayOf(0, 0x7f))) {
                    val ex = server.call(procedure, protocol.contentType, body)
                    assertThat(protocol.outcome(ex).code).describedAs("$procedure $protocol").isEqualTo(Code.UNAUTHENTICATED)
                    assertThat(ex.bytesRead.get()).describedAs("$procedure $protocol").isZero()
                }
            }
        }
        assertThat(log).isEmpty()
        val allowed = server.call("Unary", "application/proto", "x".toByteArray(), mapOf("Authorization" to "Bearer ok"))
        assertThat(allowed.body.readUtf8()).isEqualTo("ok")
    }

    /** A request that fails to decode fails inside interceptCall's next, as in connect-py (`test/test_interceptor.py:483-509`). */
    @Test
    fun interceptCallSeesRequestsThatFailToDecode() {
        val seen = mutableListOf<Code>()
        val observe = object : ServerInterceptor {
            override suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T = try {
                next()
            } catch (e: ConnectException) {
                seen += e.code
                throw e
            }
        }
        val server = server(*handlers(fail = false).toTypedArray()) { interceptor(observe) }
        assertThat(server.call("Unary", "application/proto", "!bad".toByteArray()).status).isEqualTo(400)
        assertThat(Enveloped.GRPC.outcome(server.callEnveloped(Enveloped.GRPC, "ServerStream", listOf("!bad"))).code).isEqualTo(Code.INVALID_ARGUMENT)
        assertThat(seen).containsExactly(Code.INVALID_ARGUMENT, Code.INVALID_ARGUMENT)
        assertThat(log).isEmpty()
    }

    private class Principal(val name: String) : AbstractCoroutineContextElement(Principal) {
        companion object Key : CoroutineContext.Key<Principal>
    }

    /** Handlers of every stream kind run inside the context interceptCall gives next. */
    @Test
    fun interceptCallCanPassContextToHandlers() {
        val auth = object : ServerInterceptor {
            override suspend fun <T> interceptCall(ctx: HandlerContext, next: suspend () -> T): T = withContext(Principal("alice")) { next() }
        }
        suspend fun principal() = Msg(currentCoroutineContext()[Principal]?.name ?: "nobody")
        val server = server(
            unary { _, _ -> principal() },
            serverStream { _, _, s -> s.send(principal()) },
            clientStream { s, _ ->
                while (s.receive() != null) continue
                principal()
            },
            bidi { s, _ -> s.send(principal()) },
        ) { interceptor(auth) }
        assertThat(server.call("Unary", "application/proto", "x".toByteArray()).body.readUtf8()).isEqualTo("alice")
        for (procedure in listOf("Unary", "ServerStream", "ClientStream", "Bidi")) {
            assertThat(Enveloped.GRPC.outcome(server.callEnveloped(Enveloped.GRPC, procedure, listOf("x"))).messages).describedAs(procedure).containsExactly("alice")
        }
    }
}
