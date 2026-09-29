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

import com.connectrpc.Headers
import io.ktor.http.content.OutgoingContent
import io.ktor.server.application.ApplicationCall
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.server.routing.RoutingCall
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import io.netty.handler.codec.http2.DefaultHttp2DataFrame
import io.netty.handler.codec.http2.DefaultHttp2Headers
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame
import io.netty.handler.codec.http2.Http2DataFrame
import io.netty.handler.codec.http2.Http2Headers
import io.netty.handler.codec.http2.Http2HeadersFrame
import io.netty.handler.codec.http2.Http2StreamChannel
import io.netty.util.concurrent.PromiseCombiner
import java.io.IOException

/** HTTP trailers for one call's streamed response. */
internal interface GrpcTrailers {
    /** Prepares the call; runs before the response is started. */
    fun install()

    /** Sets the trailers; runs before the response body completes. */
    fun set(trailers: Headers)
}

/**
 * Trailer support on the Netty engine over HTTP/2, the only Ktor 3.6.0
 * transport this adapter sends trailers on: of Ktor's engines only Netty
 * HTTP/2 and HTTP/3 send [OutgoingContent.trailers] at all
 * (ktor-server-netty `http2/NettyHttp2ApplicationResponse.kt:54`,
 * `http3/NettyHttp3ApplicationResponse.kt:51`), and on HTTP/2 not reliably
 * (see [GrpcTrailersWriter]).
 *
 * The Netty engine is a compile-only dependency; Netty classes are only
 * touched once the call is known to be a Netty call.
 */
internal object NettyGrpcTrailers {
    /** Whether the Netty engine is on the classpath; Netty classes are touched only when it is. */
    val nettyPresent = try {
        Class.forName("io.ktor.server.netty.NettyApplicationCall", false, NettyGrpcTrailers::class.java.classLoader)
        true
    } catch (e: ClassNotFoundException) {
        false
    }

    fun forCall(call: ApplicationCall): GrpcTrailers? = if (nettyPresent) Http2StreamTrailers.forCall(call) else null
}

private class Http2StreamTrailers(private val context: ChannelHandlerContext) : GrpcTrailers {
    private val writer = GrpcTrailersWriter()

    /**
     * Puts the writer in front of Ktor's handler. Netty runs `handlerAdded` as
     * an event-loop task (netty-transport 4.2.17 `DefaultChannelPipeline.java:
     * 198-199, 1157-1165`), queued ahead of the response's write tasks because
     * those only start once the call responds (ktor-server-netty 3.6.0
     * `NettyApplicationResponse.kt:119-137`, `cio/NettyHttpResponsePipeline.kt:107-121`).
     * Ktor's handler is missing only from the pipeline of a stream that has
     * closed: Netty removes every handler once a closed channel is
     * deregistered (`DefaultChannelPipeline.java:1406-1412`).
     */
    override fun install() {
        try {
            context.pipeline().addBefore(context.name(), null, writer)
        } catch (e: NoSuchElementException) {
            throw IOException("HTTP/2 stream closed", e)
        }
    }

    override fun set(trailers: Headers) {
        writer.trailers = DefaultHttp2Headers().apply {
            for ((name, values) in trailers) for (value in values) add(name, value)
        }
    }

    companion object {
        fun forCall(call: ApplicationCall): GrpcTrailers? {
            val engineCall = (call as? RoutingCall)?.pipelineCall?.engineCall ?: call
            val context = (engineCall as? NettyApplicationCall)?.context ?: return null
            return if (context.channel() is Http2StreamChannel) Http2StreamTrailers(context) else null
        }
    }
}

/**
 * Ends one HTTP/2 stream with a HEADERS frame carrying [trailers].
 *
 * [OutgoingContent.trailers] cannot do this on Ktor 3.6.0's Netty engine.
 * `NettyHttp2ApplicationResponse` copies it into the stream's trailer block
 * only after the body is written (`http2/NettyHttp2ApplicationResponse.kt:
 * 50-61`), and by then `BaseApplicationResponse` has closed the body channel
 * (ktor-server-core `engine/BaseApplicationResponse.kt:185`, ktor-utils
 * `cio/Readers.kt:37-40`). The event-loop writer leaves its body loop once
 * that channel is drained and closed and reads the trailer block right away
 * (`cio/NettyHttpResponsePipeline.kt:348-381`); finding it empty or partly
 * filled, it ends the stream with an empty DATA frame
 * (`http2/NettyHttp2ApplicationCall.kt:37-42`). The call coroutine runs on a
 * call-group thread, not the stream's event loop
 * (`http2/NettyHttp2Handler.kt:135-138, 163`, `PinnedCallExecutor.kt:15-50`),
 * so either order happens.
 *
 * Ktor writes every frame of the response through the stream pipeline from
 * its handler's context, so they reach this handler on the event loop.
 * [trailers] is set before the body completes, hence before Ktor writes the
 * frame carrying END_STREAM; that flag moves onto the trailers.
 */
private class GrpcTrailersWriter : ChannelOutboundHandlerAdapter() {
    @Volatile
    var trailers: Http2Headers? = null

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        val trailers = trailers
        when {
            trailers == null -> ctx.write(msg, promise)

            msg is Http2HeadersFrame && msg.isEndStream -> {
                msg.headers().add(trailers)
                ctx.write(msg, promise)
            }

            msg is Http2DataFrame && msg.isEndStream -> {
                val combiner = PromiseCombiner(ctx.executor())
                if (msg.content().isReadable || msg.padding() > 0) {
                    combiner.add(ctx.write(DefaultHttp2DataFrame(msg.content(), false, msg.padding())))
                } else {
                    msg.release()
                }
                combiner.add(ctx.write(DefaultHttp2HeadersFrame(trailers, true)))
                combiner.finish(promise)
            }

            else -> ctx.write(msg, promise)
        }
    }
}
