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

import io.ktor.server.application.ApplicationCall
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.server.netty.NettyApplicationResponse
import io.ktor.server.routing.RoutingCall
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import io.netty.handler.codec.http.LastHttpContent
import io.netty.handler.codec.http2.Http2DataFrame
import io.netty.handler.codec.http2.Http2HeadersFrame
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.Job

/**
 * When a call's response has reached the connection on the Netty engine.
 *
 * Stopping the Netty engine closes every connection as soon as its worker
 * event loops start to shut down: `stop` calls `shutdownGracefully` on them
 * (ktor-server-netty 3.6.0 `NettyApplicationEngine.kt:547-555`), and each loop
 * then has its IO handler close all its channels before it runs its queued
 * tasks (netty-transport 4.2.17 `SingleThreadIoEventLoop.java:195-201`; NIO:
 * `nio/NioIoHandler.java:600-612`). The engine's grace period is only the
 * loops' quiet period for tasks, so a response still in Ktor's body channel,
 * or written but not yet flushed, is cut off. A call's own coroutine does not
 * say when its response is written: Ktor completes it once the write is
 * started, and the body is written by a separate coroutine on the event loop
 * (`cio/NettyHttpResponsePipeline.kt:89-101, 224-231`).
 */
internal object NettyResponseWritten {
    /**
     * A job that completes once the message ending [call]'s response has been
     * written to the connection, or the connection has closed; null when
     * [call] is not a Netty call. Must be called before the call responds.
     */
    fun forCall(call: ApplicationCall): Job? = if (NettyGrpcTrailers.nettyPresent) Watch.forCall(call) else null

    /** Netty classes are only loaded through here, once Netty is known to be present. */
    private object Watch {
        fun forCall(call: ApplicationCall): Job? {
            val engineCall = (call as? RoutingCall)?.pipelineCall?.engineCall ?: call
            if (engineCall !is NettyApplicationCall) return null
            val context = engineCall.context
            val watcher = ResponseEndWatcher(engineCall.response)
            // Nothing more is written once the channel closes: the response
            // failed, or the stream or connection was reset.
            val closeFuture = context.channel().closeFuture()
            val onClose = ChannelFutureListener { watcher.written.complete() }
            closeFuture.addListener(onClose)
            watcher.written.invokeOnCompletion { closeFuture.removeListener(onClose) }
            // Added before the response starts, so before its first write
            // reaches the pipeline (see Http2StreamTrailers.install); Ktor's
            // handler is missing only from a closed channel's pipeline, whose
            // close future is already done.
            try {
                context.pipeline().addBefore(context.name(), null, watcher)
            } catch (e: NoSuchElementException) {
                watcher.written.complete()
            }
            return watcher.written
        }
    }
}

/**
 * Completes [written] once the message that ends [response] has been written,
 * then leaves the pipeline.
 *
 * On HTTP/1.1 the pipeline is the connection's, which carries the responses of
 * its calls one after another in request order (ktor-server-netty 3.6.0
 * `cio/NettyHttpResponsePipeline.kt:103-121`), so the response is ended by the
 * first [LastHttpContent] (which a `FullHttpResponse` also is) from its own
 * header message on. On HTTP/2 the pipeline is the stream's; its response
 * ends with the frame that sets END_STREAM, which the gRPC trailer writer in
 * front of Ktor's handler has already moved onto the trailers.
 */
private class ResponseEndWatcher(private val response: NettyApplicationResponse) : ChannelOutboundHandlerAdapter() {
    val written: CompletableJob = Job()

    private var started = false

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        // The header message is set before Ktor's event-loop writer is
        // notified to write it (`NettyApplicationResponse.kt:64-68, 126-135`);
        // a pipelined call's header can pass before this call has set one.
        if (!started) started = msg === runCatching { response.responseMessage }.getOrNull()
        val ends = started && (msg is LastHttpContent || (msg is Http2HeadersFrame && msg.isEndStream) || (msg is Http2DataFrame && msg.isEndStream))
        if (!ends) {
            ctx.write(msg, promise)
            return
        }
        val end = promise.unvoid()
        ctx.write(msg, end)
        end.addListener { written.complete() }
        ctx.pipeline().remove(this)
    }
}
