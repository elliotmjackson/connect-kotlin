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
import io.ktor.server.routing.RoutingCall
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame
import io.netty.handler.codec.http2.Http2Error
import io.netty.handler.codec.http2.Http2StreamChannel
import io.netty.util.AttributeKey
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job

/**
 * Ends a call on the Netty engine once its client has gone away, where Ktor
 * 3.6.0 leaves it running:
 *
 * - `HttpRequestLifecycle` cancels only calls whose route set them up before
 *   the stream or connection closed (ktor-server-core 3.6.0
 *   `http/HttpRequestLifecycle.kt:99-113`; ktor-server-netty 3.6.0
 *   `http2/NettyHttp2Handler.kt:109-120`, `http1/NettyHttp1Handler.kt:165-183`),
 *   and a call starts on a call-group thread some time after its request
 *   arrived (`http2/NettyHttp2Handler.kt:156-174`,
 *   `http1/NettyHttp1Handler.kt:262-286`).
 * - An HTTP/2 request body ends or fails only on END_STREAM or a RST_STREAM
 *   from the client (`http2/NettyHttp2Handler.kt:54-84`). A stream closed any
 *   other way, by its connection closing or by the server, leaves the body's
 *   frame actor waiting, so a reader never returns. The actor runs in the
 *   connection's scope (`http2/NettyHttp2ApplicationCall.kt:23`,
 *   `http2/NettyHttp2ApplicationRequest.kt:47-53`,
 *   `http2/NettyHttp2Handler.kt:40, 47`), which the engine completes when the
 *   connection closes (`NettyChannelInitializer.kt:250-252, 322-324`,
 *   `http2/NettyHttp2Handler.kt:285-287`); a completed job still waits for its
 *   children (kotlinx-coroutines 1.11.0 `CompletableJob.kt:22-26`), so the
 *   scope stays a child of the application's job and keeps the closed
 *   connection, its streams and their calls reachable.
 */
internal object NettyDisconnect {
    /**
     * Cancels [call] once its HTTP/2 stream or HTTP/1.1 connection closes and,
     * on HTTP/2, fails its request body then. Stops watching when the returned
     * handle is disposed; null when [call] is not a Netty call.
     */
    fun watch(call: ApplicationCall): DisposableHandle? = if (NettyGrpcTrailers.nettyPresent) Watch.watch(call) else null

    /** Netty classes are only loaded through here, once Netty is known to be present. */
    private object Watch {
        private val scopeWatched = AttributeKey.valueOf<Boolean>("com.connectrpc.server.ktor.scopeWatched")

        fun watch(call: ApplicationCall): DisposableHandle? {
            val engineCall = (call as? RoutingCall)?.pipelineCall?.engineCall ?: call
            if (engineCall !is NettyApplicationCall) return null
            val callJob = engineCall.coroutineContext[Job] ?: return null
            val context = engineCall.context
            val channel = context.channel()
            if (channel is Http2StreamChannel) {
                cancelScopeOnClose(channel.parent(), engineCall, callJob)
                try {
                    context.pipeline().addBefore(context.name(), null, StreamReset(callJob))
                } catch (e: NoSuchElementException) {
                    // The stream's pipeline is gone: it closed and was deregistered.
                }
                if (!channel.isActive) callJob.cancel(CancellationException("HTTP/2 stream closed"))
                return DisposableHandle {}
            }
            // HTTP/1.1: the channel is the connection, which later calls reuse.
            val closed = CancellationException("connection closed")
            val onClose = ChannelFutureListener { callJob.cancel(closed) }
            channel.closeFuture().addListener(onClose)
            if (!channel.isOpen) callJob.cancel(closed)
            return DisposableHandle { channel.closeFuture().removeListener(onClose) }
        }

        /**
         * Cancels the HTTP/2 connection's scope once [connection] closes, so
         * the request body actors of streams no call got to watch end too.
         * Only for Ktor 3.6.0's layout, where that scope is the request's and
         * the parent of every call's job; checked, since cancelling any wider
         * job would stop other connections.
         */
        private fun cancelScopeOnClose(connection: Channel, engineCall: NettyApplicationCall, callJob: Job) {
            if (connection.attr(scopeWatched).setIfAbsent(true) != null) return
            val scope = engineCall.request.coroutineContext[Job] ?: return
            if (scope === engineCall.application.coroutineContext[Job] || scope.children.none { it === callJob }) return
            connection.closeFuture().addListener(ChannelFutureListener { scope.cancel(CancellationException("HTTP/2 connection closed")) })
        }
    }
}

/**
 * Sits in front of Ktor's handler in one HTTP/2 stream's pipeline and, once
 * the stream has closed, passes it a RST_STREAM(CANCEL) frame, which is how
 * Ktor fails a request body (`http2/NettyHttp2Handler.kt:79-84`), then
 * cancels the call. It has to act before Ktor's `channelInactive` detaches
 * the call from the stream (`http2/NettyHttp2Handler.kt:109-120`). Netty
 * completes a stream's close future first and fires `channelInactive` in a
 * later event-loop task (netty-codec-http2 4.2.17
 * `AbstractHttp2StreamChannel.java:761-766, 797-811`), and runs
 * `handlerAdded` in a task queued when the handler is added from another
 * thread (netty-transport 4.2.17 `DefaultChannelPipeline.java:197-201`), so
 * a stream found closed in `handlerAdded` may still have its call attached.
 */
private class StreamReset(private val callJob: Job) : ChannelInboundHandlerAdapter() {
    override fun handlerAdded(ctx: ChannelHandlerContext) {
        if (!ctx.channel().isActive) end(ctx)
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        end(ctx)
        ctx.fireChannelInactive()
    }

    private fun end(ctx: ChannelHandlerContext) {
        ctx.fireChannelRead(DefaultHttp2ResetFrame(Http2Error.CANCEL))
        callJob.cancel(CancellationException("HTTP/2 stream closed"))
    }
}
