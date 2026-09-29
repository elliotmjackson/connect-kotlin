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
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame
import io.netty.handler.codec.http2.Http2Error
import io.netty.handler.codec.http2.Http2StreamChannel

/**
 * Aborts a call's response on the Netty engine (see
 * [com.connectrpc.server.http.HttpExchange], "Aborting").
 *
 * Failing the response is not enough: Ktor writes the body from its own
 * coroutine on the event loop, which waits for each flush to reach the
 * connection (ktor-server-netty 3.6.0 `cio/NettyHttpResponsePipeline.kt:
 * 348-374`), so with a client that has stopped reading it never notices the
 * body channel failed. Closing the stream or connection fails that write.
 */
internal object NettyAbort {
    /**
     * Resets [call]'s HTTP/2 stream with RST_STREAM(CANCEL), or closes its
     * HTTP/1.1 connection; does nothing when [call] is not a Netty call.
     */
    fun abort(call: ApplicationCall) {
        if (NettyGrpcTrailers.nettyPresent) Abort.abort(call)
    }

    /** Netty classes are only loaded through here, once Netty is known to be present. */
    private object Abort {
        fun abort(call: ApplicationCall) {
            val engineCall = (call as? RoutingCall)?.pipelineCall?.engineCall ?: call
            if (engineCall !is NettyApplicationCall) return
            val channel = engineCall.context.channel()
            if (channel is Http2StreamChannel) {
                // close() sends RST_STREAM itself only while the stream is open and has not
                // seen END_STREAM both ways (netty-codec-http2 4.2.17
                // `AbstractHttp2StreamChannel.java:733-741`; readEOS is set once the stream is
                // closed, `AbstractHttp2StreamChannel.java:344-345`, called from
                // `Http2MultiplexCodec.java:189-193`, the codec ktor-server-netty 3.6.0 installs
                // in `NettyChannelInitializer.kt:245, 266`), so the reset is sent here too, for a
                // stream whose response end Netty was already handed. It is queued, not
                // awaited: its write completes only once the connection has sent it
                // (`AbstractHttp2StreamChannel.java:1074-1117`), never while a client that
                // stopped reading keeps the socket full. Both operations run on the channel's
                // event loop in the order issued; the reset close() may add is dropped as a
                // duplicate (`Http2ConnectionHandler.java:809-812`).
                channel.writeAndFlush(DefaultHttp2ResetFrame(Http2Error.CANCEL))
            }
            channel.close()
        }
    }
}
