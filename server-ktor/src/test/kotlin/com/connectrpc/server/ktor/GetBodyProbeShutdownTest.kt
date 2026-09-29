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

import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.ServerObserver
import com.connectrpc.server.UnaryHandler
import okio.buffer
import okio.source
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

/**
 * A GET whose client announced a body and never sends it waits in the
 * server's probe for that body. Stopping the engine ends it with
 * `unavailable` like any call in flight, instead of waiting for its response
 * until the adapter's five-second drain gives up.
 */
class GetBodyProbeShutdownTest {
    private val started = CountDownLatch(1)

    private val registry = HandlerRegistry.builder()
        .codec(TestSerializationStrategy)
        .register(
            object : UnaryHandler<TestMessage, TestMessage> {
                override val methodSpec = MethodSpec(
                    "test.v1.TestService/Get",
                    TestMessage::class,
                    TestMessage::class,
                    StreamType.UNARY,
                    Idempotency.NO_SIDE_EFFECTS,
                )

                override suspend fun handle(request: TestMessage, ctx: HandlerContext) = request
            },
        )
        .build()

    @Test
    fun stopEndsAStalledGetBodyProbe() {
        val config = ServerConfig(observer = { _, _, _ -> ServerObserver.Call { }.also { started.countDown() } })
        val server = TestServer.start(registry, config, shutdownGracePeriod = Duration.ZERO)
        Socket("127.0.0.1", server.port).use { socket ->
            socket.soTimeout = 10_000
            socket.getOutputStream().write(
                (
                    "GET /test.v1.TestService/Get?encoding=proto&message=a HTTP/1.1\r\nHost: localhost\r\n" +
                        "Transfer-Encoding: chunked\r\n\r\n"
                    ).toByteArray(),
            )
            socket.getOutputStream().flush()
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue()
            val stopping = System.nanoTime()
            server.close()
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopping)).isLessThan(3_000)
            val statusLine = socket.getInputStream().source().buffer().readUtf8LineStrict()
            assertThat(statusLine).startsWith("HTTP/1.1 503")
        }
    }
}
