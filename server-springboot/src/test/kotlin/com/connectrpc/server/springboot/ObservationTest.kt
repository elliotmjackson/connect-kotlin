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

package com.connectrpc.server.springboot

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerObserver
import com.connectrpc.server.UnaryHandler
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.http.server.observation.ServerRequestObservationContext
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Connect calls are tagged in Spring Boot's `http.server.requests` metrics with
 * their procedure path rather than `UNKNOWN`.
 */
class ObservationTest {
    companion object {
        /** Observations stopped by the server, handed over once the meter has been recorded. */
        private val stopped = LinkedBlockingQueue<ServerRequestObservationContext>()

        /** `procedure protocol code` for each request the [ServerObserver] bean saw end. */
        private val observed = LinkedBlockingQueue<String>()

        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java, "connectrpc.path-prefix=/rpc")
    }

    private val client = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Before
    fun clear() {
        server.context.getBean(MeterRegistry::class.java).clear()
        stopped.clear()
        observed.clear()
    }

    @Test
    fun registeredProcedureIsTaggedWithItsPath() {
        assertThat(call("test.v1.TestService/Ping")).isEqualTo(200)
        assertThat(awaitUri()).isEqualTo("/rpc/test.v1.TestService/Ping")
        val timer = server.context.getBean(MeterRegistry::class.java).find("http.server.requests").timer()
        assertThat(timer?.id?.getTag("uri")).isEqualTo("/rpc/test.v1.TestService/Ping")
        assertThat(timer?.count()).isEqualTo(1)
    }

    /** Arbitrary paths under a service mapping must not become tag values. */
    @Test
    fun unknownMethodOfRegisteredServiceIsNotTaggedWithItsPath() {
        assertThat(call("test.v1.TestService/Nope")).isEqualTo(404)
        assertThat(awaitUri()).isEqualTo("NOT_FOUND")
    }

    /** A ServerObserver bean observes calls and requests rejected before a handler, next to the http.server.requests tags. */
    @Test
    fun serverObserverBeanSeesEveryRequest() {
        assertThat(call("test.v1.TestService/Ping")).isEqualTo(200)
        assertThat(observed.poll(5, TimeUnit.SECONDS)).isEqualTo("test.v1.TestService/Ping connect null")
        assertThat(call("test.v1.TestService/Nope")).isEqualTo(404)
        assertThat(observed.poll(5, TimeUnit.SECONDS)).isEqualTo("null null UNIMPLEMENTED")
    }

    private fun call(procedure: String): Int {
        val request = Request.Builder()
            .url("http://127.0.0.1:${server.port}/rpc/$procedure")
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        return client.newCall(request).execute().use { it.code }
    }

    private fun awaitUri(): String? {
        val context = checkNotNull(stopped.poll(5, TimeUnit.SECONDS)) { "no http.server.requests observation stopped" }
        return context.lowCardinalityKeyValues.firstOrNull { it.key == "uri" }?.value
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        /**
         * Handlers stop in reverse registration order (micrometer-observation 1.17
         * `SimpleObservation.notifyOnObservationStopped`). Boot adds its meter handler
         * to this registry after the one registered here, so the meter exists by the
         * time this handler sees the observation stop.
         */
        @Bean
        open fun observationRegistry(): ObservationRegistry = ObservationRegistry.create().apply {
            observationConfig().observationHandler(
                object : ObservationHandler<ServerRequestObservationContext> {
                    override fun supportsContext(context: Observation.Context) = context is ServerRequestObservationContext
                    override fun onStop(context: ServerRequestObservationContext) {
                        stopped.add(context)
                    }
                },
            )
        }

        @Bean
        open fun connectRpcObserver() = ServerObserver { procedure, protocol, _ ->
            ServerObserver.Call { code -> observed.add("$procedure $protocol $code") }
        }

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = HandlerRegistry.builder()
            .codec(TestSerializationStrategy)
            .register(
                object : UnaryHandler<TestMessage, TestMessage> {
                    override val methodSpec = MethodSpec("test.v1.TestService/Ping", TestMessage::class, TestMessage::class, StreamType.UNARY)
                    override suspend fun handle(request: TestMessage, ctx: HandlerContext) = request
                },
            )
            .build()
    }
}
