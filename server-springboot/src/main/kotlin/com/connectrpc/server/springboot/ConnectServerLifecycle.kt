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

import com.connectrpc.server.ConnectServer
import kotlinx.coroutines.runBlocking
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.SmartLifecycle
import kotlin.time.Duration

/**
 * Shuts [server] down when the application context closes, just before Spring
 * Boot shuts the web server down.
 *
 * A graceful web server shutdown (`server.shutdown=graceful`, the default,
 * spring-boot-web-server 4.1.0 `ServerProperties.java:107`) stops accepting
 * requests and waits for the active ones to end (spring-boot-tomcat 4.1.0
 * `GracefulShutdown.java:65-111`) for up to
 * `spring.lifecycle.timeout-per-shutdown-phase`, then stops the server. A
 * long-lived stream never ends by itself, so it would be cut off with its
 * connection. [stop] runs [ConnectServer.shutdown] first: calls that start
 * from then on fail with `unavailable`, and calls in flight get [gracePeriod]
 * and then end with `unavailable`, in their protocol's error shape.
 *
 * Its phase is one above the web server's graceful shutdown
 * ([WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE], spring-boot-web-server
 * 4.1.0 `WebServerApplicationContext.java:40`), and phases stop from the
 * highest down (spring-context 7.0.8 `DefaultLifecycleProcessor.java:436-449`).
 * So the container still accepts requests while calls end, and the calls
 * rejected meanwhile get a Connect error rather than the container's own
 * refusal; the graceful shutdown then waits for the responses of the ended
 * calls. [stop] blocks until the calls still running after [gracePeriod] have
 * been cancelled. A blocking stop is not cut short by the phase timeout: the
 * processor calls each bean's `stop` before it starts to wait on the phase
 * (`DefaultLifecycleProcessor.java:643-654`, `SmartLifecycle.java:142-145`).
 *
 * Shutting a [ConnectServer] down cannot be undone, so the lifecycle is not
 * pauseable, and [start] after [stop] does not serve calls again.
 *
 * @param server The server to shut down.
 * @param gracePeriod How long calls in flight may run once the context
 *     starts to close.
 */
class ConnectServerLifecycle(private val server: ConnectServer, private val gracePeriod: Duration) : SmartLifecycle {
    @Volatile
    private var running = false

    override fun start() {
        running = true
    }

    override fun stop() {
        running = false
        runBlocking { server.shutdown(gracePeriod) }
    }

    override fun isRunning(): Boolean = running

    override fun isPauseable(): Boolean = false

    override fun getPhase(): Int = WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE + 1
}
