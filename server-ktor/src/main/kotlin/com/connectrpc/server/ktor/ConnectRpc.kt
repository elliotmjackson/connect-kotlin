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
import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerConfig
import com.connectrpc.server.http.HttpExchange
import com.connectrpc.server.http.ResponseSink
import com.connectrpc.server.http.StreamingBody
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationStopPreparing
import io.ktor.server.application.install
import io.ktor.server.http.HttpRequestLifecycle
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.request.queryString
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.application
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.util.AttributeKey
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okio.Buffer
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Serves every procedure of [registry] at `/<procedure>` on this application,
 * over Connect, gRPC and gRPC-Web. See [Route.connectRpc].
 *
 * ```
 * embeddedServer(Netty, configure = {
 *     connector { port = 8080 }
 *     // HTTP/2 over TLS and cleartext (h2c), which gRPC needs.
 *     enableHttp2 = true
 *     enableH2c = true
 * }) {
 *     connectRpc(
 *         HandlerRegistry.builder()
 *             .codec(GoogleJavaProtobufStrategy())
 *             .codec(GoogleJavaJSONStrategy())
 *             .registerAll(ElizaServiceImpl().handlers())
 *             .build(),
 *     )
 * }.start(wait = true)
 * ```
 *
 * @param shutdownGracePeriod See [Route.connectRpc].
 */
fun Application.connectRpc(
    registry: HandlerRegistry,
    config: ServerConfig = ServerConfig(),
    shutdownGracePeriod: Duration = DEFAULT_SHUTDOWN_GRACE_PERIOD,
) {
    routing { connectRpc(ConnectServer(registry, config), shutdownGracePeriod) }
}

/**
 * Mounts [server]'s procedures under this route, one child route per
 * procedure path, for every HTTP method: the server itself answers 405 and
 * 415.
 *
 * - Client disconnects cancel the handler: [HttpRequestLifecycle] with
 *   `cancelCallOnClose` is installed on each route (Ktor 3.6 supports it on the
 *   CIO and Netty engines, HTTP/1.1 connection close and HTTP/2 stream close),
 *   and on Netty the adapter watches each call's stream or connection too,
 *   for the cases the plugin misses (see [NettyDisconnect]).
 * - A response that fails part way (its write throws, or a handler `Error`
 *   after it started) is aborted: on Netty the adapter resets the HTTP/2
 *   stream or closes the HTTP/1.1 connection (see [NettyAbort]). The call's
 *   deadline does not bound response writes; the engine's write limits do
 *   (see the module README).
 * - gRPC needs HTTP trailers, which this adapter sends only on the Netty
 *   engine over HTTP/2 (see [NettyGrpcTrailers]); elsewhere gRPC calls fail
 *   with `unimplemented` while Connect and gRPC-Web work on any engine.
 * - Procedure paths are case-sensitive and matched byte for byte
 *   (protocol.md "Path", lines 146-148). Ktor routing is more lenient: it
 *   skips empty segments and compares percent-decoded ones, so it also routes
 *   `//pkg.Svc//Method` or `/pkg.Svc/%4Dethod` here. The server is given the
 *   raw request path (see [enginePath]) and answers those with 404, so the
 *   path an authorization check reads is the path that was served.
 * - When the engine stops, [ConnectServer.shutdown] runs with
 *   [shutdownGracePeriod]: calls that start from then on fail with
 *   `unavailable`, calls in flight get the grace period and then end with
 *   `unavailable`, in their protocol's error shape. It runs on
 *   [ApplicationStopPreparing], which the Netty engine raises first in `stop`,
 *   before it closes anything (ktor-server-netty 3.6.0
 *   `NettyApplicationEngine.kt:515-525`; `ApplicationStopping` comes only
 *   after the engine has stopped, ktor-server-core 3.6.0
 *   `EmbeddedServerJvm.kt:423-431`), and holds `stop` until those calls'
 *   responses have been written, for at most five seconds after the grace
 *   period: the engine's own grace period does not wait for responses (see
 *   [NettyResponseWritten]). Every server mounted on one application shuts
 *   down at the same time, so `stop` waits for the longest grace period, not
 *   their sum.
 *
 * @param server The server to mount.
 * @param shutdownGracePeriod How long calls in flight may run once the engine
 *     starts to stop. Defaults to one second, the engine's default
 *     `shutdownGracePeriod` (ktor-server-core 3.6.0 `ApplicationEngine.kt:61`).
 */
fun Route.connectRpc(server: ConnectServer, shutdownGracePeriod: Duration = DEFAULT_SHUTDOWN_GRACE_PERIOD) {
    val calls = InFlightCalls()
    Mounts.of(application) += Mount(server, shutdownGracePeriod, calls)
    for (procedure in server.procedures) {
        route("/$procedure") {
            install(HttpRequestLifecycle) { cancelCallOnClose = true }
            handle { calls.track(call) { serve(server, call, procedure) } }
        }
    }
}

private suspend fun serve(server: ConnectServer, call: ApplicationCall, procedure: String) {
    val disconnect = NettyDisconnect.watch(call)
    val exchange = KtorExchange(call, enginePath(call.request.path(), procedure))
    try {
        server.serve(exchange)
    } finally {
        disconnect?.dispose()
        // Once serve has returned: closing the stream or connection cancels the
        // call (NettyDisconnect, HttpRequestLifecycle), which while serve was
        // still running would change the code it reports.
        if (exchange.aborted) NettyAbort.abort(call)
    }
}

private val DEFAULT_SHUTDOWN_GRACE_PERIOD = 1.seconds

/** Ktor's default engine `shutdownTimeout` (ktor-server-core 3.6.0 `ApplicationEngine.kt:68`). */
private val RESPONSE_DRAIN_TIMEOUT = 5.seconds

private class Mount(val server: ConnectServer, val gracePeriod: Duration, val calls: InFlightCalls)

/**
 * Every server mounted on one application. One [ApplicationStopPreparing]
 * subscriber shuts them all down concurrently, so separate mounts share one
 * stop window instead of each waiting for the one before it.
 */
private class Mounts private constructor() {
    private val mounts = CopyOnWriteArrayList<Mount>()

    operator fun plusAssign(mount: Mount) {
        mounts += mount
    }

    private fun stop() = runBlocking {
        mounts.map { mount ->
            launch {
                mount.server.shutdown(mount.gracePeriod)
                mount.calls.join(RESPONSE_DRAIN_TIMEOUT)
            }
        }.joinAll()
    }

    companion object {
        private val KEY = AttributeKey<Mounts>("com.connectrpc.server.ktor.Mounts")

        fun of(application: Application): Mounts = synchronized(application.attributes) {
            application.attributes.getOrNull(KEY) ?: Mounts().also { mounts ->
                application.attributes.put(KEY, mounts)
                application.monitor.subscribe(ApplicationStopPreparing) { mounts.stop() }
            }
        }
    }
}

/** The calls being served, each until its response has been written. */
private class InFlightCalls {
    private val calls: MutableSet<Job> = ConcurrentHashMap.newKeySet()

    suspend fun track(call: ApplicationCall, serve: suspend () -> Unit) {
        val written = NettyResponseWritten.forCall(call)
        val done = Job()
        calls += done
        done.invokeOnCompletion { calls -= done }
        try {
            serve()
        } finally {
            if (written == null) done.complete() else written.invokeOnCompletion { done.complete() }
        }
    }

    /** Waits up to [timeout] for the calls in flight now. */
    suspend fun join(timeout: Duration) {
        withTimeoutOrNull(timeout) { calls.toList().joinAll() }
    }
}

/**
 * The path to route on, from the raw (not percent-decoded) request path of a
 * call Ktor routed to [procedure]. A canonical path is the mount prefix Ktor
 * matched followed by `/<procedure>`; anything with an empty, `.` or `..`
 * segment or any percent-encoding is passed whole, names no procedure, and is
 * answered with 404.
 */
internal fun enginePath(rawPath: String, procedure: String): String {
    val canonical = rawPath.startsWith('/') &&
        '%' !in rawPath &&
        rawPath.split('/').drop(1).none { it.isEmpty() || it == "." || it == ".." }
    return if (canonical && rawPath.endsWith("/$procedure")) "/$procedure" else rawPath
}

private const val CHUNK = 16 * 1024

/** [HttpExchange] over one Ktor call. */
private class KtorExchange(private val call: ApplicationCall, override val path: String) : HttpExchange {
    override val method: String = call.request.httpMethod.value
    override val rawQuery: String? = call.request.queryString().ifEmpty { null }

    // names() lists each spelling a field arrived under on HTTP/1.1, and getAll
    // ignores case, so keying by spelling would repeat every value.
    override val requestHeaders: Headers = call.request.headers.let { headers ->
        headers.names().map { it.lowercase() }.distinct().associateWith { headers.getAll(it).orEmpty() }
    }
    private val trailers: GrpcTrailers? = NettyGrpcTrailers.forCall(call)
    override val supportsTrailers: Boolean get() = trailers != null

    private var body: ByteReadChannel? = null

    /** Whether a response failed or was cancelled, so the exchange must be aborted (see [HttpExchange], "Aborting"). */
    var aborted = false
        private set

    // Allocated on the first read: requests answered 404, 405 or 415 read no body.
    private var chunk: ByteArray? = null

    override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
        val channel = body ?: call.receiveChannel().also { body = it }
        val buffer = chunk ?: ByteArray(CHUNK).also { chunk = it }
        val read = channel.readAvailable(buffer, 0, minOf(maxBytes, CHUNK.toLong()).toInt())
        if (read == -1) {
            // readAvailable returns -1 for a channel closed with a cause too
            // (ktor-io 3.6.0 `ByteReadChannelOperations.kt:273-283`); a reset
            // stream must not look like the end of the request. Nor may a
            // body cut off with its connection: Netty's HTTP/1.1 decoder
            // passes on no end for it (netty-codec-http 4.2.17
            // `HttpObjectDecoder.java:600-620`) and Ktor closes the body
            // normally once the pipeline goes (ktor-server-netty 3.6.0
            // `cio/RequestBodyHandler.kt:74-78, 204-209`), after the close
            // has cancelled the call (see NettyDisconnect).
            channel.closedCause?.let { throw IOException("request body failed", it) }
            currentCoroutineContext().ensureActive()
            return -1
        }
        sink.write(buffer, 0, read)
        return read.toLong()
    }

    override suspend fun respond(status: Int, headers: Headers, body: ByteArray) = abortOnFailure {
        setHeaders(headers)
        call.respond(
            object : OutgoingContent.ByteArrayContent() {
                override val status = HttpStatusCode.fromValue(status)
                override val contentLength = body.size.toLong()
                override fun bytes() = body
            },
        )
    }

    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) = abortOnFailure {
        setHeaders(headers)
        trailers?.install()
        call.respond(
            object : OutgoingContent.WriteChannelContent() {
                override val status = HttpStatusCode.fromValue(status)
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    val result = try {
                        body.writeTo(ChannelSink(channel))
                    } catch (e: Throwable) {
                        // Ktor fails the body channel with this exception once
                        // writeTo throws (ktor-server-core 3.6.0
                        // `engine/BaseApplicationResponse.kt:185-190`; ktor-utils
                        // `cio/Readers.kt:32-36`; ktor-io
                        // `ByteWriteChannelOperations.kt:123-129`), and ktor-io
                        // reads the cause back as a copy (`CloseToken.kt:17-26`).
                        // A cancelled coroutine throws a JobCancellationException,
                        // whose copy is null unless kotlinx-coroutines runs in
                        // debug mode (1.11.0 `jvm/src/Exceptions.kt:54-63`); the
                        // channel then reads as closed normally
                        // (`ByteChannel.kt:82-99`) and the engine's writer ends the
                        // response as complete (ktor-server-netty
                        // `cio/NettyHttpResponsePipeline.kt:348-381`). Failed first
                        // with a cause ktor-io keeps, the channel fails the writer.
                        channel.cancel(IOException("response aborted", e))
                        throw e
                    }
                    // Must be set before writeTo returns: the body channel closes then.
                    if (result.isNotEmpty()) checkNotNull(trailers) { "trailers on a call without trailer support" }.set(result)
                }
            },
        )
    }

    /**
     * Runs [block], one response, and marks the exchange aborted if it throws
     * or is cancelled. On engines other than Netty the engine's own handling
     * of the failed response applies.
     */
    private inline fun abortOnFailure(block: () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            aborted = true
            throw e
        }
    }

    /**
     * Headers from the server are final (lower-case, validated, including
     * Content-Type); `safeOnly = false` lets Content-Type through, and Ktor
     * keeps it over [OutgoingContent.contentType] (ktor-server-core 3.6.0
     * `engine/BaseApplicationResponse.kt:105-109`).
     */
    private fun setHeaders(headers: Headers) {
        for ((name, values) in headers) {
            for (value in values) call.response.headers.append(name, value, safeOnly = false)
        }
    }
}

/**
 * [ByteWriteChannel.writeFully] suspends while Ktor's channel buffer is full,
 * and the Netty engine awaits each flush's write future
 * (ktor-server-netty 3.6.0 `cio/NettyHttpResponsePipeline.kt:363-371`), so a
 * client that stops reading stops the writer.
 */
private class ChannelSink(private val channel: ByteWriteChannel) : ResponseSink {
    private val chunk = ByteArray(CHUNK)

    override suspend fun write(source: Buffer) {
        while (!source.exhausted()) {
            val n = source.read(chunk)
            channel.writeFully(chunk, 0, n)
        }
    }

    override suspend fun flush() = channel.flush()
}
