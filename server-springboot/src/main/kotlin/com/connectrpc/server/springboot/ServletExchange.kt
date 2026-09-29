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

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.Headers
import com.connectrpc.server.http.HttpExchange
import com.connectrpc.server.http.ResponseSink
import com.connectrpc.server.http.StreamingBody
import jakarta.servlet.AsyncContext
import jakarta.servlet.AsyncEvent
import jakarta.servlet.AsyncListener
import jakarta.servlet.ReadListener
import jakarta.servlet.ServletInputStream
import jakarta.servlet.ServletOutputStream
import jakarta.servlet.ServletResponse
import jakarta.servlet.ServletResponseWrapper
import jakarta.servlet.WriteListener
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import okio.Buffer
import org.apache.catalina.connector.Response
import org.apache.catalina.connector.ResponseFacade
import org.apache.coyote.ActionCode
import java.io.IOException
import java.lang.reflect.Field
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Scratch buffer size for dropping unread request bytes (see [ServletExchange.complete]). */
private const val DISCARD_BUFFER_BYTES = 8192

/**
 * How long a read waits for the container to report request bytes it already holds as
 * readable before the call fails (see [ServletExchange.readRequestBody]). Tomcat 11 HTTP/2
 * reports them from a task on its executor (StreamProcessor.java:338-344,374-391), so the
 * wait only covers that task's queueing delay.
 */
private val STALLED_BODY_TIMEOUT: Duration = 5.seconds

/**
 * [HttpExchange] over one asynchronous servlet request, using Servlet 6.1
 * non-blocking I/O (Jakarta Servlet 6.1 §3.8 / §5.7, "Non Blocking IO").
 *
 * No thread waits for the network: a read or write that cannot proceed
 * suspends until the container calls [ReadListener.onDataAvailable] or
 * [WriteListener.onWritePossible]. Container callbacks only wake the
 * suspended coroutine; all stream access happens on the coroutine, so the
 * request's reader and writer (full-duplex bidi) run independently.
 *
 * Once the container reports an error, a timeout or completion, [closed] is
 * set before the container can recycle the request, and every later I/O call
 * fails with [IOException] instead of touching recycled objects.
 */
internal class ServletExchange(
    private val request: HttpServletRequest,
    private val response: HttpServletResponse,
    private val async: AsyncContext,
    override val path: String,
) : HttpExchange {
    override val method: String = request.method
    override val rawQuery: String? = request.queryString

    /**
     * `getHeaderNames` lists a field once per spelling, and `getHeaders` already
     * returns the values of every spelling, so names are merged case-insensitively
     * (RFC 9110 §5.1: field names are case-insensitive).
     */
    override val requestHeaders: Headers = buildMap {
        for (name in request.headerNames) {
            val key = name.lowercase(Locale.ROOT)
            if (key !in this) put(key, request.getHeaders(name).toList())
        }
    }

    /**
     * HTTP/2 always carries trailers (RFC 9113 §8.1). On HTTP/1.1 a trailer
     * supplier set before the response is committed makes the container use
     * chunked encoding, which carries them (RFC 9112 §7.1.2; Tomcat 11
     * `Http11Processor.prepareResponse` "If trailer fields are set, always use
     * chunking"). HTTP/1.0 has no trailers.
     */
    override val supportsTrailers: Boolean = request.protocol == "HTTP/2.0" || request.protocol == "HTTP/1.1"

    private val http2: Boolean = request.protocol == "HTTP/2.0"

    private val input: ServletInputStream = request.inputStream
    private val output: ServletOutputStream = response.outputStream
    private val readable = Channel<Unit>(Channel.CONFLATED)
    private val writable = Channel<Unit>(Channel.CONFLATED)
    private val completed = AtomicBoolean()

    @Volatile private var closed: IOException? = null

    @Volatile private var readFailure: Throwable? = null

    @Volatile private var writeFailure: Throwable? = null

    /** Set by the container's error, timeout and completion callbacks. */
    @Volatile private var endedByContainer = false

    /** Set by [abort], before the container is told. */
    @Volatile private var aborted = false

    @Volatile private var trailers: Map<String, String> = emptyMap()

    /**
     * Registers the container callbacks; [call] is cancelled when the client
     * goes away. Must be called on the container thread that started [async].
     */
    fun start(call: Job) {
        async.addListener(Listener(call))
        input.setReadListener(Reader(call))
        output.setWriteListener(Writer(call))
    }

    /** Ends the async request unless the container already has. */
    fun complete() {
        if (!completed.compareAndSet(false, true)) return
        closed = closed ?: IOException("response completed")
        completeAsync()
    }

    private fun completeAsync() {
        try {
            async.complete()
        } catch (e: IllegalStateException) {
            // The container ended the request between the CAS and this call. Tomcat 11 then rejects
            // complete() from application threads once AsyncListener.onError has returned
            // (AsyncContextImpl.check) and completes the request itself (AsyncContextImpl.setErrorState).
            if (!endedByContainer) throw e
        }
    }

    /**
     * Aborts an incomplete response (see [HttpExchange], "Aborting"). The Servlet API
     * has no way to, so this asks Tomcat for `CLOSE_NOW` (see [TomcatResponses]): it
     * drops what is left to write and dispatches [AsyncListener.onError], which completes
     * the request, and then resets its HTTP/2 stream or closes its HTTP/1.1 connection.
     * Other containers get `AsyncContext.complete()`, which commits and closes the
     * response (Servlet 6.1 `AsyncContext#complete`): a stream then ends without its
     * end-of-stream message or `grpc-status`, which clients read as an error, but a
     * unary response whose whole body the container already holds is delivered whole.
     */
    private fun abort() {
        if (closed != null || !completed.compareAndSet(false, true)) return
        aborted = true
        closed = IOException("response aborted")
        readable.trySend(Unit)
        writable.trySend(Unit)
        if (!TomcatResponses.closeNow(response)) completeAsync()
    }

    /** Runs [block], one response, and aborts the exchange if it throws or is cancelled. */
    private inline fun abortOnFailure(block: () -> Unit) {
        try {
            block()
        } catch (e: Throwable) {
            try {
                abort()
            } catch (failed: RuntimeException) {
                // The container ended the request meanwhile; its objects are no longer usable (see [io]).
                e.addSuppressed(failed)
            }
            throw e
        }
    }

    /**
     * Called when [com.connectrpc.server.ConnectServer.serve] threw instead of
     * returning (an [Error] from a handler). An uncommitted response becomes a
     * bare 500. A response already being written was aborted when the error
     * cancelled it, so clients never see its end-of-stream message or
     * `grpc-status` trailer and report an error.
     */
    fun fail() {
        if (closed != null) return
        try {
            if (!response.isCommitted) {
                response.reset()
                response.status = 500
            }
        } catch (e: IllegalStateException) {
            // Committed or recycled by the container meanwhile; nothing left to change.
        }
    }

    /**
     * Reads and drops request body bytes the container still holds, so their HTTP/2
     * flow-control credit goes back to the client. Tomcat 11 (at least 11.0.22-11.0.26)
     * returns credit for bytes the application reads, or swallows them when it resets
     * the stream (`Stream.close` → `swallowUnread`, Stream.java:822-825), but a stream
     * that ends without an error after the client's END_STREAM, with bytes still
     * buffered, is only replaced (StreamProcessor.java:132-136, Stream.java:843-853) and
     * those bytes stay counted against the connection window. That window is Tomcat's
     * `initialWindowSize`, 65,535 bytes by default (Http2UpgradeHandler.java:679-695), so
     * a few responses that leave their request unread (an unknown method, a deadline, an
     * early error) stop the client sending any request body on the connection; the calls
     * on it then wait until Tomcat's `streamReadTimeout` resets them with INTERNAL_ERROR.
     *
     * Runs in [AsyncListener.onComplete], on the container thread that then finishes the
     * response and checks the stream (AbstractProcessorLight.java:56-95,
     * AsyncStateMachine.java:280-287), so it also catches bytes that arrived after the
     * handler stopped reading. The container has removed the non-blocking listeners by
     * then (AsyncStateMachine.java:268-271,318), so reads are blocking and only
     * `available()` bytes are read. Bytes that arrive later are either swallowed by
     * Tomcat's reset for an unfinished request (StreamProcessor.java:108-116) or, in the
     * short time before that check, still lost.
     */
    private fun discardBufferedRequestBody() {
        var scratch: ByteArray? = null
        try {
            while (input.available() > 0) {
                val buffer = scratch ?: ByteArray(DISCARD_BUFFER_BYTES).also { scratch = it }
                if (input.read(buffer) < 0) return
            }
        } catch (e: IOException) {
            // The client reset the stream; Tomcat swallows its bytes on that path.
        } catch (e: RuntimeException) {
            // The request's objects are no longer usable (see [io]).
        }
    }

    /**
     * On Tomcat 11 HTTP/2 (11.0.22, unchanged in 11.0.26), `isReady()` can return false and
     * register no read interest (Stream.java:1336-1337). No `onDataAvailable` or
     * `onAllDataRead` follows (Stream.java:1386-1395), and every later `isReady()` returns
     * false (coyote/Request.java:236-240). This happens when the request's last frame lands
     * inside `isReady()`:
     *
     * - END_STREAM with nothing left to read, arriving after `isReady()`'s own `isFinished()`
     *   check (connector/InputBuffer.java:250): `isFinished()` is true from then on.
     * - A DATA frame with END_STREAM, arriving between the two reads of
     *   `Stream.isRequestBodyFullyRead` (see [bodyFinished]): it stays unread with
     *   `available()` above zero. No Servlet call makes it readable, so the call fails with
     *   UNAVAILABLE once [STALLED_BODY_TIMEOUT] passes without the container reporting it.
     *
     * `available()` is only asked on HTTP/2: on HTTP/1.1 it reads from the socket on the
     * calling thread (connector/InputBuffer.java:202-209, Http11InputBuffer.java:640-665).
     */
    override suspend fun readRequestBody(sink: Buffer, maxBytes: Long): Long {
        require(maxBytes > 0) { "maxBytes must be > 0" }
        while (true) {
            ensureOpen()
            readFailure?.let { throw IOException("request body read failed", it) }
            // Tomcat's isReady() is false once the body is finished, so check that first.
            // End of body comes from the stream; onAllDataRead only wakes this reader.
            val ready = io { if (bodyFinished()) return -1 else input.isReady }
            if (ready) {
                val read = io { readInto(sink, minOf(maxBytes, Int.MAX_VALUE.toLong()).toInt()) }
                if (read != 0) return read.toLong()
            } else if (io { bodyFinished() }) {
                return -1
            } else if (http2 && io { input.available() } > 0) {
                awaitBufferedBody()
            } else {
                readable.receive()
            }
        }
    }

    /**
     * Tomcat 11 HTTP/2 (11.0.22, unchanged in 11.0.26) buffers a DATA frame and then records
     * its END_STREAM without the stream's read lock (Http2Parser.java:186-212), and
     * Stream.isRequestBodyFullyRead reads the buffer position before that state
     * (Stream.java:1356). One `isFinished()` can therefore be true while the final frame is
     * still unread; once one call has seen END_STREAM, the next reads the position after it
     * and sees the frame.
     */
    private fun bodyFinished(): Boolean = input.isFinished && input.isFinished

    /** Waits for the container to report buffered request bytes as readable (see [readRequestBody]). */
    private suspend fun awaitBufferedBody() {
        if (withTimeoutOrNull(STALLED_BODY_TIMEOUT) { readable.receive() } != null) return
        if (io { input.isReady }) return
        throw ConnectException(Code.UNAVAILABLE, "the servlet container did not make the received request body readable")
    }

    /** Reads straight into [sink]'s tail segment; returns the byte count, or -1 at end of body. */
    private fun readInto(sink: Buffer, maxBytes: Int): Int {
        val cursor = sink.readAndWriteUnsafe()
        try {
            val oldSize = sink.size
            val capacity = cursor.expandBuffer(1)
            val read = input.read(cursor.data!!, cursor.start, minOf(capacity, maxBytes.toLong()).toInt())
            cursor.resizeBuffer(oldSize + maxOf(read, 0))
            return read
        } finally {
            cursor.close()
        }
    }

    override suspend fun respond(status: Int, headers: Headers, body: ByteArray) = abortOnFailure {
        io {
            writeHead(status, headers)
            response.setContentLengthLong(body.size.toLong())
        }
        if (body.isNotEmpty()) {
            awaitWritable()
            io { output.write(body) }
        }
        // Push the body to the connection before complete(), rather than leaving it
        // in the container's buffer for the completion path to write.
        awaitWritable()
        io { output.flush() }
        awaitWritable()
    }

    override suspend fun respondStreaming(status: Int, headers: Headers, body: StreamingBody) = abortOnFailure {
        io {
            writeHead(status, headers)
            // Must precede commit (Servlet 6.1 HttpServletResponse#setTrailerFields).
            if (supportsTrailers) response.setTrailerFields { trailers }
        }
        awaitWritable()
        io { output.flush() }
        val result = body.writeTo(Sink())
        // Servlet trailers are single-valued; repeated fields are combined as RFC 9110 §5.3
        // allows, with "," alone so `-bin` values survive gRPC's split on ","
        // (PROTOCOL-HTTP2.md "Custom-Metadata").
        trailers = result.mapValues { (_, values) -> values.joinToString(",") }
        awaitWritable()
    }

    private fun writeHead(status: Int, headers: Headers) {
        response.status = status
        for ((name, values) in headers) {
            for (value in values) response.addHeader(name, value)
        }
    }

    private inner class Sink : ResponseSink {
        override suspend fun write(source: Buffer) {
            while (!source.exhausted()) {
                awaitWritable()
                io {
                    val cursor = source.readUnsafe()
                    val written = try {
                        cursor.seek(0)
                        val count = cursor.end - cursor.start
                        output.write(cursor.data!!, cursor.start, count)
                        count
                    } finally {
                        cursor.close()
                    }
                    source.skip(written.toLong())
                }
            }
        }

        override suspend fun flush() {
            awaitWritable()
            io { output.flush() }
        }
    }

    /** Suspends until a write or flush will not block (Servlet 6.1 ServletOutputStream#isReady). */
    private suspend fun awaitWritable() {
        while (true) {
            ensureOpen()
            writeFailure?.let { throw IOException("response write failed", it) }
            if (io { output.isReady }) return
            writable.receive()
        }
    }

    private fun ensureOpen() {
        closed?.let { throw IOException("request is no longer active", it) }
    }

    /**
     * Runs one non-blocking container call. After the container has finished
     * the request its objects are recycled and throw unchecked exceptions
     * (Tomcat `discardFacades`); those become the transport failure they are.
     */
    private inline fun <T> io(block: () -> T): T {
        ensureOpen()
        try {
            return block()
        } catch (e: RuntimeException) {
            closed?.let { throw IOException("request is no longer active", e) }
            throw e
        }
    }

    private fun close(cause: IOException, call: Job) {
        if (closed == null) closed = cause
        // An aborted call ends by itself; cancelling it would change the code it reports.
        if (!aborted) call.cancel(CancellationException("client disconnected", cause))
        readable.trySend(Unit)
        writable.trySend(Unit)
    }

    private inner class Reader(private val call: Job) : ReadListener {
        override fun onDataAvailable() {
            readable.trySend(Unit)
        }

        override fun onAllDataRead() {
            readable.trySend(Unit)
        }

        override fun onError(t: Throwable) {
            readFailure = t
            close(IOException("request body read failed", t), call)
        }
    }

    private inner class Writer(private val call: Job) : WriteListener {
        override fun onWritePossible() {
            writable.trySend(Unit)
        }

        override fun onError(t: Throwable) {
            writeFailure = t
            close(IOException("response write failed", t), call)
        }
    }

    /**
     * Client disconnects (HTTP/1 connection close, HTTP/2 RST_STREAM) reach the
     * application as [AsyncListener.onError] (Tomcat 11 `Stream.receiveReset`
     * dispatches `DISPATCH_ERROR`). Completing inside `onError` stops the
     * container from dispatching to the error page (Servlet 6.1 §2.3.3.3).
     */
    private inner class Listener(private val call: Job) : AsyncListener {
        override fun onComplete(event: AsyncEvent) {
            // Only after complete() from the finished call: then no coroutine touches the
            // request any more. On the container's error path Tomcat resets the stream,
            // and an aborted response must not be ended as if it were whole.
            if (completed.get() && !endedByContainer && !aborted) {
                if (http2 && readFailure == null) discardBufferedRequestBody()
                if (writeFailure == null) endResponse()
            }
            endedByContainer = true
            completed.set(true)
            close(IOException("request completed by the container"), call)
        }

        /**
         * Writes the end of the response (HTTP/2 END_STREAM and trailers, the HTTP/1.1
         * last chunk) before Tomcat counts the request as finished. Tomcat lowers the
         * context's in-progress async count right after `onComplete` returns
         * (AsyncStateMachine.java:284-286) and only then finishes the response
         * (CoyoteAdapter.java:244-247). Spring Boot's graceful shutdown stops the web
         * server once that count is zero (spring-boot-tomcat 4.1.0
         * `GracefulShutdown.java:98-117`), and that check starts right as the calls
         * ended by [ConnectServerLifecycle] complete, so without this the connection
         * could close before the end of their responses was written. Reads are over
         * by now: closing the output also closes the request body (OutputBuffer.java:235-239),
         * and the container has removed the non-blocking listeners, so this write blocks
         * as Tomcat's own would.
         */
        private fun endResponse() {
            try {
                output.close()
            } catch (e: IOException) {
                // The client left; Tomcat ends the stream on its error path.
            } catch (e: RuntimeException) {
                // The request's objects are no longer usable (see [io]).
            }
        }

        override fun onTimeout(event: AsyncEvent) = containerEnded(IOException("async request timed out"))

        override fun onError(event: AsyncEvent) = containerEnded(IOException("async request failed", event.throwable))

        private fun containerEnded(cause: IOException) {
            endedByContainer = true
            close(cause, call)
            // abort() leaves completing the request to this dispatch.
            if (aborted) completeAsync() else complete()
        }

        override fun onStartAsync(event: AsyncEvent) = Unit
    }
}

/**
 * Tomcat's abort, reached through the connector response behind Tomcat's
 * [HttpServletResponse] facade, which keeps it in a protected field; spring-web
 * 7.0.8 `TomcatHttpHandlerAdapter` reads the same field.
 */
private object TomcatResponses {
    private const val FACADE = "org.apache.catalina.connector.ResponseFacade"

    /** Asks Tomcat to abort [response]'s request (see [ServletExchange]); false on other containers. */
    fun closeNow(response: ServletResponse): Boolean {
        var unwrapped = response
        while (unwrapped is ServletResponseWrapper) unwrapped = unwrapped.response
        return unwrapped.javaClass.name == FACADE && Tomcat.closeNow(unwrapped)
    }

    /** Tomcat classes are only loaded through here, once a Tomcat facade has been found. */
    private object Tomcat {
        private val connectorResponse: Field? = try {
            ResponseFacade::class.java.getDeclaredField("response").apply { isAccessible = true }
        } catch (e: ReflectiveOperationException) {
            null
        } catch (e: RuntimeException) {
            null
        }

        /**
         * `ActionCode.CLOSE_NOW` drops what is left to write and sets the error state
         * `CLOSE_NOW`, which from a non-container thread dispatches the async request's
         * error (Tomcat 11.0.22 `AbstractProcessor.java:102-126, 433-441`). When that
         * dispatch ends, the processor closes the HTTP/1.1 connection or resets the
         * HTTP/2 stream (`AbstractProcessor.java:255-260`, `http2/StreamProcessor.java:123-131`).
         */
        fun closeNow(facade: ServletResponse): Boolean {
            val response = connectorResponse?.get(facade) as? Response ?: return false
            response.coyoteResponse.action(ActionCode.CLOSE_NOW, null)
            return true
        }
    }
}
