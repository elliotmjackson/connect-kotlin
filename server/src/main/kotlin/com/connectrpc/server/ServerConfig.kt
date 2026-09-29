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

import com.connectrpc.server.compression.DeflateServerCompressionPool
import com.connectrpc.server.compression.GzipServerCompressionPool
import com.connectrpc.server.compression.ServerCompressionPool
import kotlin.time.Duration

/**
 * Set of configuration used to set up a [ConnectServer].
 *
 * Without [maxTimeout], a call ends at the deadline the client sends
 * (`Connect-Timeout-Ms`, `grpc-timeout`) and otherwise only when the client
 * finishes or disconnects, so idle and slow clients, including clients that
 * stop reading the response, are bounded by the HTTP server's own timeouts
 * (for Ktor, see the `server-ktor` README).
 *
 * @param readMaxBytes The largest request message accepted, in bytes, both on
 *     the wire and after decompression. Larger messages fail with
 *     `resource_exhausted` without being buffered. Zero disables the limit.
 *     Defaults to 4 MiB, as in connect-go (`connecthttp/protocol.go:41-43`).
 * @param sendMaxBytes The largest response message sent, in bytes, before
 *     compression. Sending a larger message fails with `resource_exhausted`.
 *     Zero disables the limit.
 * @param compressionPools The encodings accepted on requests and used on
 *     responses, advertised in this order. Replaces the default gzip and
 *     deflate; empty disables compression.
 * @param compressMinBytes Response messages smaller than this, in bytes, are
 *     sent uncompressed.
 * @param requireConnectProtocolHeader Whether to reject Connect unary requests
 *     without `Connect-Protocol-Version: 1` (GET: without `connect=v1`), which
 *     the protocol allows servers to require.
 * @param maxTimeout The longest a call may read its request and run its
 *     handler. A call whose client sends a longer timeout, or none, gets
 *     this one instead, as protocol.md allows ("server implementations may
 *     clamp timeouts to an appropriate maximum", lines 191-194 and 290-293),
 *     and fails with `deadline_exceeded` when it passes. The deadline does
 *     not bound writing the response, as in connect-go v1.20.0, whose
 *     deadline is the handler's context (`handler.go:317-326`) while
 *     response bytes go straight to the `http.ResponseWriter`
 *     (`duplex_http_call.go:397-405`), and connect-es, whose deadline is the
 *     handler context's signal (`implementation.ts:161-177`): a client that
 *     stops reading holds its response until the HTTP server's own write
 *     limits end the stream or connection (see the adapter's docs).
 *     connect-es rejects longer timeouts with `invalid_argument` instead
 *     (`maxTimeoutMs`, `protocol/universal-handler.ts:95-99`), which fails
 *     well-behaved clients whose default is longer and leaves calls without
 *     a timeout unbounded. Null, the default, sets no limit.
 * @param observer Told of the start and end of every request, for metrics and
 *     tracing; see [ServerObserver].
 */
class ServerConfig(
    val readMaxBytes: Long = DEFAULT_READ_MAX_BYTES,
    val sendMaxBytes: Long = 0,
    val compressionPools: List<ServerCompressionPool> = listOf(GzipServerCompressionPool, DeflateServerCompressionPool),
    val compressMinBytes: Long = 1024,
    val requireConnectProtocolHeader: Boolean = false,
    val maxTimeout: Duration? = null,
    val observer: ServerObserver? = null,
) {
    init {
        require(readMaxBytes >= 0) { "readMaxBytes must be >= 0" }
        require(sendMaxBytes >= 0) { "sendMaxBytes must be >= 0" }
        require(compressMinBytes >= 0) { "compressMinBytes must be >= 0" }
        require(maxTimeout == null || maxTimeout.isPositive()) { "maxTimeout must be positive" }
        val names = compressionPools.map { it.name() }
        require(names.size == names.toSet().size) { "duplicate compression pool name in $names" }
    }

    companion object {
        /** 4 MiB, connect-go v2's default read limit (`connecthttp/protocol.go:41-43`). */
        const val DEFAULT_READ_MAX_BYTES: Long = 4L * 1024 * 1024
    }
}
