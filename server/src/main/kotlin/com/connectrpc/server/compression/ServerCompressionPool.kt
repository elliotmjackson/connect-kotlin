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

package com.connectrpc.server.compression

import okio.Buffer
import okio.Source

/**
 * Conforming types provide the functionality to compress responses and
 * decompress requests using a specific algorithm.
 *
 * Unlike the client's [com.connectrpc.compression.CompressionPool], decompression
 * is streaming, so the server stops reading once a message exceeds
 * [com.connectrpc.server.ServerConfig.readMaxBytes]; a small compressed payload
 * can expand without bound (connect-go bounds its decompressor the same way,
 * `connecthttp/compression.go:39-65`).
 */
interface ServerCompressionPool {
    /**
     * The name of the compression pool, which corresponds to the `content-encoding`,
     * `connect-content-encoding` and `grpc-encoding` headers. Example: `gzip`.
     */
    fun name(): String

    /**
     * Returns a source of the decompressed bytes of [source]. Closing the
     * returned source releases the decompressor and closes [source].
     */
    fun decompress(source: Source): Source

    /**
     * Compresses [buffer], consuming it.
     *
     * @return The compressed data.
     */
    fun compress(buffer: Buffer): Buffer
}
