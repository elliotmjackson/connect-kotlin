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
import okio.GzipSink
import okio.GzipSource
import okio.Source
import okio.buffer

/**
 * The `gzip` compression pool (RFC 1952).
 */
object GzipServerCompressionPool : ServerCompressionPool {
    override fun name(): String = "gzip"

    override fun decompress(source: Source): Source = GzipSource(source)

    override fun compress(buffer: Buffer): Buffer {
        val result = Buffer()
        GzipSink(result).buffer().use { it.writeAll(buffer) }
        return result
    }
}
