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
import okio.DeflaterSink
import okio.ForwardingSource
import okio.InflaterSource
import okio.Source
import okio.buffer
import java.util.zip.Deflater
import java.util.zip.Inflater

/**
 * The `deflate` compression pool: zlib-wrapped (RFC 1950) DEFLATE data; raw
 * DEFLATE is not `deflate` (grpc `doc/compression.md`, "deflate").
 */
object DeflateServerCompressionPool : ServerCompressionPool {
    override fun name(): String = "deflate"

    override fun decompress(source: Source): Source {
        val inflater = Inflater()
        return object : ForwardingSource(InflaterSource(source.buffer(), inflater)) {
            override fun close() {
                try {
                    super.close()
                } finally {
                    inflater.end()
                }
            }
        }
    }

    override fun compress(buffer: Buffer): Buffer {
        val result = Buffer()
        val deflater = Deflater()
        try {
            DeflaterSink(result, deflater).buffer().use { it.writeAll(buffer) }
        } finally {
            deflater.end()
        }
        return result
    }
}
