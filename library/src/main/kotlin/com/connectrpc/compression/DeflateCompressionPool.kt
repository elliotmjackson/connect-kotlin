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

package com.connectrpc.compression

import okio.Buffer
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream

/**
 * Implements the `deflate` content-coding (RFC 1950 zlib-wrapped DEFLATE).
 * Built on top of the JDK's [java.util.zip] classes — no extra dependency.
 */
object DeflateCompressionPool : CompressionPool {
    override fun name(): String = "deflate"

    override fun compress(input: Buffer): Buffer {
        val raw = input.readByteArray()
        val out = java.io.ByteArrayOutputStream()
        DeflaterOutputStream(out, Deflater(Deflater.DEFAULT_COMPRESSION, /* nowrap = */ false)).use { it.write(raw) }
        return Buffer().apply { write(out.toByteArray()) }
    }

    override fun decompress(input: Buffer): Buffer {
        if (input.size == 0L) return Buffer()
        val raw = input.readByteArray()
        val out = java.io.ByteArrayOutputStream()
        InflaterInputStream(java.io.ByteArrayInputStream(raw), Inflater(/* nowrap = */ false))
            .use { it.copyTo(out) }
        return Buffer().apply { write(out.toByteArray()) }
    }
}
