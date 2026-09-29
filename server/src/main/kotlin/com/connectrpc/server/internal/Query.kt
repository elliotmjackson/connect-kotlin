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

package com.connectrpc.server.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import java.io.ByteArrayOutputStream

/**
 * A parsed URL query in any parameter order, unknown parameters kept
 * (protocol.md "Unary-Get-Request": servers "MUST" accept both). Values
 * are percent-decoded to bytes, with `+` as space as in HTML form encoding
 * (Go's `url.ParseQuery`, which connect-go uses).
 */
internal class Query private constructor(private val params: List<Pair<String, ByteArray>>) {
    fun bytes(name: String): ByteArray? = params.firstOrNull { it.first == name }?.second

    fun first(name: String): String? = bytes(name)?.toString(Charsets.UTF_8)

    fun strings(): Map<String, List<String>> = params.groupBy({ it.first }, { it.second.toString(Charsets.UTF_8) })

    companion object {
        fun parse(raw: String?): Query {
            if (raw.isNullOrEmpty()) return Query(emptyList())
            val params = raw.split('&').filter { it.isNotEmpty() }.map { pair ->
                val eq = pair.indexOf('=')
                val name = if (eq < 0) pair else pair.substring(0, eq)
                val value = if (eq < 0) "" else pair.substring(eq + 1)
                decode(name).toString(Charsets.UTF_8) to decode(value)
            }
            return Query(params)
        }

        /** RFC 3986 §2.1 HEXDIG: ASCII only, unlike [Char.digitToIntOrNull]. */
        private fun hexValue(c: Char): Int? = when (c) {
            in '0'..'9' -> c - '0'
            in 'a'..'f' -> c - 'a' + 10
            in 'A'..'F' -> c - 'A' + 10
            else -> null
        }

        private fun decode(s: String): ByteArray {
            val out = ByteArrayOutputStream(s.length)
            var i = 0
            while (i < s.length) {
                val c = s[i]
                when {
                    c == '+' -> out.write(' '.code)

                    c == '%' -> {
                        val hi = s.getOrNull(i + 1)?.let(::hexValue)
                        val lo = s.getOrNull(i + 2)?.let(::hexValue)
                        if (hi == null || lo == null) throw ConnectException(Code.INVALID_ARGUMENT, "malformed percent-encoding in query")
                        out.write((hi shl 4) or lo)
                        i += 2
                    }

                    c.code < 0x80 -> out.write(c.code)

                    else -> out.write(c.toString().toByteArray(Charsets.UTF_8))
                }
                i++
            }
            return out.toByteArray()
        }
    }
}
