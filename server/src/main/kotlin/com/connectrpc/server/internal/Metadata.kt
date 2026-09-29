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

import com.connectrpc.Headers
import java.util.logging.Level

/** Read-only headers keyed by lower-case name; lookups ignore case. */
internal class CaseInsensitiveHeaders private constructor(
    private val map: Map<String, List<String>>,
) : Map<String, List<String>> by map {
    override fun get(key: String): List<String>? = map[key.lowercase()]

    override fun containsKey(key: String): Boolean = map.containsKey(key.lowercase())

    override fun toString(): String = map.toString()

    override fun equals(other: Any?): Boolean = map == other

    override fun hashCode(): Int = map.hashCode()

    companion object {
        fun of(headers: Headers): Headers {
            if (headers is CaseInsensitiveHeaders) return headers
            val out = LinkedHashMap<String, MutableList<String>>(headers.size)
            for ((name, values) in headers) {
                out.getOrPut(name.lowercase()) { ArrayList(values.size) }.addAll(values)
            }
            return CaseInsensitiveHeaders(out)
        }
    }
}

/** First value of [name] in [headers], ignoring name case. */
internal fun Headers.first(name: String): String? = this[name]?.firstOrNull()

/**
 * Names the protocols own. Application metadata with these names never
 * reaches the wire, so it cannot override or duplicate protocol fields
 * (connect-go `connecthttp/header.go:24-45`; protocol.md reserves the
 * `connect-` prefix; PROTOCOL-HTTP2.md "Custom-Metadata" reserves `grpc-`).
 */
private val PROTOCOL_HEADERS = setOf(
    "content-type",
    "content-length",
    "content-encoding",
    "accept-encoding",
    "transfer-encoding",
    "trailer",
    "date",
)

internal fun isReservedName(name: String): Boolean = name in PROTOCOL_HEADERS || name.startsWith("connect-") || name.startsWith("grpc-")

/** RFC 9110 §5.6.2 `tchar`. */
private fun isTokenChar(c: Char): Boolean = c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c in "!#$%&'*+-.^_`|~"

internal fun isToken(name: String): Boolean = name.isNotEmpty() && name.all(::isTokenChar)

/**
 * Replaces characters that may not appear in a field value with SP. RFC 9110
 * §5.5: CR, LF and NUL "MUST" be rejected or replaced with SP, other CTLs are
 * invalid. Go's `http.Header.Write` does the same for CR and LF
 * (`net/http/header.go:139, 206`).
 */
internal fun sanitizeValue(value: String): String {
    var i = 0
    while (i < value.length && !isInvalidValueChar(value[i])) i++
    if (i == value.length) return value
    val chars = value.toCharArray()
    for (j in i until chars.size) {
        if (isInvalidValueChar(chars[j])) chars[j] = ' '
    }
    return String(chars)
}

private fun isInvalidValueChar(c: Char): Boolean = (c < ' ' && c != '\t') || c == '\u007f'

/**
 * True when every character is ASCII. protocol.md and PROTOCOL-HTTP2.md
 * define non-binary metadata values as ASCII-Value (%x20-%x7E); HPACK
 * encoders reject wider characters and HTTP/1.1 stacks drop them. ASCII
 * controls are left to [sanitizeValue].
 */
private fun isAsciiValue(value: String): Boolean = value.all { it <= '\u007f' }

/**
 * Collects response metadata with lower-case names. Application entries
 * with reserved or invalid names are dropped. Non-ASCII values are dropped,
 * as connect-go's HTTP/2 transport omits invalid values
 * (`golang.org/x/net/http2/write.go:369-373`); other values are sanitised.
 */
internal class MetadataBuilder {
    private val map = LinkedHashMap<String, MutableList<String>>()

    /** Adds a protocol-owned field. */
    fun set(name: String, value: String): MetadataBuilder = apply { map[name] = mutableListOf(value) }

    /** Adds application metadata, optionally renaming each entry with [prefix]. */
    fun addApplication(metadata: Map<String, List<String>>, prefix: String = ""): MetadataBuilder = apply {
        for ((rawName, values) in metadata) {
            val name = rawName.lowercase()
            if (values.isEmpty() || !isToken(name) || isReservedName(name)) continue
            val kept = ArrayList<String>(values.size)
            for (value in values) {
                if (isAsciiValue(value)) {
                    kept.add(sanitizeValue(value))
                } else {
                    LOG.log(Level.FINE, "dropped a non-ASCII value of metadata \"$name\"")
                }
            }
            if (kept.isNotEmpty()) map.getOrPut(prefix + name) { ArrayList(kept.size) }.addAll(kept)
        }
    }

    /** A snapshot; later changes to this builder do not affect it. */
    fun build(): Headers = map.mapValuesTo(LinkedHashMap(map.size)) { it.value.toList() }
}
