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

import com.connectrpc.ConnectException
import com.connectrpc.server.internal.Query
import com.connectrpc.server.internal.canonicalContentType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import java.util.Random

/** URL and Content-Type parsers, over generated inputs where the grammar allows. */
class ParsersTest {
    @Test
    fun queryDecodesAnyPercentEncodedBytes() {
        val random = Random(3)
        repeat(500) {
            val name = "p" + String(CharArray(random.nextInt(6)) { 'a' + random.nextInt(26) })
            val value = ByteArray(random.nextInt(40)) { random.nextInt(256).toByte() }
            val parsed = Query.parse("x=1&${encode(name.toByteArray(), random)}=${encode(value, random)}&y")
            assertThat(parsed.bytes(name)).describedAs(value.contentToString()).isEqualTo(value)
        }
    }

    @Test
    fun queryParameterRules() {
        val query = Query.parse("b=2&a=1&&a=3&flag&sp=a+b&plus=a%2Bb&e=&%61%62=c&raw=é")
        // Any order; the first of repeated names wins (Go's `url.Values.Get`).
        assertThat(query.first("a")).isEqualTo("1")
        assertThat(query.first("b")).isEqualTo("2")
        assertThat(query.first("flag")).isEqualTo("")
        assertThat(query.first("sp")).isEqualTo("a b")
        assertThat(query.first("plus")).isEqualTo("a+b")
        assertThat(query.first("e")).isEqualTo("")
        assertThat(query.first("ab")).isEqualTo("c")
        assertThat(query.first("raw")).isEqualTo("é")
        assertThat(query.first("missing")).isNull()
        assertThat(query.strings()).containsEntry("a", listOf("1", "3")).containsEntry("flag", listOf(""))
        assertThat(Query.parse(null).first("a")).isNull()
        assertThat(Query.parse("").strings()).isEmpty()
        for (bad in listOf("a=%", "a=%4", "a=%zz", "a=%g0", "%=1", "a=%\u0664\u0661", "a=%\uff21\uff21")) {
            assertThatThrownBy { Query.parse(bad) }.describedAs(bad).isInstanceOf(ConnectException::class.java)
        }
    }

    @Test
    fun contentTypeCanonicalisation() {
        val cases = mapOf(
            "application/json" to "application/json",
            "Application/Grpc+Proto" to "application/grpc+proto",
            "  application/proto  " to "application/proto",
            "application/json;charset=utf-8" to "application/json",
            "application/json; Charset=\"UTF-8\"" to "application/json",
            "application/json;" to "application/json",
            "application/json; ; charset=utf-8" to "application/json",
            "application/json; charset=latin1" to null,
            "application/json; boundary=x" to null,
            "application/json; charset=utf-8; q=1" to null,
            "application/json; charset" to null,
        )
        for ((input, expected) in cases) {
            assertThat(canonicalContentType(input)).describedAs(input).isEqualTo(expected)
        }
    }

    /** Percent-encodes [bytes], choosing per byte between literal (when unreserved) and `%XX` in either case. */
    private fun encode(bytes: ByteArray, random: Random): String {
        val sb = StringBuilder()
        for (b in bytes) {
            val u = b.toInt() and 0xff
            val unreserved = u.toChar().let { it in 'a'..'z' || it in 'A'..'Z' || it in '0'..'9' || it in "-._~" }
            when {
                unreserved && random.nextBoolean() -> sb.append(u.toChar())
                u == ' '.code && random.nextBoolean() -> sb.append('+')
                else -> sb.append('%').append(String.format(if (random.nextBoolean()) "%02X" else "%02x", u))
            }
        }
        return sb.toString()
    }
}
