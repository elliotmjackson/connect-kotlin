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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test

class BinaryHeadersTest {
    @Test
    fun encodesUnpaddedStandardBase64() {
        assertThat(encodeBinaryHeader(byteArrayOf(1, 2))).isEqualTo("AQI")
        assertThat(encodeBinaryHeader(byteArrayOf(-5, -1))).isEqualTo("+/8")
    }

    @Test
    fun decodesPaddedUnpaddedAndCommaJoinedValues() {
        assertThat(decodeBinaryHeaders("AQI")).containsExactly(byteArrayOf(1, 2))
        assertThat(decodeBinaryHeaders("AQI=")).containsExactly(byteArrayOf(1, 2))
        assertThat(decodeBinaryHeaders("AQID,BAUG")).containsExactly(byteArrayOf(1, 2, 3), byteArrayOf(4, 5, 6))
        assertThat(decodeBinaryHeaders(" AQI= , +/8")).containsExactly(byteArrayOf(1, 2), byteArrayOf(-5, -1))
        assertThatThrownBy { decodeBinaryHeaders("AQID,@@") }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun handlersSeeBinaryRequestMetadataVerbatim() {
        val server = server(
            unary { req, ctx ->
                val decoded = ctx.requestHeaders["X-Id-Bin"]!!.flatMap(::decodeBinaryHeaders)
                Msg(decoded.joinToString(";") { it.joinToString(",") })
            },
        )
        val ex = server.call("Unary", "application/proto", "a".toByteArray(), mapOf("x-id-bin" to "AQID,BAUG"))
        assertThat(ex.body.readUtf8()).isEqualTo("1,2,3;4,5,6")
    }
}
