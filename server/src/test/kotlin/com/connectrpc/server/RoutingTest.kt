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

import com.connectrpc.Idempotency
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class RoutingTest {
    private val server = server(
        unary { req, _ -> Msg("echo:${req.text}") },
        unary("Get", Idempotency.NO_SIDE_EFFECTS) { req, _ -> req },
        serverStream { req, _, s -> s.send(req) },
    )

    @Test
    fun unknownProcedureIs404() {
        assertThat(server.call("Nope", "application/proto").status).isEqualTo(404)
        assertThat(server.call("unary", "application/proto").status).isEqualTo(404)
    }

    @Test
    fun unsupportedMethodIs405WithAllow() {
        val put = server.call("Unary", "application/proto", method = "PUT")
        assertThat(put.status).isEqualTo(405)
        assertThat(put.header("allow")).isEqualTo("POST")
        val getNotIdempotent = server.call("Unary", null, method = "GET", query = "encoding=proto&message=x")
        assertThat(getNotIdempotent.status).isEqualTo(405)
        val delete = server.call("Get", "application/proto", method = "DELETE")
        assertThat(delete.header("allow")).isEqualTo("GET, POST")
    }

    @Test
    fun unsupportedContentTypeIs415WithAcceptPost() {
        for (contentType in listOf(null, "text/plain", "application/xml", "application/json; charset=latin1", "application/grpc+xml")) {
            val ex = server.call("Unary", contentType, "a".toByteArray())
            assertThat(ex.status).describedAs(contentType).isEqualTo(415)
            assertThat(ex.header("accept-post")).isEqualTo(
                "application/grpc, application/grpc+json, application/grpc+proto, application/grpc-web, " +
                    "application/grpc-web+json, application/grpc-web+proto, application/json, application/proto",
            )
        }
    }

    @Test
    fun contentTypeMustMatchStreamType() {
        assertThat(server.call("Unary", "application/connect+proto", env(0, "a")).status).isEqualTo(415)
        val ex = server.call("ServerStream", "application/proto", "a".toByteArray())
        assertThat(ex.status).isEqualTo(415)
        assertThat(ex.header("accept-post")).contains("application/connect+proto").doesNotContain("application/proto,")
    }

    @Test
    fun contentTypeIsCanonicalised() {
        val mixed = server.call("Unary", "Application/JSON", "a".toByteArray())
        assertThat(mixed.status).isEqualTo(200)
        assertThat(mixed.header("content-type")).isEqualTo("Application/JSON")
        val charset = server.call("Unary", "application/json; charset=UTF-8", "a".toByteArray())
        assertThat(charset.status).isEqualTo(200)
        assertThat(charset.header("content-type")).isEqualTo("application/json; charset=UTF-8")
        assertThat(charset.body.readUtf8()).isEqualTo("echo:a")
    }
}
