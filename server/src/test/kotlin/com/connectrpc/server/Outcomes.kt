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

import com.connectrpc.Code
import com.squareup.moshi.Moshi
import okio.Buffer
import java.io.ByteArrayOutputStream

private val jsonAdapter = Moshi.Builder().build().adapter(Any::class.java)

/** Parses [json] with Moshi, an independent check on the server's hand-written JSON. */
@Suppress("UNCHECKED_CAST")
fun parseJson(json: String): Map<String, Any?> = jsonAdapter.fromJson(json) as Map<String, Any?>

/** What a client of any protocol sees at the end of a call. */
data class Outcome(
    val messages: List<String>,
    /** Null on success. */
    val code: Code?,
    val message: String?,
    /** Application trailers, protocol fields removed. */
    val trailers: Map<String, List<String>>,
)

/** The enveloped protocols, each with a client-side reading of the response. Connect unary is not enveloped. */
enum class Enveloped(val contentType: String, val encodingHeader: String) {
    CONNECT("application/connect+proto", "connect-content-encoding") {
        override fun outcome(exchange: FakeExchange): Outcome {
            val all = frames(exchange.bodyBytes)
            val end = all.last()
            check(end.flags == 0x02) { "last Connect frame has flags ${end.flags}" }
            val json = parseJson(end.text)

            @Suppress("UNCHECKED_CAST")
            val error = json["error"] as Map<String, Any?>?

            @Suppress("UNCHECKED_CAST")
            val metadata = (json["metadata"] as Map<String, List<String>>?).orEmpty()
            return Outcome(
                messages = all.dropLast(1).map { it.text },
                code = error?.let { Code.fromName(it["code"] as String) },
                message = error?.get("message") as String?,
                trailers = metadata,
            )
        }
    },
    GRPC("application/grpc", "grpc-encoding") {
        override fun outcome(exchange: FakeExchange): Outcome = grpcOutcome(frames(exchange.bodyBytes).map { it.text }, statusFields(exchange))

        // Trailers-Only responses carry the status in the headers (PROTOCOL-HTTP2.md "Trailers-Only").
        override fun statusFields(exchange: FakeExchange): Map<String, List<String>> = exchange.trailers.ifEmpty { exchange.headers }
    },
    GRPC_WEB("application/grpc-web", "grpc-encoding") {
        override fun outcome(exchange: FakeExchange): Outcome {
            val all = frames(exchange.bodyBytes)
            return grpcOutcome(all.filter { it.flags != 0x80 }.map { it.text }, statusFields(exchange))
        }

        /** The trailer frame's HTTP/1 header block (PROTOCOL-WEB.md "Message framing"), or the headers of a Trailers-Only response. */
        override fun statusFields(exchange: FakeExchange): Map<String, List<String>> {
            check(exchange.trailers.isEmpty()) { "gRPC-Web sent HTTP trailers" }
            val end = frames(exchange.bodyBytes).lastOrNull() ?: return exchange.headers
            check(end.flags == 0x80) { "last gRPC-Web frame has flags ${end.flags}" }
            return end.text.split("\r\n").filter { it.isNotEmpty() }.groupBy({ it.substringBefore(": ") }, { it.substringAfter(": ") })
        }
    },
    ;

    abstract fun outcome(exchange: FakeExchange): Outcome

    /** gRPC status and trailer fields as sent. */
    open fun statusFields(exchange: FakeExchange): Map<String, List<String>> = throw UnsupportedOperationException("$this has no gRPC status fields")
}

private fun grpcOutcome(messages: List<String>, fields: Map<String, List<String>>): Outcome {
    val status = checkNotNull(fields["grpc-status"]) { "no grpc-status in $fields" }.single().toInt()
    return Outcome(
        messages = messages,
        code = if (status == 0) null else Code.fromValue(status),
        message = fields["grpc-message"]?.single()?.let(::percentDecode),
        trailers = fields.filterKeys { !it.startsWith("grpc-") },
    )
}

/** Inverse of gRPC's `grpc-message` percent-encoding (PROTOCOL-HTTP2.md "Status-Message"). */
fun percentDecode(value: String): String {
    val out = ByteArrayOutputStream()
    var i = 0
    while (i < value.length) {
        if (value[i] == '%') {
            out.write(value.substring(i + 1, i + 3).toInt(16))
            i += 3
        } else {
            out.write(value[i].code)
            i++
        }
    }
    return out.toString(Charsets.UTF_8.name())
}

/** Serves one enveloped call of [protocol] to [procedure] with [messages] as the request body. */
fun ConnectServer.callEnveloped(
    protocol: Enveloped,
    procedure: String,
    messages: List<String>,
    headers: Map<String, String> = emptyMap(),
): FakeExchange {
    val body = Buffer()
    for (m in messages) body.write(env(0, m))
    return call(procedure, protocol.contentType, body.readByteArray(), headers)
}
