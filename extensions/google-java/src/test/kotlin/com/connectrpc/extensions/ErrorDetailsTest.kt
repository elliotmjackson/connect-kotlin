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

package com.connectrpc.extensions

import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.ProtocolClientConfig
import com.connectrpc.ResponseMessage
import com.connectrpc.StreamType
import com.connectrpc.impl.ProtocolClient
import com.connectrpc.protocols.NetworkProtocol
import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.http.InMemoryHTTPClient
import com.connectrpc.server.unaryHandler
import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.SoftAssertions
import org.junit.Test

class ErrorDetailsTest {
    private val spec = MethodSpec("test.v1.TestService/Fail", StringValue::class, StringValue::class, StreamType.UNARY)

    private val server = ConnectServer(
        HandlerRegistry.builder()
            .codec(GoogleJavaProtobufStrategy())
            .codec(GoogleJavaJSONStrategy())
            .register(
                unaryHandler(spec) { _, _ ->
                    throw ConnectException(Code.INVALID_ARGUMENT, "bad")
                        .withDetails(StringValue.of("first"))
                        .withDetails(Int32Value.of(7), StringValue.of("second"))
                },
            )
            .build(),
    )

    @Test
    fun detailsReachTheClientOnEveryProtocolAndCodec() {
        val softly = SoftAssertions()
        for (protocol in NetworkProtocol.entries) {
            for (codec in listOf(GoogleJavaProtobufStrategy(), GoogleJavaJSONStrategy())) {
                val case = "$protocol ${codec.serializationName()}"
                val client = ProtocolClient(
                    InMemoryHTTPClient(server),
                    ProtocolClientConfig(host = "http://in-memory", serializationStrategy = codec, networkProtocol = protocol),
                )
                val response = runBlocking { client.unary(StringValue.of("x"), emptyMap(), spec) }
                val cause = (response as? ResponseMessage.Failure)?.cause
                softly.assertThat(cause?.code).`as`(case).isEqualTo(Code.INVALID_ARGUMENT)
                softly.assertThat(cause?.unpackedDetails(StringValue::class)?.map { it.value }).`as`(case).containsExactly("first", "second")
                softly.assertThat(cause?.unpackedDetails(Int32Value::class)?.map { it.value }).`as`(case).containsExactly(7)
            }
        }
        softly.assertAll()
    }
}
