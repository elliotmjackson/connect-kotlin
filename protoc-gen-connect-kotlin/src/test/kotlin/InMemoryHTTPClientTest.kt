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

import buf.servergen.`in`.v1.Payload
import com.connectrpc.Code
import com.connectrpc.ConnectException
import com.connectrpc.MethodSpec
import com.connectrpc.ProtocolClientConfig
import com.connectrpc.RequestCompression
import com.connectrpc.ResponseMessage
import com.connectrpc.SerializationStrategy
import com.connectrpc.StreamType
import com.connectrpc.compression.GzipCompressionPool
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.impl.ProtocolClient
import com.connectrpc.protocols.GETConfiguration
import com.connectrpc.protocols.NetworkProtocol
import com.connectrpc.server.BidiStream
import com.connectrpc.server.ClientMessageStream
import com.connectrpc.server.ConnectServer
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.ServerMessageStream
import com.connectrpc.server.http.InMemoryHTTPClient
import com.connectrpc.servergen.`fun`.v1.KeywordServiceClient
import com.connectrpc.servergen.`fun`.v1.KeywordServiceHandler
import com.connectrpc.servergen.`fun`.v1.ServerGenProto.Outer.Inner
import connectrpc.eliza.v1.Eliza.ConverseRequest
import connectrpc.eliza.v1.Eliza.ConverseResponse
import connectrpc.eliza.v1.Eliza.IntroduceRequest
import connectrpc.eliza.v1.Eliza.IntroduceResponse
import connectrpc.eliza.v1.Eliza.SayRequest
import connectrpc.eliza.v1.Eliza.SayResponse
import connectrpc.eliza.v1.ElizaServiceClient
import connectrpc.eliza.v1.ElizaServiceHandler
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.SoftAssertions
import org.junit.Test

/**
 * Runs the generated Eliza client against the generated Eliza handler through
 * [InMemoryHTTPClient], over every protocol, codec and stream type, with and
 * without request compression.
 */
class InMemoryHTTPClientTest {
    private class ElizaImpl : ElizaServiceHandler {
        override suspend fun say(request: SayRequest, ctx: HandlerContext): SayResponse {
            if (request.sentence == "fail") {
                throw ConnectException(Code.FAILED_PRECONDITION, "no", metadata = mapOf("reason" to listOf("asked")))
            }
            ctx.responseHeaders["x-echo"] = mutableListOf(ctx.requestHeaders["x-test"].orEmpty().joinToString())
            ctx.responseTrailers["x-method"] = mutableListOf(ctx.httpMethod)
            return SayResponse.newBuilder().setSentence("You said: ${request.sentence}").build()
        }

        override suspend fun converse(stream: BidiStream<ConverseRequest, ConverseResponse>, ctx: HandlerContext) {
            while (true) {
                val request = stream.receive() ?: break
                stream.send(ConverseResponse.newBuilder().setSentence("Why ${request.sentence}?").build())
            }
        }

        override suspend fun introduce(request: IntroduceRequest, ctx: HandlerContext, stream: ServerMessageStream<IntroduceResponse>) {
            for (i in 1..3) stream.send(IntroduceResponse.newBuilder().setSentence("${request.name} $i").build())
            if (request.name == "fail") throw ConnectException(Code.RESOURCE_EXHAUSTED, "after three")
        }
    }

    private class KeywordsImpl : KeywordServiceHandler {
        override suspend fun `is`(stream: ClientMessageStream<Inner>, ctx: HandlerContext): Inner {
            val values = mutableListOf<String>()
            while (true) values += (stream.receive() ?: break).value
            return inner(values.joinToString("+"))
        }
    }

    private val server = ConnectServer(
        HandlerRegistry.builder()
            .codec(GoogleJavaProtobufStrategy())
            .codec(GoogleJavaJSONStrategy())
            .registerAll(ElizaImpl().handlers())
            .registerAll(KeywordsImpl().handlers())
            .build(),
    )

    private data class Case(val protocol: NetworkProtocol, val codec: SerializationStrategy, val gzip: Boolean) {
        override fun toString() = "$protocol ${codec.serializationName()}${if (gzip) " gzip" else ""}"
    }

    private val cases = NetworkProtocol.entries.flatMap { protocol ->
        listOf(GoogleJavaProtobufStrategy(), GoogleJavaJSONStrategy()).flatMap { codec ->
            listOf(false, true).map { gzip -> Case(protocol, codec, gzip) }
        }
    }

    private fun client(case: Case, get: GETConfiguration = GETConfiguration.Disabled) = ProtocolClient(
        InMemoryHTTPClient(server),
        ProtocolClientConfig(
            host = "http://in-memory",
            serializationStrategy = case.codec,
            networkProtocol = case.protocol,
            requestCompression = if (case.gzip) RequestCompression(0, GzipCompressionPool) else null,
            getConfiguration = get,
        ),
    )

    private fun each(block: suspend SoftAssertions.(Case) -> Unit) {
        val softly = SoftAssertions()
        for (case in cases) runBlocking { withTimeout(10_000) { softly.block(case) } }
        softly.assertAll()
    }

    @Test
    fun unaryCarriesMessageHeadersAndTrailers() = each { case ->
        val response = ElizaServiceClient(client(case)).say(SayRequest.newBuilder().setSentence("hi").build(), mapOf("x-test" to listOf("v")))
        assertThat(response).`as`("$case").isInstanceOf(ResponseMessage.Success::class.java)
        val success = response as? ResponseMessage.Success ?: return@each
        assertThat(success.message.sentence).`as`("$case").isEqualTo("You said: hi")
        assertThat(success.headers["x-echo"]).`as`("$case").containsExactly("v")
        assertThat(success.trailers["x-method"]).`as`("$case").containsExactly("POST")
    }

    @Test
    fun connectGetReachesTheHandlerAsGet() {
        val softly = SoftAssertions()
        for (case in cases.filter { it.protocol == NetworkProtocol.CONNECT }) {
            val response = runBlocking {
                ElizaServiceClient(client(case, GETConfiguration.Enabled)).say(SayRequest.newBuilder().setSentence("hi").build())
            }
            softly.assertThat((response as? ResponseMessage.Success)?.trailers?.get("x-method")).`as`("$case").containsExactly("GET")
        }
        softly.assertAll()
    }

    @Test
    fun unaryErrorKeepsCodeMessageAndMetadata() = each { case ->
        val response = ElizaServiceClient(client(case)).say(SayRequest.newBuilder().setSentence("fail").build())
        val cause = (response as? ResponseMessage.Failure)?.cause
        assertThat(cause?.code).`as`("$case").isEqualTo(Code.FAILED_PRECONDITION)
        assertThat(cause?.message).`as`("$case").isEqualTo("no")
        assertThat(cause?.metadata?.get("reason")).`as`("$case").containsExactly("asked")
    }

    @Test
    fun serverStreamDeliversEveryMessageThenTheError() = each { case ->
        val eliza = ElizaServiceClient(client(case))
        val ok = eliza.introduce()
        ok.sendAndClose(IntroduceRequest.newBuilder().setName("Ann").build())
        assertThat(ok.responseChannel().toList().map { it.sentence }).`as`("$case").containsExactly("Ann 1", "Ann 2", "Ann 3")

        val failing = eliza.introduce()
        failing.sendAndClose(IntroduceRequest.newBuilder().setName("fail").build())
        val received = mutableListOf<String>()
        val error = runCatching { for (message in failing.responseChannel()) received += message.sentence }.exceptionOrNull()
        assertThat(received).`as`("$case").containsExactly("fail 1", "fail 2", "fail 3")
        assertThat((error as? ConnectException)?.code).`as`("$case").isEqualTo(Code.RESOURCE_EXHAUSTED)
    }

    @Test
    fun bidiStreamIsFullDuplex() = each { case ->
        val stream = ElizaServiceClient(client(case)).converse()
        val responses = stream.responseChannel()
        for (sentence in listOf("a", "b")) {
            stream.send(ConverseRequest.newBuilder().setSentence(sentence).build())
            // Each reply arrives before the next request is sent.
            assertThat(responses.receive().sentence).`as`("$case").isEqualTo("Why $sentence?")
        }
        stream.sendClose()
        assertThat(responses.toList()).`as`("$case").isEmpty()
    }

    @Test
    fun clientStreamReceivesEveryRequest() = each { case ->
        val stream = KeywordServiceClient(client(case)).`is`()
        for (value in listOf("x", "y", "z")) stream.send(inner(value))
        assertThat(stream.receiveAndClose().value).`as`("$case").isEqualTo("x+y+z")
    }

    @Test
    fun methodsWithoutAnOverrideAnswerUnimplemented() = each { case ->
        // buf.gen.yaml generates the handlers with generateServerHandlerDefaults=true.
        val keywords = KeywordServiceClient(client(case))
        val unary = keywords.`object`(Payload.getDefaultInstance())
        val unaryCause = (unary as? ResponseMessage.Failure)?.cause
        assertThat(unaryCause?.code).`as`("$case").isEqualTo(Code.UNIMPLEMENTED)
        assertThat(unaryCause?.message).`as`("$case").isEqualTo("buf.servergen.v1.KeywordService.Object is not implemented")

        val stream = keywords.`in`()
        stream.sendAndClose(inner("x"))
        val streamCause = runCatching { stream.responseChannel().toList() }.exceptionOrNull() as? ConnectException
        assertThat(streamCause?.code).`as`("$case").isEqualTo(Code.UNIMPLEMENTED)
        assertThat(streamCause?.message).`as`("$case").isEqualTo("buf.servergen.v1.KeywordService.In is not implemented")
    }

    @Test
    fun unknownProcedureIsUnimplemented() = each { case ->
        val client = client(case)
        val spec = MethodSpec("connectrpc.eliza.v1.ElizaService/Missing", SayRequest::class, SayResponse::class, StreamType.UNARY)
        val response = client.unary(SayRequest.getDefaultInstance(), emptyMap(), spec)
        assertThat((response as? ResponseMessage.Failure)?.cause?.code).`as`("$case").isEqualTo(Code.UNIMPLEMENTED)
    }
}

private fun inner(value: String): Inner = Inner.newBuilder().setValue(value).build()
