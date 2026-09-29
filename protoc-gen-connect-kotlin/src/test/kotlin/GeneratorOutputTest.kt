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

import buf.deprecation.v1.MethodDeprecated
import buf.deprecation.v1.ServiceDeprecated
import buf.servergen.`in`.v1.Payload
import buf.servergen.shapes.v1.Shapes
import com.connectrpc.protocgen.connect.Generator
import com.connectrpc.protocgen.connect.internal.Plugin
import com.connectrpc.servergen.`fun`.v1.ServerGenProto
import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodOptions
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto
import com.google.protobuf.DescriptorProtos.SourceCodeInfo
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.SoftAssertions
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

/**
 * Runs the plugin in-process on CodeGeneratorRequests built from descriptors
 * and checks the files it emits.
 */
class GeneratorOutputTest {
    private fun request(parameter: String, vararg toGenerate: FileDescriptorProto): CodeGeneratorRequest = CodeGeneratorRequest.newBuilder()
        .setParameter(parameter)
        .addAllProtoFile(toGenerate.asList())
        .addAllFileToGenerate(toGenerate.map { it.name })
        .build()

    /** Builds a request whose proto_file lists dependencies before dependents, as protoc does. */
    private fun request(parameter: String, vararg toGenerate: FileDescriptor): CodeGeneratorRequest {
        val ordered = LinkedHashMap<String, FileDescriptorProto>()
        fun visit(file: FileDescriptor) {
            if (file.name in ordered) return
            file.dependencies.forEach(::visit)
            ordered[file.name] = file.toProto()
        }
        toGenerate.forEach(::visit)
        return CodeGeneratorRequest.newBuilder()
            .setParameter(parameter)
            .addAllProtoFile(ordered.values)
            .addAllFileToGenerate(toGenerate.map { it.name })
            .build()
    }

    private fun generate(request: CodeGeneratorRequest, generator: Generator = Generator()): CodeGeneratorResponse {
        val out = ByteArrayOutputStream()
        Plugin.run(generator, ByteArrayInputStream(request.toByteArray()), out)
        return CodeGeneratorResponse.parseFrom(out.toByteArray()).also {
            assertThat(it.error).isEmpty()
        }
    }

    private fun CodeGeneratorResponse.content(path: String): String = fileList.single { it.name == path }.content

    private val keywordFile = ServerGenProto.getDescriptor()
    private val keywordDir = "com/connectrpc/servergen/fun/v1"

    private data class ParameterCase(
        val parameter: String,
        val files: Set<String>,
        // Generated variants of the unary `Object` RPC in the client interface
        // and client class: "suspend", "callback", "blocking".
        val unaryVariants: Set<String> = emptySet(),
    )

    /**
     * Returns the unary variants generated for the `Object` RPC. Whitespace is
     * collapsed so line wrapping does not matter.
     */
    private fun unaryVariants(source: String): Set<String> {
        val text = source.replace("\\s+".toRegex(), " ")
        val variants = mutableSetOf<String>()
        var index = text.indexOf("fun `object`(")
        while (index >= 0) {
            variants += if (text.substring(0, index).endsWith("suspend ")) "suspend" else "callback"
            index = text.indexOf("fun `object`(", index + 1)
        }
        if ("fun objectBlocking(" in text) variants += "blocking"
        return variants
    }

    @Test
    fun parametersSelectGeneratedFilesAndClientVariants() {
        val clientInterface = "$keywordDir/KeywordServiceClientInterface.kt"
        val client = "$keywordDir/KeywordServiceClient.kt"
        val handler = "$keywordDir/KeywordServiceHandler.kt"
        val clientFiles = setOf(clientInterface, client)
        val cases = listOf(
            ParameterCase("", clientFiles, setOf("suspend")),
            ParameterCase("generateServerHandler=true", clientFiles + handler, setOf("suspend")),
            ParameterCase("generateClient=false,generateServerHandler=true", setOf(handler)),
            // Keys are normalized from snake_case to lowerCamelCase.
            ParameterCase("generate_client=false,generate_server_handler=true", setOf(handler)),
            ParameterCase("generateClient=false", emptySet()),
            // The last occurrence of a repeated key wins.
            ParameterCase("generateServerHandler=true,generateServerHandler=false", clientFiles, setOf("suspend")),
            ParameterCase("generateCallbackMethods=true", clientFiles, setOf("suspend", "callback")),
            ParameterCase("generateCoroutineMethods=false,generateCallbackMethods=true", clientFiles, setOf("callback")),
            ParameterCase("generateBlockingUnaryMethods=true", clientFiles, setOf("suspend", "blocking")),
        )
        val softly = SoftAssertions()
        for (case in cases) {
            val response = generate(request(case.parameter, keywordFile))
            softly.assertThat(response.fileList.map { it.name }).`as`(case.parameter)
                .containsExactlyInAnyOrderElementsOf(case.files)
            for (path in listOf(clientInterface, client).filter { it in case.files }) {
                softly.assertThat(unaryVariants(response.content(path))).`as`("${case.parameter}: $path")
                    .isEqualTo(case.unaryVariants)
            }
        }
        softly.assertAll()
    }

    @Test
    fun rejectsUnknownParametersAndNonBooleanValues() {
        val supported = "supported parameters are generateCallbackMethods, generateCoroutineMethods, " +
            "generateBlockingUnaryMethods, generateClient, generateServerHandler, generateServerHandlerDefaults"
        val cases = mapOf(
            "generateServerHandler=yes" to "invalid value \"yes\" for parameter generateServerHandler: want true or false",
            "generateServerHandler=1" to "invalid value \"1\" for parameter generateServerHandler: want true or false",
            "generateClient=TRUE" to "invalid value \"TRUE\" for parameter generateClient: want true or false",
            // A bare key has an empty value.
            "generateServerHandler" to "invalid value \"\" for parameter generateServerHandler: want true or false",
            "generateServerHandlers=true" to "unknown parameter \"generateServerHandlers\"; $supported",
            // The name is reported after snake_case normalization.
            "generate_server_handlers=true,generateClient=false" to "unknown parameter \"generateServerHandlers\"; $supported",
        )
        val softly = SoftAssertions()
        for ((parameter, error) in cases) {
            val out = ByteArrayOutputStream()
            Plugin.run(Generator(), ByteArrayInputStream(request(parameter, keywordFile).toByteArray()), out)
            val response = CodeGeneratorResponse.parseFrom(out.toByteArray())
            softly.assertThat(response.error).`as`(parameter).isEqualTo(error)
            softly.assertThat(response.fileList).`as`(parameter).isEmpty()
        }
        softly.assertAll()
    }

    @Test
    fun emitsOneHandlerFilePerServiceUnderTheJavaPackage() {
        val cases = mapOf(
            keywordFile to listOf("$keywordDir/KeywordServiceHandler.kt"),
            Shapes.getDescriptor() to listOf(
                "buf/servergen/shapes/v1/NoMethodsServiceHandler.kt",
                "buf/servergen/shapes/v1/WellKnownServiceHandler.kt",
            ),
            // no_package.proto has neither a proto package nor java_package.
            NoPackage.getDescriptor() to listOf("ElizaServiceHandler.kt"),
            // A file without services produces no output.
            Payload.getDescriptor().file to emptyList(),
        )
        for ((file, expected) in cases) {
            val response = generate(request("generateClient=false,generateServerHandler=true", file))
            assertThat(response.fileList.map { it.name }).`as`(file.name).containsExactlyInAnyOrderElementsOf(expected)
        }
    }

    @Test
    @Suppress("DEPRECATION")
    fun sameRequestProducesIdenticalOutput() {
        val request = request(
            "generateServerHandler=true,generateCallbackMethods=true,generateBlockingUnaryMethods=true",
            keywordFile,
            Shapes.getDescriptor(),
            MethodDeprecated.getDescriptor(),
            ServiceDeprecated.getDescriptor(),
            buf.deprecation.v1.FileDeprecated.getDescriptor(),
        )
        val generator = Generator()
        val first = generate(request, generator)
        assertThat(first.fileList).isNotEmpty
        assertThat(generate(request, generator)).isEqualTo(first)
        assertThat(generate(request)).isEqualTo(first)
    }

    @Test
    fun failsWhenADependencyIsMissing() {
        val request = CodeGeneratorRequest.newBuilder()
            .addProtoFile(keywordFile.toProto())
            .addFileToGenerate(keywordFile.name)
            .build()
        assertThatThrownBy { generate(request) }
            .isInstanceOf(Plugin.PluginException::class.java)
            .hasMessageContaining(Payload.getDescriptor().file.name)
    }

    @Test
    fun failsWhenAFileToGenerateIsMissing() {
        val request = CodeGeneratorRequest.newBuilder()
            .addFileToGenerate("absent.proto")
            .build()
        assertThatThrownBy { generate(request) }.isInstanceOf(RuntimeException::class.java)
    }

    /**
     * Returns the text of the KDoc block directly above the first line that
     * contains [declaration], skipping annotations, with the leading `*` of
     * each line removed; null when there is none.
     */
    private fun kdocAbove(source: String, declaration: String): String? {
        val lines = source.lines()
        var end = lines.indexOfFirst { declaration in it } - 1
        check(end >= 0) { "no declaration $declaration" }
        while (lines[end].trim().startsWith("@")) end--
        if (lines[end].trim() != "*/") return null
        val start = (end downTo 0).first { lines[it].trim() == "/**" }
        return lines.subList(start + 1, end).joinToString("\n") { it.trim().removePrefix("*").trim() }
    }

    private fun decodeEntities(text: String): String = text.replace("&#(\\d+);".toRegex()) { it.groupValues[1].toInt().toChar().toString() }

    @Test
    fun commentsFromSourceInfoBecomeKdoc() {
        val evil = "Escapes */ and /* and [link] and @tag and 100%."
        fun method(name: String) = MethodDescriptorProto.newBuilder()
            .setName(name).setInputType(".docs.v1.Msg").setOutputType(".docs.v1.Msg")
        fun location(vararg path: Int, leading: String? = null, trailing: String? = null, detached: String? = null) = SourceCodeInfo.Location.newBuilder().addAllPath(path.asList()).apply {
            leading?.let { leadingComments = it }
            trailing?.let { trailingComments = it }
            detached?.let { addLeadingDetachedComments(it) }
        }
        // Paths follow descriptor.proto field numbers: FileDescriptorProto.service = 6,
        // ServiceDescriptorProto.method = 2.
        val file = FileDescriptorProto.newBuilder()
            .setName("docs/v1/docs.proto")
            .setPackage("docs.v1")
            .setSyntax("proto3")
            .addMessageType(DescriptorProto.newBuilder().setName("Msg"))
            .addService(
                ServiceDescriptorProto.newBuilder().setName("DocsService")
                    .addMethod(method("Leading"))
                    .addMethod(method("Trailing"))
                    .addMethod(method("Detached"))
                    .addMethod(method("Evil"))
                    .addMethod(method("Undocumented").setOptions(MethodOptions.newBuilder().setDeprecated(true))),
            )
            .setSourceCodeInfo(
                SourceCodeInfo.newBuilder()
                    .addLocation(location(6, 0, leading = " Serves docs.\n"))
                    .addLocation(location(6, 0, 2, 0, leading = " First line.\n Second line.\n", trailing = " Not used.\n"))
                    .addLocation(location(6, 0, 2, 1, leading = "  \n", trailing = " Trailing only.\n"))
                    .addLocation(location(6, 0, 2, 2, detached = " Detached.\n"))
                    .addLocation(location(6, 0, 2, 3, leading = " $evil\n")),
            )
            .build()

        val handler = generate(request("generateClient=false,generateServerHandler=true", file))
            .content("docs/v1/DocsServiceHandler.kt")

        assertThat(kdocAbove(handler, "interface DocsServiceHandler")!!.lines().first()).isEqualTo("Serves docs.")
        assertThat(kdocAbove(handler, "fun leading(")).isEqualTo("First line.\nSecond line.")
        assertThat(kdocAbove(handler, "fun trailing(")).isEqualTo("Trailing only.")
        assertThat(kdocAbove(handler, "fun detached(")).isNull()
        assertThat(kdocAbove(handler, "fun undocumented(")).isNull()
        val escaped = kdocAbove(handler, "fun evil(")!!
        assertThat(escaped).doesNotContain("*/", "/*")
        assertThat(decodeEntities(escaped)).isEqualTo(evil)
    }

    /** The raw KDoc block directly above the first line containing [declaration]. */
    private fun rawKdocAbove(source: String, declaration: String): String {
        val lines = source.lines()
        val end = lines.indexOfFirst { declaration in it } - 1
        val start = (end downTo 0).first { lines[it].trim() == "/**" }
        return lines.subList(start, end + 1).joinToString("\n") { it.trimStart() }
    }

    @Test
    fun kdocDropsTheSpaceProtocKeepsAfterTheCommentMarker() {
        val file = FileDescriptorProto.newBuilder()
            .setName("docs/v1/docs.proto")
            .setPackage("docs.v1")
            .setSyntax("proto3")
            .addMessageType(DescriptorProto.newBuilder().setName("Msg"))
            .addService(
                ServiceDescriptorProto.newBuilder().setName("DocsService")
                    .addMethod(MethodDescriptorProto.newBuilder().setName("Fetch").setInputType(".docs.v1.Msg").setOutputType(".docs.v1.Msg")),
            )
            .setSourceCodeInfo(
                SourceCodeInfo.newBuilder()
                    // As protoc reports `// Serves docs.` and `// Fetches:` / `//   indented`.
                    .addLocation(SourceCodeInfo.Location.newBuilder().addAllPath(listOf(6, 0)).setLeadingComments(" Serves docs.\n Second line.\n"))
                    .addLocation(SourceCodeInfo.Location.newBuilder().addAllPath(listOf(6, 0, 2, 0)).setLeadingComments(" Fetches:\n   indented\n")),
            )
            .build()
        val response = generate(request("generateServerHandler=true", file))
        val handler = response.content("docs/v1/DocsServiceHandler.kt")
        val client = response.content("docs/v1/DocsServiceClientInterface.kt")

        assertThat(rawKdocAbove(handler, "interface DocsServiceHandler")).isEqualTo(
            """
            /**
            * Serves docs.
            * Second line.
            *
            * Server-side interface for `docs.v1.DocsService`. Implement one method per RPC, then pass `handlers()` to `HandlerRegistry.Builder.registerAll(...)`.
            */
            """.trimIndent(),
        )
        val method = "/**\n* Fetches:\n*   indented\n*/"
        assertThat(rawKdocAbove(handler, "fun fetch(")).isEqualTo(method)
        assertThat(rawKdocAbove(client, "interface DocsServiceClientInterface")).isEqualTo("/**\n* Serves docs.\n* Second line.\n*/")
        assertThat(rawKdocAbove(client, "fun fetch(")).isEqualTo(method)
    }
}
