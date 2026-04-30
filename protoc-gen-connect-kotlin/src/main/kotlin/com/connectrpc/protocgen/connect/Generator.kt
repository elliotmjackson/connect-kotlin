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

package com.connectrpc.protocgen.connect

import com.connectrpc.BidirectionalStreamInterface
import com.connectrpc.ClientOnlyStreamInterface
import com.connectrpc.Idempotency
import com.connectrpc.MethodSpec
import com.connectrpc.ProtocolClientInterface
import com.connectrpc.ResponseMessage
import com.connectrpc.ServerOnlyStreamInterface
import com.connectrpc.StreamType
import com.connectrpc.UnaryBlockingCall
import com.connectrpc.protocgen.connect.internal.CodeGenerator
import com.connectrpc.protocgen.connect.internal.Configuration
import com.connectrpc.protocgen.connect.internal.Plugin
import com.connectrpc.protocgen.connect.internal.SourceInfo
import com.connectrpc.protocgen.connect.internal.getClassName
import com.connectrpc.protocgen.connect.internal.getFileJavaPackage
import com.connectrpc.protocgen.connect.internal.parse
import com.connectrpc.protocgen.connect.internal.withSourceInfo
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodOptions.IdempotencyLevel
import com.google.protobuf.Descriptors
import com.google.protobuf.compiler.PluginProtos
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.LambdaTypeName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.STAR
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName

/*
 * These are constants since com.connectrpc.Headers and com.connectrpc.http.Cancelable
 * are type aliases which doesn't have an underlying class for KotlinPoet to know what to do.
 *
 * The conventional and nicer way is to use the class type: Headers::class.asClassType() but
 * type aliasing does not allow for that.
 *
 * Instead, this is the way to reference these objects for now. If there is ever a desire to
 * move off of type aliases, this can be changed without user API breakage.
 */
private val HEADERS_CLASS_NAME = ClassName("com.connectrpc", "Headers")
private val CANCELABLE_CLASS_NAME = ClassName("com.connectrpc.http", "Cancelable")

private const val SERVER_PACKAGE = "com.connectrpc.server"
private val SERVER_HANDLER = ClassName(SERVER_PACKAGE, "Handler")
private val SERVER_HANDLER_CONTEXT = ClassName(SERVER_PACKAGE, "HandlerContext")
private val SERVER_UNARY_HANDLER = ClassName(SERVER_PACKAGE, "UnaryHandler")
private val SERVER_SERVER_STREAM_HANDLER = ClassName(SERVER_PACKAGE, "ServerStreamHandler")
private val SERVER_CLIENT_STREAM_HANDLER = ClassName(SERVER_PACKAGE, "ClientStreamHandler")
private val SERVER_BIDI_STREAM_HANDLER = ClassName(SERVER_PACKAGE, "BidiStreamHandler")
private val SERVER_MESSAGE_STREAM = ClassName(SERVER_PACKAGE, "ServerMessageStream")
private val SERVER_CLIENT_MESSAGE_STREAM = ClassName(SERVER_PACKAGE, "ClientMessageStream")
private val SERVER_BIDI_STREAM = ClassName(SERVER_PACKAGE, "BidiStream")

class Generator : CodeGenerator {
    private lateinit var descriptorSource: Plugin.DescriptorSource
    private lateinit var configuration: Configuration
    private val protoFileMap = mutableMapOf<String, FileDescriptorProto>()

    override fun generate(
        request: PluginProtos.CodeGeneratorRequest,
        descriptorSource: Plugin.DescriptorSource,
        response: Plugin.Response,
    ) {
        this.descriptorSource = descriptorSource
        configuration = parse(request.parameter)
        for (protoFile in request.protoFileList) {
            protoFileMap[protoFile.name] = protoFile
        }
        for (fileName in request.fileToGenerateList) {
            val file =
                descriptorSource.findFileByName(fileName) ?: throw RuntimeException("no descriptor sources found.")
            if (file.services.isEmpty()) {
                // Avoid generating files with no service definitions.
                continue
            }
            val fileMap = parseFile(file)
            for ((className, fileSpec) in fileMap) {
                try {
                    response.addFile("${className.canonicalName.packageToDirectory()}.kt", fileSpec.toString())
                } catch (e: Throwable) {
                    throw Throwable("failure on generating ${file.name}", e)
                }
            }
        }
    }

    override fun getSupportedFeatures(): Array<PluginProtos.CodeGeneratorResponse.Feature> {
        return arrayOf(
            PluginProtos.CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL,
            PluginProtos.CodeGeneratorResponse.Feature.FEATURE_SUPPORTS_EDITIONS,
        )
    }

    override fun getMinimumEdition(): DescriptorProtos.Edition {
        return DescriptorProtos.Edition.EDITION_PROTO2
    }

    override fun getMaximumEdition(): DescriptorProtos.Edition {
        return DescriptorProtos.Edition.EDITION_2023
    }

    private fun parseFile(file: Descriptors.FileDescriptor): Map<ClassName, FileSpec> {
        val baseSourceInfo = SourceInfo(protoFileMap[file.name]!!, descriptorSource, emptyList())
        val fileSpecs = mutableMapOf<ClassName, FileSpec>()
        val packageName = getFileJavaPackage(file)
        for ((sourceInfo, service) in file.services.withSourceInfo(
            baseSourceInfo,
            FileDescriptorProto.SERVICE_FIELD_NUMBER,
        )) {
            val interfaceFileSpec = FileSpec.builder(packageName, file.name)
                .addFileComment("Code generated by connect-kotlin. DO NOT EDIT.\n")
                .addFileComment("\n")
                .addFileComment("Source: ${file.name}\n")
                .suppressDeprecationWarnings(file)
                .addType(serviceClientInterface(packageName, service, file, sourceInfo))
                .build()
            fileSpecs[serviceClientInterfaceClassName(packageName, service)] = interfaceFileSpec

            val implementationFileSpecBuilder = FileSpec.builder(packageName, file.name)
                .addImport(MethodSpec::class.java.`package`.name, "MethodSpec")
                .addImport(StreamType::class.java.`package`.name, "StreamType")
                .addFileComment("Code generated by connect-kotlin. DO NOT EDIT.\n")
                .addFileComment("\n")
                .addFileComment("Source: ${file.name}\n")
                .suppressDeprecationWarnings(file)
                // Set the file package for the generated methods.
                .addType(serviceClientImplementation(packageName, service, file, sourceInfo))
            for (method in service.methods) {
                if (method.options.hasIdempotencyLevel()) {
                    implementationFileSpecBuilder.addImport(Idempotency::class.java.`package`.name, "Idempotency")
                    break
                }
            }
            val implementationFileSpec = implementationFileSpecBuilder.build()
            fileSpecs[serviceClientImplementationClassName(packageName, service)] = implementationFileSpec

            if (configuration.generateServerHandler) {
                val serverHandlerFileSpecBuilder = FileSpec.builder(packageName, file.name)
                    .addImport(MethodSpec::class.java.`package`.name, "MethodSpec")
                    .addImport(StreamType::class.java.`package`.name, "StreamType")
                    .addFileComment("Code generated by connect-kotlin. DO NOT EDIT.\n")
                    .addFileComment("\n")
                    .addFileComment("Source: ${file.name}\n")
                    .suppressDeprecationWarnings(file)
                    .addType(serviceServerHandler(packageName, service, file, sourceInfo))
                for (method in service.methods) {
                    if (method.options.hasIdempotencyLevel()) {
                        serverHandlerFileSpecBuilder.addImport(Idempotency::class.java.`package`.name, "Idempotency")
                        break
                    }
                }
                val serverHandlerFileSpec = serverHandlerFileSpecBuilder.build()
                fileSpecs[serviceServerHandlerClassName(packageName, service)] = serverHandlerFileSpec
            }
        }
        return fileSpecs
    }

    /**
     * Generates the abstract `<Service>Handler` server base. Users extend it,
     * implement one method per RPC, and call `handlers()` to get a list ready
     * for [com.connectrpc.server.HandlerRegistry.Builder.registerAll].
     */
    private fun serviceServerHandler(
        packageName: String,
        service: Descriptors.ServiceDescriptor,
        file: Descriptors.FileDescriptor,
        sourceInfo: SourceInfo,
    ): TypeSpec {
        val builder = TypeSpec.classBuilder(serviceServerHandlerClassName(packageName, service))
            .addModifiers(KModifier.ABSTRACT)
            .addServiceDeprecation(service, file)
            .addKdoc(
                "Server-side abstract base for `${service.fullName}`. Override one method " +
                    "per RPC, then pass `handlers()` to `HandlerRegistry.Builder.registerAll(...)`.",
            )

        for ((methodSourceInfo, method) in service.methods.withSourceInfo(
            sourceInfo,
            DescriptorProtos.ServiceDescriptorProto.METHOD_FIELD_NUMBER,
        )) {
            builder.addFunction(serverHandlerAbstractMethod(method, methodSourceInfo))
        }

        builder.addFunction(serverHandlersFactoryMethod(service))
        return builder.build()
    }

    private fun serverHandlerAbstractMethod(
        method: Descriptors.MethodDescriptor,
        sourceInfo: SourceInfo,
    ): FunSpec {
        val inputClassName = classNameFromType(method.inputType)
        val outputClassName = classNameFromType(method.outputType)
        val methodName = method.name.lowerCamelCase()
        val fnBuilder = FunSpec.builder(methodName)
            .addKdoc(sourceInfo.comment().sanitizeKdoc())
            .addMethodDeprecation(method)
            .addModifiers(KModifier.ABSTRACT, KModifier.SUSPEND)

        when {
            method.isClientStreaming && method.isServerStreaming -> fnBuilder
                .addParameter(
                    "stream",
                    SERVER_BIDI_STREAM.parameterizedBy(inputClassName, outputClassName),
                )
                .addParameter("ctx", SERVER_HANDLER_CONTEXT)

            method.isServerStreaming -> fnBuilder
                .addParameter("request", inputClassName)
                .addParameter("ctx", SERVER_HANDLER_CONTEXT)
                .addParameter(
                    "stream",
                    SERVER_MESSAGE_STREAM.parameterizedBy(outputClassName),
                )

            method.isClientStreaming -> fnBuilder
                .addParameter(
                    "stream",
                    SERVER_CLIENT_MESSAGE_STREAM.parameterizedBy(inputClassName),
                )
                .addParameter("ctx", SERVER_HANDLER_CONTEXT)
                .returns(outputClassName)

            else -> fnBuilder
                .addParameter("request", inputClassName)
                .addParameter("ctx", SERVER_HANDLER_CONTEXT)
                .returns(outputClassName)
        }
        return fnBuilder.build()
    }

    private fun serverHandlersFactoryMethod(
        service: Descriptors.ServiceDescriptor,
    ): FunSpec {
        val handlerStar = SERVER_HANDLER.parameterizedBy(STAR, STAR)
        val returnType = ClassName("kotlin.collections", "List").parameterizedBy(handlerStar)
        val code = CodeBlock.builder()
        code.add("return listOf(\n").indent()
        for ((index, method) in service.methods.withIndex()) {
            code.add("%L", serverHandlerInstance(service, method))
            if (index < service.methods.size - 1) code.add(",\n") else code.add("\n")
        }
        code.unindent().add(")\n")
        return FunSpec.builder("handlers")
            .addKdoc("Returns the list of server handlers wired to this service's RPCs.")
            .returns(returnType)
            .addCode(code.build())
            .build()
    }

    private fun serverHandlerInstance(
        service: Descriptors.ServiceDescriptor,
        method: Descriptors.MethodDescriptor,
    ): CodeBlock {
        val inputClassName = classNameFromType(method.inputType)
        val outputClassName = classNameFromType(method.outputType)
        val path = "${service.fullName}/${method.name}"
        val streamTypeRef = when {
            method.isClientStreaming && method.isServerStreaming -> "StreamType.BIDI"
            method.isServerStreaming -> "StreamType.SERVER"
            method.isClientStreaming -> "StreamType.CLIENT"
            else -> "StreamType.UNARY"
        }
        val (handlerType, methodSpecExtraArgs) = serverHandlerType(method)
        val idempotencyArg = when (method.options.idempotencyLevel) {
            IdempotencyLevel.NO_SIDE_EFFECTS -> ", Idempotency.NO_SIDE_EFFECTS"
            IdempotencyLevel.IDEMPOTENT -> ", Idempotency.IDEMPOTENT"
            else -> ""
        }
        val methodName = method.name.lowerCamelCase()

        val callTarget = when {
            method.isClientStreaming && method.isServerStreaming ->
                "$methodName(stream, ctx)"
            method.isServerStreaming ->
                "$methodName(request, ctx, stream)"
            method.isClientStreaming ->
                "$methodName(stream, ctx)"
            else ->
                "$methodName(request, ctx)"
        }

        val handleSignature = when {
            method.isClientStreaming && method.isServerStreaming ->
                CodeBlock.of(
                    "override suspend fun handle(stream: %T<%T, %T>, ctx: %T): %T = $callTarget",
                    SERVER_BIDI_STREAM,
                    inputClassName,
                    outputClassName,
                    SERVER_HANDLER_CONTEXT,
                    Unit::class.asTypeName(),
                )

            method.isServerStreaming ->
                CodeBlock.of(
                    "override suspend fun handle(request: %T, ctx: %T, stream: %T<%T>): %T = $callTarget",
                    inputClassName,
                    SERVER_HANDLER_CONTEXT,
                    SERVER_MESSAGE_STREAM,
                    outputClassName,
                    Unit::class.asTypeName(),
                )

            method.isClientStreaming ->
                CodeBlock.of(
                    "override suspend fun handle(stream: %T<%T>, ctx: %T): %T = $callTarget",
                    SERVER_CLIENT_MESSAGE_STREAM,
                    inputClassName,
                    SERVER_HANDLER_CONTEXT,
                    outputClassName,
                )

            else ->
                CodeBlock.of(
                    "override suspend fun handle(request: %T, ctx: %T): %T = $callTarget",
                    inputClassName,
                    SERVER_HANDLER_CONTEXT,
                    outputClassName,
                )
        }

        return CodeBlock.builder()
            .add(
                "object : %T<%T, %T> {\n",
                handlerType,
                inputClassName,
                outputClassName,
            )
            .indent()
            .add(
                "override val methodSpec = MethodSpec(\n",
            )
            .indent()
            .add("%S,\n", path)
            .add("%T::class,\n", inputClassName)
            .add("%T::class,\n", outputClassName)
            .add("$streamTypeRef$idempotencyArg,\n")
            .unindent()
            .add(")\n")
            .add(handleSignature)
            .add("\n")
            .unindent()
            .add("}")
            .build()
    }

    private fun serverHandlerType(
        method: Descriptors.MethodDescriptor,
    ): Pair<ClassName, List<Any>> = when {
        method.isClientStreaming && method.isServerStreaming -> SERVER_BIDI_STREAM_HANDLER to emptyList()
        method.isServerStreaming -> SERVER_SERVER_STREAM_HANDLER to emptyList()
        method.isClientStreaming -> SERVER_CLIENT_STREAM_HANDLER to emptyList()
        else -> SERVER_UNARY_HANDLER to emptyList()
    }

    private fun serviceClientInterface(
        packageName: String,
        service: Descriptors.ServiceDescriptor,
        file: Descriptors.FileDescriptor,
        sourceInfo: SourceInfo,
    ): TypeSpec {
        val interfaceBuilder = TypeSpec.interfaceBuilder(serviceClientInterfaceClassName(packageName, service))
        val functionSpecs = interfaceMethods(service.methods, sourceInfo)
        return interfaceBuilder
            .addServiceDeprecation(service, file)
            .addKdoc(sourceInfo.comment().sanitizeKdoc())
            .addFunctions(functionSpecs)
            .build()
    }

    private fun interfaceMethods(
        methods: List<Descriptors.MethodDescriptor>,
        baseSourceInfo: SourceInfo,
    ): List<FunSpec> {
        val functions = mutableListOf<FunSpec>()
        val headerParameterSpec = ParameterSpec.builder("headers", HEADERS_CLASS_NAME)
            .defaultValue("%L", "emptyMap()")
            .build()
        for ((sourceInfo, method) in methods.withSourceInfo(
            baseSourceInfo,
            DescriptorProtos.ServiceDescriptorProto.METHOD_FIELD_NUMBER,
        )) {
            val inputClassName = classNameFromType(method.inputType)
            val outputClassName = classNameFromType(method.outputType)
            if (method.isClientStreaming && method.isServerStreaming) {
                val streamingBuilder = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.ABSTRACT)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter(headerParameterSpec)
                    .returns(
                        BidirectionalStreamInterface::class.asClassName()
                            .parameterizedBy(inputClassName, outputClassName),
                    )
                functions.add(streamingBuilder.build())
            } else if (method.isServerStreaming) {
                val serverStreamingFunction = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.ABSTRACT)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter(headerParameterSpec)
                    .returns(
                        ServerOnlyStreamInterface::class.asClassName().parameterizedBy(inputClassName, outputClassName),
                    )
                    .build()
                functions.add(serverStreamingFunction)
            } else if (method.isClientStreaming) {
                val clientStreamingFunction = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.ABSTRACT)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter(headerParameterSpec)
                    .returns(
                        ClientOnlyStreamInterface::class.asClassName().parameterizedBy(inputClassName, outputClassName),
                    )
                    .build()
                functions.add(clientStreamingFunction)
            } else {
                if (configuration.generateCoroutineMethods) {
                    val unarySuspendFunction = FunSpec.builder(method.name.lowerCamelCase())
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addMethodDeprecation(method)
                        .addModifiers(KModifier.ABSTRACT)
                        .addModifiers(KModifier.SUSPEND)
                        .addParameter("request", inputClassName)
                        .addParameter(headerParameterSpec)
                        .returns(ResponseMessage::class.asClassName().parameterizedBy(outputClassName))
                        .build()
                    functions.add(unarySuspendFunction)
                }
                if (configuration.generateCallbackMethods) {
                    val callbackType = LambdaTypeName.get(
                        parameters = listOf(
                            ParameterSpec(
                                "",
                                ResponseMessage::class.asTypeName().parameterizedBy(outputClassName),
                            ),
                        ),
                        returnType = Unit::class.java.asTypeName(),
                    )
                    val unaryCallbackFunction = FunSpec.builder(method.name.lowerCamelCase())
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addMethodDeprecation(method)
                        .addModifiers(KModifier.ABSTRACT)
                        .addParameter("request", inputClassName)
                        .addParameter(headerParameterSpec)
                        .addParameter("onResult", callbackType)
                        .returns(CANCELABLE_CLASS_NAME)
                        .build()
                    functions.add(unaryCallbackFunction)
                }
                if (configuration.generateBlockingUnaryMethods) {
                    val unarySuspendFunction = FunSpec.builder("${method.name.lowerCamelCase()}Blocking")
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addModifiers(KModifier.ABSTRACT)
                        .addParameter("request", inputClassName)
                        .addParameter(headerParameterSpec)
                        .returns(UnaryBlockingCall::class.asClassName().parameterizedBy(outputClassName))
                        .build()
                    functions.add(unarySuspendFunction)
                }
            }
        }
        return functions
    }

    private fun serviceClientImplementation(
        javaPackageName: String,
        service: Descriptors.ServiceDescriptor,
        file: Descriptors.FileDescriptor,
        sourceInfo: SourceInfo,
    ): TypeSpec {
        // The javaPackageName is used instead of the package name for imports and code references.
        val classBuilder = TypeSpec.classBuilder(serviceClientImplementationClassName(javaPackageName, service))
            .addSuperinterface(serviceClientInterfaceClassName(javaPackageName, service))
            .primaryConstructor(
                FunSpec.constructorBuilder()
                    .addParameter("client", ProtocolClientInterface::class)
                    .build(),
            )
            .addProperty(
                PropertySpec.builder("client", ProtocolClientInterface::class, KModifier.PRIVATE)
                    .initializer("client")
                    .build(),
            )
        val functionSpecs = implementationMethods(service.methods, sourceInfo)
        return classBuilder
            .addKdoc(sourceInfo.comment().sanitizeKdoc())
            .addServiceDeprecation(service, file)
            .addFunctions(functionSpecs)
            .build()
    }

    private fun implementationMethods(
        methods: List<Descriptors.MethodDescriptor>,
        baseSourceInfo: SourceInfo,
    ): List<FunSpec> {
        val functions = mutableListOf<FunSpec>()
        for ((sourceInfo, method) in methods.withSourceInfo(
            baseSourceInfo,
            DescriptorProtos.ServiceDescriptorProto.METHOD_FIELD_NUMBER,
        )) {
            val inputClassName = classNameFromType(method.inputType)
            val outputClassName = classNameFromType(method.outputType)
            val methodSpecBuilder = CodeBlock.builder()
                .addStatement("MethodSpec(")
                .addStatement("\"${method.service.fullName}/${method.name}\",")
                .indent()
                .addStatement("$inputClassName::class,")
                .addStatement("$outputClassName::class,")
            if (method.isClientStreaming && method.isServerStreaming) {
                methodSpecBuilder.addStatement("StreamType.${StreamType.BIDI.name},")
            } else if (method.isClientStreaming) {
                methodSpecBuilder.addStatement("StreamType.${StreamType.CLIENT.name},")
            } else if (method.isServerStreaming) {
                methodSpecBuilder.addStatement("StreamType.${StreamType.SERVER.name},")
            } else {
                methodSpecBuilder.addStatement("StreamType.${StreamType.UNARY.name},")
            }
            when (method.options.idempotencyLevel) {
                IdempotencyLevel.NO_SIDE_EFFECTS -> methodSpecBuilder.addStatement("idempotency = Idempotency.${Idempotency.NO_SIDE_EFFECTS.name},")

                IdempotencyLevel.IDEMPOTENT -> methodSpecBuilder.addStatement("idempotency = Idempotency.${Idempotency.IDEMPOTENT.name},")

                else -> {
                    // Use default value in method spec.
                }
            }
            val methodSpecCallBlock = methodSpecBuilder
                .unindent()
                .addStatement("),")
                .build()
            if (method.isClientStreaming && method.isServerStreaming) {
                val streamingFunction = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.OVERRIDE)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter("headers", HEADERS_CLASS_NAME)
                    .returns(
                        BidirectionalStreamInterface::class.asClassName()
                            .parameterizedBy(
                                inputClassName,
                                outputClassName,
                            ),
                    )
                    .addStatement(
                        "return %L",
                        CodeBlock.builder()
                            .addStatement("client.stream(")
                            .indent()
                            .addStatement("headers,")
                            .add(methodSpecCallBlock)
                            .unindent()
                            .addStatement(")")
                            .build(),
                    )
                    .build()
                functions.add(streamingFunction)
            } else if (method.isServerStreaming) {
                val serverStreamingFunction = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.OVERRIDE)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter("headers", HEADERS_CLASS_NAME)
                    .returns(
                        ServerOnlyStreamInterface::class.asClassName().parameterizedBy(inputClassName, outputClassName),
                    )
                    .addStatement(
                        "return %L",
                        CodeBlock.builder()
                            .addStatement("client.serverStream(")
                            .indent()
                            .addStatement("headers,")
                            .add(methodSpecCallBlock)
                            .unindent()
                            .addStatement(")")
                            .build(),
                    )
                    .build()
                functions.add(serverStreamingFunction)
            } else if (method.isClientStreaming) {
                val clientStreamingFunction = FunSpec.builder(method.name.lowerCamelCase())
                    .addKdoc(sourceInfo.comment().sanitizeKdoc())
                    .addMethodDeprecation(method)
                    .addModifiers(KModifier.OVERRIDE)
                    .addModifiers(KModifier.SUSPEND)
                    .addParameter("headers", HEADERS_CLASS_NAME)
                    .returns(
                        ClientOnlyStreamInterface::class.asClassName().parameterizedBy(inputClassName, outputClassName),
                    )
                    .addStatement(
                        "return %L",
                        CodeBlock.builder()
                            .addStatement("client.clientStream(")
                            .indent()
                            .addStatement("headers,")
                            .add(methodSpecCallBlock)
                            .unindent()
                            .addStatement(")")
                            .build(),
                    )
                    .build()
                functions.add(clientStreamingFunction)
            } else {
                if (configuration.generateCoroutineMethods) {
                    val unarySuspendFunction = FunSpec.builder(method.name.lowerCamelCase())
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addMethodDeprecation(method)
                        .addModifiers(KModifier.SUSPEND)
                        .addModifiers(KModifier.OVERRIDE)
                        .addParameter("request", inputClassName)
                        .addParameter("headers", HEADERS_CLASS_NAME)
                        .returns(ResponseMessage::class.asClassName().parameterizedBy(outputClassName))
                        .addStatement(
                            "return %L",
                            CodeBlock.builder()
                                .addStatement("client.unary(")
                                .indent()
                                .addStatement("request,")
                                .addStatement("headers,")
                                .add(methodSpecCallBlock)
                                .unindent()
                                .addStatement(")")
                                .build(),
                        )
                        .build()
                    functions.add(unarySuspendFunction)
                }
                if (configuration.generateCallbackMethods) {
                    val callbackType = LambdaTypeName.get(
                        parameters = listOf(
                            ParameterSpec(
                                "",
                                ResponseMessage::class.asTypeName().parameterizedBy(outputClassName),
                            ),
                        ),
                        returnType = Unit::class.java.asTypeName(),
                    )
                    val unaryCallbackFunction = FunSpec.builder(method.name.lowerCamelCase())
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addMethodDeprecation(method)
                        .addModifiers(KModifier.OVERRIDE)
                        .addParameter("request", inputClassName)
                        .addParameter("headers", HEADERS_CLASS_NAME)
                        .addParameter("onResult", callbackType)
                        .returns(CANCELABLE_CLASS_NAME)
                        .addStatement(
                            "return %L",
                            CodeBlock.builder()
                                .addStatement("client.unary(")
                                .indent()
                                .addStatement("request,")
                                .addStatement("headers,")
                                .add(methodSpecCallBlock)
                                .addStatement("onResult")
                                .unindent()
                                .addStatement(")")
                                .build(),
                        )
                        .build()
                    functions.add(unaryCallbackFunction)
                }
                if (configuration.generateBlockingUnaryMethods) {
                    val unarySuspendFunction = FunSpec.builder("${method.name.lowerCamelCase()}Blocking")
                        .addKdoc(sourceInfo.comment().sanitizeKdoc())
                        .addModifiers(KModifier.OVERRIDE)
                        .addParameter("request", inputClassName)
                        .addParameter("headers", HEADERS_CLASS_NAME)
                        .returns(UnaryBlockingCall::class.asClassName().parameterizedBy(outputClassName))
                        .addStatement(
                            "return %L",
                            CodeBlock.builder()
                                .addStatement("client.unaryBlocking(")
                                .indent()
                                .addStatement("request,")
                                .addStatement("headers,")
                                .add(methodSpecCallBlock)
                                .unindent()
                                .addStatement(")")
                                .build(),
                        )
                        .build()
                    functions.add(unarySuspendFunction)
                }
            }
        }
        return functions
    }

    private fun classNameFromType(descriptor: Descriptors.Descriptor): ClassName {
        // Get the package of the descriptor's file.
        // e.g. "com.connectrpc".
        val packageName = getFileJavaPackage(descriptor.file)
        // Get the fully qualified class name of the descriptor
        // and subtract the file's package.
        // e.g. "com.connectrpc.EmptyMessage.InnerMessage"
        // becomes ["EmptyMessage", "InnerMessage"].
        val names = getClassName(descriptor)
            .removePrefix(packageName)
            .removePrefix(".")
            .split(".")
        // Case when there is a nested entity.
        // e.g Nested message definitions and messages within "*OuterClass.java".
        if (names.size > 1) {
            return ClassName(packageName, names.first(), *names.subList(1, names.size).toTypedArray())
        }
        return ClassName(packageName, names.first())
    }

    private fun String.sanitizeKdoc(): String {
        return this
            // Remove trailing whitespace on each line.
            .replace("[^\\S\n]+\n".toRegex(), "\n")
            .replace("\\s+$".toRegex(), "")
            .replace("\\*/".toRegex(), "&#42;/")
            .replace("/\\*".toRegex(), "/&#42;")
            .replace("""[""", "&#91;")
            .replace("""]""", "&#93;")
            .replace("@", "&#64;")
            .replace("%", "%%")
    }
}

private fun serviceClientInterfaceClassName(packageName: String, service: Descriptors.ServiceDescriptor): ClassName {
    return ClassName(packageName, "${service.name}ClientInterface")
}

private fun serviceClientImplementationClassName(
    packageName: String,
    service: Descriptors.ServiceDescriptor,
): ClassName {
    return ClassName(packageName, "${service.name}Client")
}

private fun serviceServerHandlerClassName(
    packageName: String,
    service: Descriptors.ServiceDescriptor,
): ClassName {
    return ClassName(packageName, "${service.name}Handler")
}

private fun String.lowerCamelCase(): String {
    return replaceFirstChar { char -> char.lowercaseChar() }
}

private fun String.packageToDirectory(): String {
    val dir = replace('.', '/')
    if (get(0) == '/') {
        return dir.substring(1)
    }
    return dir
}

private fun TypeSpec.Builder.addServiceDeprecation(
    service: Descriptors.ServiceDescriptor,
    file: Descriptors.FileDescriptor,
): TypeSpec.Builder {
    if (service.options.deprecated) {
        this.addAnnotation(
            AnnotationSpec.builder(Deprecated::class)
                .addMember("%S", "The service is deprecated in the Protobuf source file.")
                .build(),
        )
    } else if (file.options.deprecated) {
        this.addAnnotation(
            AnnotationSpec.builder(Deprecated::class)
                .addMember("%S", "The Protobuf source file that defines this service is deprecated.")
                .build(),
        )
    }
    return this
}

private fun FunSpec.Builder.addMethodDeprecation(
    method: Descriptors.MethodDescriptor,
): FunSpec.Builder {
    if (method.options.deprecated) {
        this.addAnnotation(
            AnnotationSpec.builder(Deprecated::class)
                .addMember("%S", "The method is deprecated in the Protobuf source file.")
                .build(),
        )
    }
    return this
}

private fun FileSpec.Builder.suppressDeprecationWarnings(
    file: Descriptors.FileDescriptor,
): FileSpec.Builder {
    val hasDeprecated = file.options.deprecated || file.services.find { s -> s.options.deprecated || s.methods.find { m -> m.options.deprecated } != null } != null
    if (hasDeprecated) {
        this.addAnnotation(
            AnnotationSpec.builder(Suppress::class)
                .addMember("%S", "DEPRECATION")
                .build(),
        )
    }
    return this
}
