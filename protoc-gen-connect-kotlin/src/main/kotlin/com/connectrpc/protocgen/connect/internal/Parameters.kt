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

package com.connectrpc.protocgen.connect.internal

internal const val CALLBACK_SIGNATURE = "generateCallbackMethods"
internal const val COROUTINE_SIGNATURE = "generateCoroutineMethods"
internal const val BLOCKING_UNARY_SIGNATURE = "generateBlockingUnaryMethods"
internal const val SERVER_HANDLER_SIGNATURE = "generateServerHandler"
internal const val CLIENT_SIGNATURE = "generateClient"
internal const val SERVER_HANDLER_DEFAULTS_SIGNATURE = "generateServerHandlerDefaults"

/**
 * The protoc plugin configuration class representation.
 */
internal data class Configuration(
    // Enable or disable callback signature generation.
    val generateCallbackMethods: Boolean,
    // Enable or disable coroutine signature generation.
    val generateCoroutineMethods: Boolean,
    // Enable or disable blocking unary signature generation.
    val generateBlockingUnaryMethods: Boolean,
    // Enable or disable `<Service>ClientInterface` / `<Service>Client` generation.
    // On by default; set to false with generateServerHandler=true for server-only output.
    val generateClient: Boolean,
    // Enable or disable `<Service>Handler` server interface generation.
    // Off by default: generated handlers depend on connect-kotlin-server.
    val generateServerHandler: Boolean,
    // Give every `<Service>Handler` method a default body that throws
    // `Code.UNIMPLEMENTED`, so adding an RPC does not break implementations.
    // Off by default: a missing method is then a compile error.
    val generateServerHandlerDefaults: Boolean,
)

/**
 * Parse options passed as a string.
 *
 * Key values are parsed with `parseGeneratorParameter()`.
 * The key values are expected to be in camel casing but
 * will internally translate from snake casing to camel
 * casing.
 *
 * An unknown key, or a value other than `true` or `false`, throws
 * [IllegalArgumentException], as protoc-gen-connect-go rejects unknown and
 * malformed flags (`cmd/protoc-gen-connect-go/main.go:123`, `ParamFunc: flagSet.Set`).
 */
internal fun parse(input: String): Configuration {
    val parameters = parseGeneratorParameter(input)
    val unknown = parameters.keys - SUPPORTED_PARAMETERS
    require(unknown.isEmpty()) {
        "unknown parameter ${unknown.joinToString { "\"$it\"" }}; supported parameters are ${SUPPORTED_PARAMETERS.joinToString()}"
    }
    fun flag(name: String, default: Boolean): Boolean {
        val value = parameters[name] ?: return default
        return requireNotNull(value.toBooleanStrictOrNull()) {
            "invalid value \"$value\" for parameter $name: want true or false"
        }
    }
    return Configuration(
        generateCallbackMethods = flag(CALLBACK_SIGNATURE, false),
        generateCoroutineMethods = flag(COROUTINE_SIGNATURE, true),
        generateBlockingUnaryMethods = flag(BLOCKING_UNARY_SIGNATURE, false),
        generateClient = flag(CLIENT_SIGNATURE, true),
        generateServerHandler = flag(SERVER_HANDLER_SIGNATURE, false),
        generateServerHandlerDefaults = flag(SERVER_HANDLER_DEFAULTS_SIGNATURE, false),
    )
}

private val SUPPORTED_PARAMETERS = listOf(
    CALLBACK_SIGNATURE,
    COROUTINE_SIGNATURE,
    BLOCKING_UNARY_SIGNATURE,
    CLIENT_SIGNATURE,
    SERVER_HANDLER_SIGNATURE,
    SERVER_HANDLER_DEFAULTS_SIGNATURE,
)
