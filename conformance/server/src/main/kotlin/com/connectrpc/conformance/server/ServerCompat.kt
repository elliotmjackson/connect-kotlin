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

package com.connectrpc.conformance.server

import com.connectrpc.conformance.v1.ClientCompatProto
import com.connectrpc.conformance.v1.ConfigProto
import com.connectrpc.conformance.v1.HTTPVersion
import com.connectrpc.conformance.v1.ServerCompatProto
import com.connectrpc.conformance.v1.ServerCompatRequest
import com.connectrpc.conformance.v1.ServerCompatResponse
import com.connectrpc.conformance.v1.ServiceProto
import com.connectrpc.conformance.v1.SuiteProto
import com.connectrpc.extensions.GoogleJavaJSONStrategy
import com.connectrpc.extensions.GoogleJavaProtobufStrategy
import com.connectrpc.server.HandlerRegistry
import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import kotlin.system.exitProcess

// The conformance runner and a server under test exchange one
// ServerCompatRequest and one ServerCompatResponse, each prefixed with its
// length as a 4-byte big-endian integer (connectrpc/conformance
// `docs/testing_servers.md`).

/** Reads the runner's length-prefixed [ServerCompatRequest]. */
fun readServerCompatRequest(input: InputStream): ServerCompatRequest {
    val data = DataInputStream(input)
    val bytes = ByteArray(data.readInt())
    data.readFully(bytes)
    return ServerCompatRequest.parseFrom(bytes)
}

/** Writes the length-prefixed [ServerCompatResponse] for a server listening on 127.0.0.1:[port]. */
fun writeServerCompatResponse(output: OutputStream, port: Int, pemCert: ByteArray?) {
    val response = ServerCompatResponse.newBuilder().setHost("127.0.0.1").setPort(port)
    if (pemCert != null) response.pemCert = ByteString.copyFrom(pemCert)
    val bytes = response.build().toByteArray()
    val data = DataOutputStream(output)
    data.writeInt(bytes.size)
    data.write(bytes)
    data.flush()
}

/** Whether [request] asks for HTTP/2; exits for versions other than HTTP/1.1 and HTTP/2. */
fun wantsHttp2(request: ServerCompatRequest): Boolean = when (request.httpVersion) {
    HTTPVersion.HTTP_VERSION_1 -> false

    HTTPVersion.HTTP_VERSION_2 -> true

    else -> {
        System.err.println("server only supports HTTP/1.1 and HTTP/2, got ${request.httpVersion}")
        exitProcess(1)
    }
}

/** The server certificate PEM when [request] asks for TLS, else null; exits when TLS lacks a cert or key. */
fun serverCertPem(request: ServerCompatRequest): ByteArray? {
    if (!request.useTls) return null
    val creds = request.serverCreds
    if (creds.cert.isEmpty || creds.key.isEmpty) {
        System.err.println("use_tls=true but server_creds is missing cert/key")
        exitProcess(1)
    }
    return creds.cert.toByteArray()
}

/** The conformance service with the protobuf and JSON codecs. */
fun buildConformanceRegistry(): HandlerRegistry = HandlerRegistry.builder()
    .codec(GoogleJavaProtobufStrategy())
    .codec(GoogleJavaJSONStrategy(buildConformanceTypeRegistry()))
    .registerAll(ConformanceServiceImpl().handlers())
    .build()

/**
 * The conformance handlers pack the original request into `Any` for error
 * details. Protobuf JSON requires a [TypeRegistry] that can resolve those type
 * URLs back to descriptors, so it holds everything declared in the
 * conformance v1 protos.
 */
private fun buildConformanceTypeRegistry(): TypeRegistry = TypeRegistry.newBuilder()
    .add(ServiceProto.getDescriptor().messageTypes)
    .add(ConfigProto.getDescriptor().messageTypes)
    .add(ServerCompatProto.getDescriptor().messageTypes)
    .add(ClientCompatProto.getDescriptor().messageTypes)
    .add(SuiteProto.getDescriptor().messageTypes)
    .build()
