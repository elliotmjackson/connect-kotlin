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

package com.connectrpc.server.springboot

import com.connectrpc.AnyError
import com.connectrpc.CODEC_NAME_PROTO
import com.connectrpc.Codec
import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ErrorDetailParser
import com.connectrpc.SerializationStrategy
import okio.Buffer
import okio.BufferedSource
import kotlin.reflect.KClass

/** Opaque-bytes test message — same shape as :server-ktor's TestProtocol. */
internal data class TestMessage(val bytes: ByteArray) {
    constructor(text: String) : this(text.toByteArray(Charsets.UTF_8))

    override fun equals(other: Any?): Boolean =
        other is TestMessage && bytes.contentEquals(other.bytes)

    override fun hashCode(): Int = bytes.contentHashCode()

    fun text(): String = String(bytes, Charsets.UTF_8)
}

internal object TestSerializationStrategy : SerializationStrategy {
    override fun serializationName(): String = CODEC_NAME_PROTO

    @Suppress("UNCHECKED_CAST")
    override fun <E : Any> codec(clazz: KClass<E>): Codec<E> = TestCodec as Codec<E>

    override fun errorDetailParser(): ErrorDetailParser = NoopParser
}

private object TestCodec : Codec<TestMessage> {
    override fun encodingName(): String = CODEC_NAME_PROTO
    override fun serialize(message: TestMessage): Buffer = Buffer().write(message.bytes)
    override fun deterministicSerialize(message: TestMessage): Buffer = serialize(message)
    override fun deserialize(source: BufferedSource): TestMessage = TestMessage(source.readByteArray())
}

private object NoopParser : ErrorDetailParser {
    override fun <E : Any> unpack(any: AnyError, clazz: KClass<E>): E? = null
    override fun parseDetails(bytes: ByteArray): List<ConnectErrorDetail> = emptyList()
}
