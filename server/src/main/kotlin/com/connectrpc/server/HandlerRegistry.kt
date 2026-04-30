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

import com.connectrpc.SerializationStrategy

/**
 * A collection of [Handler]s keyed by procedure path, plus the codecs the server
 * will use to encode/decode wire messages.
 *
 * Built via [Builder] and consumed by transport adapters (e.g. the Ktor module)
 * to dispatch incoming requests.
 */
class HandlerRegistry private constructor(
    private val handlers: Map<String, Handler<*, *>>,
    private val codecs: Map<String, SerializationStrategy>,
    /**
     * Interceptors in registration order — the first registered is outermost
     * (sees the request first, response last).
     */
    val interceptors: List<ServerInterceptor>,
) {
    /** All registered procedure paths. */
    val procedures: Set<String> get() = handlers.keys

    /** All registered codec names (e.g. "proto", "json"). */
    val codecNames: Set<String> get() = codecs.keys

    /** Look up the handler for the given fully-qualified procedure path. */
    fun find(procedure: String): Handler<*, *>? = handlers[procedure]

    /** Resolve a codec by serialization name (e.g. "proto", "json"). */
    fun codec(name: String): SerializationStrategy? = codecs[name]

    class Builder {
        private val handlers = mutableMapOf<String, Handler<*, *>>()
        private val codecs = mutableMapOf<String, SerializationStrategy>()
        private val interceptors = mutableListOf<ServerInterceptor>()

        fun register(handler: Handler<*, *>): Builder = apply {
            val path = handler.methodSpec.path
            require(handlers.put(path, handler) == null) {
                "duplicate handler registered for procedure $path"
            }
        }

        /**
         * Registers all handlers in the iterable. Useful with the generated
         * `<Service>Handler.handlers()` helper from protoc-gen-connect-kotlin.
         */
        fun registerAll(handlers: Iterable<Handler<*, *>>): Builder = apply {
            for (h in handlers) register(h)
        }

        /**
         * Adds a codec available for content-type negotiation. At least one codec
         * must be registered before [build].
         */
        fun codec(strategy: SerializationStrategy): Builder = apply {
            val name = strategy.serializationName()
            require(codecs.put(name, strategy) == null) {
                "duplicate codec registered for serialization $name"
            }
        }

        /** Registers an interceptor. Interceptors run in the order they were added. */
        fun interceptor(interceptor: ServerInterceptor): Builder = apply {
            interceptors += interceptor
        }

        fun build(): HandlerRegistry {
            require(codecs.isNotEmpty()) { "at least one codec must be registered" }
            return HandlerRegistry(handlers.toMap(), codecs.toMap(), interceptors.toList())
        }
    }

    companion object {
        fun builder(): Builder = Builder()
    }
}
