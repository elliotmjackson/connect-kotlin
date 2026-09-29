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
 * The handlers, codecs and interceptors served by a [ConnectServer]. Created
 * with [builder].
 */
class HandlerRegistry private constructor(
    // Keyed by procedure name, e.g. `connectrpc.eliza.v1.ElizaService/Say`.
    internal val handlers: Map<String, Handler<*, *>>,
    internal val codecs: List<SerializationStrategy>,
    // In registration order; the first is outermost.
    internal val interceptors: List<ServerInterceptor>,
    private val perProcedureInterceptors: Map<String, List<ServerInterceptor>>,
) {
    /**
     * Returns the interceptors registered for [procedure] alone, which run
     * inside the global ones.
     */
    internal fun interceptorsFor(procedure: String): List<ServerInterceptor> = perProcedureInterceptors[procedure] ?: emptyList()

    /**
     * Builds a [HandlerRegistry]. Registering a second handler for a procedure,
     * or a second codec with the same serialization name, throws
     * [IllegalArgumentException] and leaves the builder unchanged.
     */
    class Builder {
        private val handlers = mutableMapOf<String, Handler<*, *>>()
        private val codecs = mutableMapOf<String, SerializationStrategy>()
        private val interceptors = mutableListOf<ServerInterceptor>()
        private val perProcedureInterceptors = mutableMapOf<String, MutableList<ServerInterceptor>>()

        /**
         * Registers a handler for its [Handler.methodSpec] path.
         *
         * @param handler The handler to serve.
         * @param interceptors Interceptors for this procedure only. They run
         *     inside the global interceptors, in the order given.
         */
        @JvmOverloads
        fun register(
            handler: Handler<*, *>,
            interceptors: List<ServerInterceptor> = emptyList(),
        ): Builder = apply {
            val path = handler.methodSpec.path
            require(path !in handlers) { duplicateHandler(path) }
            handlers[path] = handler
            if (interceptors.isNotEmpty()) {
                perProcedureInterceptors.getOrPut(path) { mutableListOf() }.addAll(interceptors)
            }
        }

        /**
         * Registers every handler in [handlers], such as the result of a
         * generated `<Service>Handler.handlers()`.
         *
         * @param handlers The handlers to serve.
         * @param interceptors Interceptors for these procedures only, such as
         *     one service's. They run inside the global interceptors, in the
         *     order given.
         */
        @JvmOverloads
        fun registerAll(
            handlers: Iterable<Handler<*, *>>,
            interceptors: List<ServerInterceptor> = emptyList(),
        ): Builder = apply {
            val list = handlers.toList()
            val paths = HashSet<String>()
            for (h in list) {
                val path = h.methodSpec.path
                require(path !in this.handlers && paths.add(path)) { duplicateHandler(path) }
            }
            for (h in list) register(h, interceptors)
        }

        /**
         * Adds a codec. Requests choose one by content type; at least one is
         * required.
         */
        fun codec(strategy: SerializationStrategy): Builder = apply {
            val name = strategy.serializationName()
            require(name !in codecs) { "duplicate codec registered for serialization $name" }
            codecs[name] = strategy
        }

        private fun duplicateHandler(path: String) = "duplicate handler registered for procedure $path"

        /**
         * Adds an interceptor for every procedure. The first interceptor added
         * is outermost: it sees the request first and the response last.
         */
        fun interceptor(interceptor: ServerInterceptor): Builder = apply {
            interceptors += interceptor
        }

        /**
         * Creates the registry.
         *
         * @throws IllegalArgumentException if no codec was added.
         */
        fun build(): HandlerRegistry {
            require(codecs.isNotEmpty()) { "at least one codec must be registered" }
            return HandlerRegistry(
                handlers = handlers.toMap(),
                codecs = codecs.values.toList(),
                interceptors = interceptors.toList(),
                perProcedureInterceptors = perProcedureInterceptors.mapValues { it.value.toList() },
            )
        }
    }

    companion object {
        /** Returns a new [Builder]. */
        fun builder(): Builder = Builder()
    }
}
