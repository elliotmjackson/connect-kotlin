# connect-kotlin-server-ktor

A Ktor adapter for the Connect-Kotlin server runtime. Speaks the
[Connect][connect-protocol], [gRPC][grpc-protocol], and
[gRPC-Web][grpc-web-protocol] protocols on a single set of routes; the
wire protocol is selected from the request `Content-Type`.

## Status

Conformance: 1367 / 1367 (Connect + gRPC + gRPC-Web, HTTP/1.1 and HTTP/2,
all four stream types, Connect-GET, TLS with optional client certs,
message receive limits, gzip and deflate compression).

Not yet supported: HTTP/2 prior-knowledge over cleartext (`h2c`) for gRPC
clients — Ktor's `enableH2c` only performs the HTTP/1.1 Upgrade dance,
which gRPC clients don't use. Run over TLS or HTTP/1.1 in production.

## Quickstart

`build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.connectrpc:connect-kotlin-server-ktor:<version>")
    // Codec — pick proto, json, or both.
    implementation("com.connectrpc:connect-kotlin-google-java-ext:<version>")
}
```

Generate server stubs by passing `generateServerHandler=true` to
`protoc-gen-connect-kotlin`. For each service `Foo`, the plugin emits an
abstract `FooHandler` with one suspending method per RPC plus a
`handlers()` factory that returns `List<Handler<*, *>>`.

Implement the service:

```kotlin
class ElizaServiceImpl : ElizaServiceHandler() {
    override suspend fun say(request: SayRequest, ctx: HandlerContext): SayResponse =
        SayResponse.newBuilder().setSentence("you said: ${request.sentence}").build()

    override suspend fun introduce(
        request: IntroduceRequest,
        ctx: HandlerContext,
        stream: ServerMessageStream<IntroduceResponse>,
    ) {
        listOf("Hi", "I'm Eliza").forEach {
            stream.send(IntroduceResponse.newBuilder().setSentence(it).build())
        }
    }
}
```

Mount it on a Ktor server:

```kotlin
fun main() {
    val registry = HandlerRegistry.builder()
        .codec(GoogleJavaProtobufStrategy())
        .codec(GoogleJavaJSONStrategy())
        .registerAll(ElizaServiceImpl().handlers())
        .build()

    embeddedServer(Netty, port = 8080) {
        connectRpc(registry)
    }.start(wait = true)
}
```

That's the whole thing — three Connect-family protocols, two codecs, both
HTTP versions, all served from one mount point.

## Interceptors

Cross-cutting concerns (auth, logging, tracing, metrics) go through
`ServerInterceptor`. Register globally on the builder, or per-procedure
via the second `register()` overload:

```kotlin
val registry = HandlerRegistry.builder()
    .codec(GoogleJavaProtobufStrategy())
    .interceptor(LoggingInterceptor(log))            // global
    .register(ElizaServiceImpl().handlers().first(),
              listOf(RateLimitInterceptor(perRpc = 100))) // per-procedure
    .build()
```

The first interceptor registered runs outermost — it sees the request
first and the response last.

## Custom compression

The runtime ships gzip and deflate. To plug in brotli or zstd, implement
`CompressionPool` and register it:

```kotlin
HandlerRegistry.builder()
    .codec(GoogleJavaProtobufStrategy())
    .compressionPool(BrotliCompressionPool)
    .registerAll(ElizaServiceImpl().handlers())
    .build()
```

Use `removeDefaultCompressionPools()` if you want only your own pools.

## Options

| Parameter                       | Default | Purpose                                                                  |
| ------------------------------- | ------- | ------------------------------------------------------------------------ |
| `maxReceiveMessageSize`         | `0`     | Cap on a single request message after decompression. `0` = unlimited.    |
| `requireConnectProtocolHeader`  | `false` | Reject Connect POSTs missing `Connect-Protocol-Version: 1`.              |
| `compressMinBytes`              | `1024`  | Smallest response message eligible for outbound compression.             |

## Cancellation and shutdown

- Per-request deadlines from `Connect-Timeout-Ms` or `Grpc-Timeout` are
  enforced as `withTimeout` around the handler; on expiry, handlers
  observe `CancellationException` and the framework returns
  `code: deadline_exceeded`.
- Client disconnects propagate to the handler coroutine via the Netty
  channel's close future, so a cancelled or dropped call cancels the
  in-flight handler.
- `Application.stop()` cancels in-flight handlers cleanly through the
  same channel-close mechanism — see `GracefulShutdownTest`.

[connect-protocol]: https://connectrpc.com
[grpc-protocol]: https://grpc.io
[grpc-web-protocol]: https://github.com/grpc/grpc-web
