# connect-kotlin-server-ktor

A Ktor adapter for the Connect-Kotlin server runtime. Speaks the
[Connect][connect-protocol], [gRPC][grpc-protocol], and
[gRPC-Web][grpc-web-protocol] protocols on a single set of routes; the
wire protocol is selected from the request `Content-Type`.

## Status

Conformance: **3798 / 3798** against the connectconformance harness with
every feature claimed — Connect + gRPC + gRPC-Web, HTTP/1.1 and HTTP/2,
TLS and cleartext including h2c prior-knowledge, both proto and JSON
codecs, all four stream types incl. full-duplex bidi, Connect-GET,
optional client certs, message receive limits, gzip and deflate
compression.

To run gRPC over HTTP/2 cleartext (h2c), enable both `enableHttp2 = true`
and `enableH2c = true` on the Netty engine. Connect and gRPC-Web also
work over HTTP/1.1 with no extra configuration.

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

    embeddedServer(
        factory = Netty,
        environment = applicationEnvironment { },
        configure = {
            connector { host = "0.0.0.0"; port = 8080 }
            enableHttp2 = true   // gRPC over HTTP/2 (with TLS or h2c)
            enableH2c = true     // HTTP/2 cleartext via prior-knowledge or Upgrade
        },
        module = { connectRpc(registry) },
    ).start(wait = true)
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
