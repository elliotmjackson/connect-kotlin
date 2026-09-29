# connect-kotlin-server-ktor

Serves Connect-Kotlin handlers from [Ktor][ktor] over the [Connect][connect-protocol],
[gRPC][grpc-protocol], and [gRPC-Web][grpc-web-protocol] protocols. The protocol is
chosen per request from its `Content-Type`.

## Quick Start

```kotlin
dependencies {
    implementation("com.connectrpc:connect-kotlin-server-ktor:<version>")
    implementation("com.connectrpc:connect-kotlin-google-java-ext:<version>")
    implementation("io.ktor:ktor-server-netty:<ktor-version>")
}
```

Generate handler interfaces with `generateServerHandler=true`, implement them, and
mount the registry:

```kotlin
class ElizaServiceImpl : ElizaServiceHandler {
    override suspend fun say(request: SayRequest, ctx: HandlerContext): SayResponse =
        SayResponse.newBuilder().setSentence("You said: ${request.sentence}").build()

    override suspend fun introduce(
        request: IntroduceRequest,
        ctx: HandlerContext,
        stream: ServerMessageStream<IntroduceResponse>,
    ) {
        for (sentence in listOf("Hi", "I'm Eliza")) {
            stream.send(IntroduceResponse.newBuilder().setSentence(sentence).build())
        }
    }

    override suspend fun converse(stream: BidiStream<ConverseRequest, ConverseResponse>, ctx: HandlerContext) {
        while (true) {
            val request = stream.receive() ?: break
            stream.send(ConverseResponse.newBuilder().setSentence("You said: ${request.sentence}").build())
        }
    }
}

fun main() {
    val registry = HandlerRegistry.builder()
        .codec(GoogleJavaProtobufStrategy())
        .codec(GoogleJavaJSONStrategy())
        .registerAll(ElizaServiceImpl().handlers())
        .build()
    embeddedServer(Netty, configure = {
        connector { port = 8080 }
        enableHttp2 = true
        enableH2c = true
    }) {
        connectRpc(registry)
    }.start(wait = true)
}
```

Handlers throw `ConnectException` to fail a call with a code. Response headers and
trailers go in `ctx.responseHeaders` and `ctx.responseTrailers`.

## Configuration

`connectRpc(registry, ServerConfig(...))`:

| Parameter                      | Default       | Purpose                                                                          |
| ------------------------------ | ------------- | -------------------------------------------------------------------------------- |
| `readMaxBytes`                 | 4 MiB         | Largest request message, on the wire and after decompression. `0` = unlimited.   |
| `sendMaxBytes`                 | `0`           | Largest response message. `0` = unlimited.                                       |
| `compressionPools`             | gzip, deflate | Accepted and advertised encodings; replaces the default set.                     |
| `compressMinBytes`             | `1024`        | Smaller response messages are sent uncompressed.                                 |
| `requireConnectProtocolHeader` | `false`       | Reject Connect unary requests without `Connect-Protocol-Version: 1`.             |

To add an encoding such as zstd, implement `ServerCompressionPool` and list it with the
built-in pools you still want:

```kotlin
connectRpc(registry, ServerConfig(compressionPools = listOf(ZstdServerCompressionPool, GzipServerCompressionPool)))
```

To mount under a prefix, create the `ConnectServer` yourself:

```kotlin
routing {
    route("/api") { connectRpc(ConnectServer(registry)) }
}
```

Procedure paths are matched byte for byte: a raw path with an empty, `.` or `..`
segment or any percent-encoding gets 404, although Ktor routing would match it.

## Interceptors

`ServerInterceptor` adds authentication, logging, tracing and metrics. Its
`interceptCall` runs once per call of every stream type before the request is
read, so put authentication there; the `wrap*` functions wrap handlers for work on
messages. Register one for every procedure, for one service with `registerAll`, or
for one procedure with `register`:

```kotlin
val registry = HandlerRegistry.builder()
    .codec(GoogleJavaProtobufStrategy())
    .interceptor(LoggingInterceptor(Logger.getLogger("rpc")))
    .registerAll(ElizaServiceImpl().handlers(), listOf(AuthInterceptor()))
    .build()
```

The first interceptor registered is outermost: it sees the request first and the
response last.

Interceptors never see requests rejected before a call starts (unknown path,
unsupported content type or compression). For metrics and tracing that count them,
set `ServerConfig(observer = …)`: a `ServerObserver` is told the procedure, protocol
and Connect code of every request.

## Engines

Connect and gRPC-Web work on every Ktor engine. gRPC needs HTTP trailers, which this
adapter sends only on the Netty engine over HTTP/2; elsewhere gRPC calls fail with
`unimplemented`. `ktor-server-netty` is a compile-only dependency of this module, so
add it yourself.

## Cancellation and timeouts

- The deadline a client sends (`Connect-Timeout-Ms`, `grpc-timeout`) covers reading
  the request and running the handler; on expiry the handler is cancelled and the call
  fails with `deadline_exceeded`, sent once the handler has returned. Blocking code is
  not interrupted: a result it returns after the deadline is discarded for
  `deadline_exceeded`. Handlers read the time left with `ctx.timeRemaining()`.
- A client disconnect cancels the handler, and on HTTP/2 fails its request body: on Netty
  the adapter watches each call's stream (HTTP/2) or connection (HTTP/1.1) itself, since
  Ktor 3.6's `HttpRequestLifecycle` misses calls whose client left before routing reached
  them; on CIO that plugin, installed on the Connect routes, does it.
- A client that stops reading suspends `send` instead of buffering responses. The
  deadline does not bound writing the response, as in connect-go, whose deadline is the
  handler's context while `http.Server.WriteTimeout` bounds writes; the engine's write
  limits below do. A response that fails part way (its write fails, or a handler throws
  an `Error` after it started) is aborted: on Netty the adapter resets the HTTP/2 stream
  (RST_STREAM `CANCEL`) or closes the HTTP/1.1 connection; on other engines the engine
  handles the failed response.
- `ServerConfig(maxTimeout = 30.seconds)` replaces a longer or missing client deadline.
  Without it, a client that sends no deadline and stalls its request holds its call
  until the engine closes the connection. Set the engine's limits in production too. On
  Netty (Ktor 3.6):

```kotlin
embeddedServer(Netty, configure = {
    // HTTP/1.1 only: close a connection idle for this long (default 0 = never). It also
    // ends a server stream whose client has finished sending, so size it above your
    // longest stream.
    requestReadTimeoutSeconds = 60
    // HTTP/1.1 only: close the connection when a response write has not completed after
    // this many seconds (default 10), which ends a call whose client stopped reading.
    responseWriteTimeoutSeconds = 10
    // Request line and headers; Connect GET URLs can exceed the defaults (4096 / 8192).
    maxInitialLineLength = 16 * 1024
    maxHeaderSize = 16 * 1024
    // HTTP/2 connections get none of the timeouts above; add Netty handlers yourself,
    // e.g. close after 5 idle minutes:
    channelPipelineConfig = {
        addFirst("idle", IdleStateHandler(0, 0, 300))
        addAfter("idle", "idleClose", object : ChannelDuplexHandler() {
            override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
                if (evt is IdleStateEvent) ctx.close() else ctx.fireUserEventTriggered(evt)
            }
        })
    }
}) { connectRpc(registry) }
```

The read and write timeouts apply only to HTTP/1.1 pipelines (ktor-server-netty 3.6.0
`NettyChannelInitializer.kt:305-311, 341-346`; the default is `NettyApplicationEngine.kt:95`).
On HTTP/2 nothing bounds a response write: a client that stops reading, or grants a
stream no more flow-control window, holds its call until it resets the stream or the
connection closes. The idle handler above closes a connection on which nothing has been
read and no write has completed for 5 minutes (netty-handler 4.2.17
`IdleStateHandler.java:131-132, 304`), so it ends such a call only when its connection
carries nothing else; servers that untrusted HTTP/2 clients reach need a proxy in front
that bounds each stream's response writes. HTTP/2 uses Netty's defaults of 100
concurrent streams per connection and an 8 KiB header list (netty-codec-http2 4.2.17
`Http2Settings.java:306-309`, `Http2CodecUtil.java:111, 117, 127`), which Ktor 3.6.0
does not expose.

Netty also closes an HTTP/2 connection with GOAWAY `ENHANCE_YOUR_CALM` once its client
has reset more than 200 streams within 30 seconds (netty-codec-http2 4.2.17
`AbstractHttp2ConnectionHandlerBuilder.java:77, 681-694`,
`Http2MaxRstFrameListener.java:48-64`), and the calls in flight on it fail with
`unavailable`. Cancellations count: a client cancels a gRPC call by resetting its stream
([gRPC over HTTP/2][grpc-protocol], "Errors"), so one connection carries at most 200
cancelled calls in any 30 seconds. Ktor 3.6.0 builds Netty's HTTP/2 codec with these
defaults and has no setting for them (`NettyChannelInitializer.kt:245, 266`,
`NettyApplicationEngine.kt:73-175`), so Netty's `decoderEnforceMaxRstFramesPerWindow`
(`Http2MultiplexCodecBuilder.java:218-221`) is out of reach.

## Shutdown

When the engine stops (`server.stop()`, or the JVM shutdown hook `embeddedServer`
installs), `connectRpc` shuts its `ConnectServer` down before the engine closes any
connection: calls that start from then on fail with `unavailable`, calls in flight get
`shutdownGracePeriod` (default 1 s) and then end with `unavailable`, and `stop` waits
for their responses to be written before the engine goes on:

```kotlin
embeddedServer(Netty, port = 8080) {
    connectRpc(registry, shutdownGracePeriod = 10.seconds)
}.start(wait = true)
```

The engine's own `shutdownGracePeriod` does not wait for responses: on Netty it only
keeps the event loops running tasks, and connections close as soon as they start to
stop (ktor-server-netty 3.6.0 `NettyApplicationEngine.kt:547-555`, netty-transport
4.2.17 `SingleThreadIoEventLoop.java:195-201`).

[connect-protocol]: https://connectrpc.com/docs/protocol
[grpc-protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
[grpc-web-protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md
[ktor]: https://ktor.io
