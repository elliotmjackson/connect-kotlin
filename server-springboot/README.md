# connect-kotlin-server-springboot

Spring Boot 4.1 auto-configuration that serves Connect-Kotlin handlers over the
[Connect][connect-protocol], [gRPC][grpc-protocol], and [gRPC-Web][grpc-web-protocol]
protocols from the application's servlet container. Requires JDK 17.

## Quick Start

```kotlin
dependencies {
    implementation("com.connectrpc:connect-kotlin-server-springboot:<version>")
    implementation("com.connectrpc:connect-kotlin-google-java-ext:<version>")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
}
```

Generate handler interfaces with `generateServerHandler=true`, implement them, and
declare a `HandlerRegistry` bean:

```kotlin
@Configuration
class RpcConfiguration {
    @Bean
    fun connectRpcRegistry(): HandlerRegistry =
        HandlerRegistry.builder()
            .codec(GoogleJavaProtobufStrategy())
            .codec(GoogleJavaJSONStrategy())
            .registerAll(ElizaServiceImpl().handlers())
            .build()
}
```

Each service is served under `/<package>.<Service>/`, for example
`/connectrpc.eliza.v1.ElizaService/Say`. Every other path stays with Spring MVC.

## Configuration

| Property                                          | Default | Purpose                                                                                   |
| ------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------- |
| `connectrpc.enabled`                              | `true`  | Register the Connect servlet.                                                             |
| `connectrpc.path-prefix`                          | `""`    | Serve every service under this path, e.g. `/rpc`. Many gRPC clients cannot send a prefix. |
| `connectrpc.read-max-bytes`                       | 4 MiB   | Largest request message, on the wire and after decompression. `0` = unlimited.            |
| `connectrpc.send-max-bytes`                       | `0`     | Largest response message. `0` = unlimited.                                                |
| `connectrpc.compress-min-bytes`                   | `1024`  | Smaller response messages are sent uncompressed.                                          |
| `connectrpc.require-connect-protocol-header`      | `false` | Reject Connect unary requests without `Connect-Protocol-Version: 1`.                      |
| `connectrpc.max-timeout`                          | unset   | Longest a call's request read and handler may run; replaces a longer or missing timeout.  |
| `connectrpc.shutdown-grace-period`                | `30s`   | Time calls in flight get at shutdown before they end with `unavailable`.                  |
| `connectrpc.handler-threads`                      | `200`   | Handlers running at once without virtual threads; they may block.                         |
| `connectrpc.tomcat.http2-overhead-data-threshold` | unset   | Tomcat HTTP/2 `overheadDataThreshold`; see [Tomcat](#tomcat).                             |
| `connectrpc.tomcat.http2-overhead-reset-factor`   | unset   | Tomcat HTTP/2 `overheadResetFactor`; see [Tomcat](#tomcat).                               |

Handlers run on virtual threads when `spring.threads.virtual.enabled=true` (Java 21+).
To change compression pools, declare a `ServerConfig` bean; it replaces the limits
above:

```kotlin
@Bean
fun connectRpcServerConfig(): ServerConfig =
    ServerConfig(compressionPools = listOf(GzipServerCompressionPool))
```

## Production settings

Set `connectrpc.max-timeout` on any server that untrusted clients can reach. Without
it, a call whose client sends no deadline (`Connect-Timeout-Ms`, `grpc-timeout`) has no
time limit: a client that announces a request body and never sends it, or never
finishes it, holds its call until it disconnects.

- The servlet container's async timeout is off for Connect calls
  (`AsyncContext.setTimeout(0)`). An absolute container timeout would also end healthy
  long-lived streams and fail calls without a Connect error; instead the server
  enforces each call's deadline, the client's or `connectrpc.max-timeout`, whichever is
  shorter, while reading the request and running the handler. A handler still running
  at the deadline is cancelled and the call fails with `deadline_exceeded`, which is
  sent once the handler has returned. Blocking code (JDBC, a blocking HTTP client) is
  not interrupted: the call ends when it returns, and a result it returns after the
  deadline is discarded for `deadline_exceeded`.
- The deadline does not bound writing the response, as in connect-go v1.20.0, whose
  deadline is the handler's context (`handler.go:317-326`) while `net/http` bounds
  writes only if `http.Server.WriteTimeout` is set (Go 1.25 `server.go:3033-3038`,
  per HTTP/2 stream `h2_bundle.go:6171-6172`). A client that stops reading holds its
  response for as long as Tomcat keeps writing to it (Tomcat 11.0.22):
  - HTTP/1.1: a write that makes no progress for the connector's `connectionTimeout`
    fails and ends the call, 60 s by default (`http11/Constants.java:26`,
    `NioEndpoint.java:491, 1042-1063`); set it with `server.tomcat.connection-timeout`.
  - HTTP/2, a client that stops reading its connection: a socket write of response
    data that makes no progress fails after `writeTimeout`, 5 s (`Http2Protocol.java:48`,
    `Http2AsyncUpgradeHandler.java:229-255`), and the call ends.
  - HTTP/2, a client that keeps reading its connection but grants a stream no more
    flow-control window: no Tomcat timeout bounds the stalled response, whether the
    request body is still open or already ended. `streamWriteTimeout` (20 s,
    `Http2Protocol.java:51`) only bounds blocking writes and this servlet writes
    non-blocking (`Stream.java:257-273`). `streamReadTimeout` (20 s,
    `Http2Protocol.java:50`) is armed only while the server waits for request data it
    asked to read (`Stream.java:1336-1343, 1526-1529`) and disarmed when request data
    arrives (`Stream.java:1389-1391`): a call that has read what the client sent, or
    leaves arrived data unread, is never reset by it, even with its request body still
    open. The call lasts until the client resets the stream or closes the
    connection. Servers that untrusted HTTP/2 clients reach need a proxy in front, or
    another transport layer, that bounds each stream's response writes.
- A response that fails part way (its write fails, or a handler throws an `Error`
  after it started) is aborted, so its client cannot take part of it for all of it:
  Tomcat resets its HTTP/2 stream or closes its HTTP/1.1 connection, and the async
  request ends. Other servlet containers get `AsyncContext.complete()`, which commits
  and closes the response: a stream then ends without its end-of-stream message or
  `grpc-status`, which clients read as an error, but a unary response whose whole body
  the container already holds reaches the client whole.
- Each open call keeps an async request, a coroutine, and on HTTP/1.1 a request buffer
  of `server.max-http-request-header-size` (8 KiB by default) plus Tomcat's 8 KiB
  socket read buffer (see [Tomcat](#tomcat)), so unbounded stalled calls cost about
  16 KiB of heap each by default, more with a larger header limit.
- For public-facing unary services, a limit a little above your slowest expected call
  is a good start, e.g. `connectrpc.max-timeout=30s`, together with the default
  `connectrpc.read-max-bytes` (4 MiB) or lower. Services with long-lived streams need
  a limit above their longest stream; serve them separately if that is much longer
  than your unary calls.

connect-go and connect-es set no default either: connect-go leaves timeouts to
`net/http`'s `http.Server` (`ReadHeaderTimeout`, `ReadTimeout`, `IdleTimeout`;
[deployment docs][connect-go-deployment]), and connect-es accepts any client timeout
unless `maxTimeoutMs` is set (`@connectrpc/connect` `universal-handler.ts`, default
`Number.MAX_SAFE_INTEGER`), leaving connection timeouts to the Node.js server.

## Spring Security and Spring MVC

- Every servlet filter mapped to a Connect path runs, including Spring Security and
  `CorsFilter`. A filter's rejection is a plain HTTP error, which Connect clients map
  to a code from the status ([protocol][http-to-error-code]).
- Handlers see the request's `SecurityContextHolder`, `RequestContextHolder`,
  `LocaleContextHolder` (from the `localeResolver` bean) and SLF4J MDC.
- With CSRF protection on, exclude the Connect paths, for example with
  `connectrpc.path-prefix=/rpc`:

  ```kotlin
  http.csrf { it.ignoringRequestMatchers("/rpc/**") }
  ```

- Spring MVC `HandlerInterceptor`s, `@ControllerAdvice` and MVC CORS (`@CrossOrigin`,
  `addCorsMappings`) belong to `DispatcherServlet` and do not apply. Use a servlet
  filter or a `ServerInterceptor` (`HandlerRegistry.Builder.interceptor`).
- `http.server.requests` metrics tag each call with its procedure path as `uri`. For
  Connect-level metrics or tracing (procedure, protocol, Connect code, including
  requests rejected before a handler), declare a `ServerObserver` bean; it becomes the
  `observer` of the properties-built `ServerConfig`.

## Testing

MockMvc drives only `DispatcherServlet` and answers Connect paths with 404. Start the
server on a random port and call it with a Connect client:

```kotlin
// JUnit 5, with spring-boot-starter-test
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ElizaServiceTest {
    @LocalServerPort
    var port: Int = 0

    @Test
    fun say() = runBlocking {
        val client = ElizaServiceClient(
            ProtocolClient(
                ConnectOkHttpClient(),
                ProtocolClientConfig(host = "http://localhost:$port", serializationStrategy = GoogleJavaProtobufStrategy()),
            ),
        )
        val response = client.say(SayRequest.newBuilder().setSentence("hi").build())
        assertThat(response.success { it.message.sentence }).isNotNull()
    }
}
```

## Tomcat

- On HTTP/2, connect-go clients (Go's HTTP/2 transport) send each streamed message's
  5-byte prefix in its own DATA frame, and Tomcat closes the connection with
  `ENHANCE_YOUR_CALM`. Set `connectrpc.tomcat.http2-overhead-data-threshold=0` to turn
  off that one check ([Tomcat HTTP/2 docs][tomcat-http2]).
- Connect GET requests carry the message in the URL, so
  `server.max-http-request-header-size` (8 KiB by default) bounds GET messages. Tomcat
  allocates a buffer of that size plus its 8 KiB socket read buffer for every HTTP/1.1
  request in progress, async calls included, and holds it until the request ends
  (Tomcat 11 `Http11InputBuffer.init`, `AbstractProtocol.ConnectionHandler`). A 1 MiB
  limit therefore costs about 1 MiB of heap per concurrent HTTP/1.1 call.
- A handler write that fails because its client reset the connection can make Tomcat
  close a later, unrelated request without a response, on any connection (Tomcat 11
  `AbstractProcessor` records the failure after the request was recycled). To keep the
  failure on the call whose client left, request-processing objects are not reused:
  `processorCache=0` and HTTP/2 `discardRequestsAndResponses=true`. This applies to
  every connector of the Tomcat service, including connectors added with
  `addAdditionalConnectors`, whether or not your factory passes them to
  `customizeConnector`, and it overrides `server.tomcat.processor-cache`. For a
  connector added to Tomcat another way, call
  `ConnectTomcat.applyRequestObjectIsolation(connector)` before it starts. There is no
  switch to keep Tomcat's reuse: under mixed load with cancellations, reuse gave Go
  clients HTTP/2 DATA frames before a response's HEADERS and cut-off HTTP/1.1 responses
  on calls nobody cancelled. Measured cost with a Connect unary JSON call, 50 concurrent
  keep-alive clients for 30 s each: HTTP/1.1 served up to 27% fewer requests per second
  (median 16% over six paired runs, one run 5% faster) and p99 latency rose by
  0.4–2.1 ms. h2c showed no difference beyond run-to-run noise (−8% and +3% in two
  paired runs).
- On HTTP/2, Tomcat 11 (at least up to 11.0.26) can lose the notification for a request's
  last frame when that frame arrives while the call checks whether its body is readable.
  Tomcat then reports neither data nor the end of the body, also to a plain servlet. When
  the frame only ends the stream, as when a client streams its request body (connect-go
  client and bidi streaming calls), the call still sees the end of its body. When the frame
  also carries data, as with a body of known length (connect-go unary and server streaming
  calls), Tomcat keeps those bytes unreadable, and the call fails with `unavailable` after
  5 s instead of waiting for them until its deadline, or forever without one. With 50
  concurrent Go clients sending 2-byte unary bodies over h2c (Tomcat 11.0.22) this hit
  about 1 in 6,000 to 8,000 calls. Clients that retry `unavailable` recover. The related
  case where `isFinished()` reports the body complete too early is handled.
- Tomcat closes the whole HTTP/2 connection (`GOAWAY STREAM_CLOSED`) when a client resets
  a stream after both sides have sent END_STREAM. Go clients do this when a call is
  cancelled while its last response frame is in flight, although RFC 9113 §5.1 allows
  it. The other calls on that connection fail as `unavailable`.
- Tomcat 11 (at least up to 11.0.26) keeps HTTP/2 flow-control credit for request bytes
  a servlet leaves unread when its response ends normally, and its connection window is
  65,535 bytes by default. Connect calls read and drop the bytes Tomcat holds when they
  end. Bytes that arrive while Tomcat writes the end of the response are still lost,
  for example when a client flushes a pending message as it closes a stream the server
  already ended, and so are bytes other servlets on the connection leave unread (a
  plain servlet does). Once the window is used up, requests on that connection wait
  for their body until Tomcat's `streamReadTimeout` (20 s) resets them with
  `INTERNAL_ERROR`.
- Tomcat answers each RST_STREAM a client sends for a call still running with an
  RST_STREAM of its own (`INTERNAL_ERROR`), although RFC 9113 §5.4.2 forbids it. Both
  count toward the connection's overhead limit: +`overheadResetFactor` (50) per reset,
  −20 per DATA frame sent or received and per HEADERS frame received, starting at −100;
  above zero Tomcat closes the connection with `ENHANCE_YOUR_CALM`
  ([Tomcat HTTP/2 docs][tomcat-http2]). A client that cancels each call after its first
  response message loses the connection at the third to fifth cancel, and the other
  calls on it fail as `unavailable`. `connectrpc.tomcat.http2-overhead-reset-factor`
  lowers the increment, at the cost of Tomcat's protection against the HTTP/2 rapid
  reset attack: the reset count is Tomcat's fix for CVE-2023-44487
  ([Tomcat security][tomcat-security]). On one h2c connection (Tomcat 11.0.22), with
  the default a client that sent HEADERS and RST_STREAM back to back was cut off after
  7 streams, and 100 cancel-after-first-message calls took 30 connections. With `20`,
  300 such calls shared one connection, and the rapid-reset client opened and reset
  about 6,900 streams before its connection ended, for an unrelated error, without the
  overhead limit tripping. Lower it only when the clients are trusted or a proxy in
  front limits resets itself.

[connect-protocol]: https://connectrpc.com/docs/protocol
[grpc-protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
[grpc-web-protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md
[http-to-error-code]: https://connectrpc.com/docs/protocol#http-to-error-code
[connect-go-deployment]: https://connectrpc.com/docs/go/deployment
[tomcat-http2]: https://tomcat.apache.org/tomcat-11.0-doc/config/http2.html
[tomcat-security]: https://tomcat.apache.org/security-11.html
