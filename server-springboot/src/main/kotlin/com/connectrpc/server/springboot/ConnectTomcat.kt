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

import org.apache.catalina.connector.Connector
import org.apache.coyote.AbstractProtocol
import org.apache.coyote.http2.Http2Protocol

/** Tomcat connector settings for serving Connect. */
object ConnectTomcat {
    /**
     * Turns off Tomcat's reuse of request-processing objects across requests on
     * [connector]: the HTTP/1.1 processor cache (`processorCache=0`) and the HTTP/2
     * request and response pool (`discardRequestsAndResponses=true`).
     *
     * Handlers write from non-container threads. When such a write fails because the
     * client reset the connection or stream, Tomcat hands the failure to a container
     * thread, which completes and recycles the request while the handler thread is still
     * inside Tomcat. In Tomcat 11.0.22 `CLIENT_FLUSH` dispatches the error in
     * `handleIOException` before it calls `response.setErrorException(ioe)`
     * (`AbstractProcessor.java:120-123,404-412`, unchanged in 11.0.26);
     * `CoyoteOutputStream.java:194-215` describes the same race. Between keep-alive
     * requests the processor returns to the cache (`AbstractProtocol.java:1341-1346`) and
     * serves the next request on any connection, whose first write dispatch then finds
     * the stale exception and closes that connection without a response
     * (`CoyoteAdapter.java:164-188`). On HTTP/2 a late flush commits a pooled response
     * already handed to another stream (`Http2Protocol.java:435-453`). Objects that are
     * never reused keep the effect on the call whose client left.
     *
     * The auto-configuration applies this to every connector in the Tomcat service and
     * offers no way to turn it off; call it for a connector added to Tomcat some other
     * way. With reuse under mixed load with client cancellations, Go clients saw
     * HTTP/2 DATA frames before a response's HEADERS and HTTP/1.1 responses cut off
     * on calls nobody cancelled. Tomcat measured `discardRequestsAndResponses=true` at
     * 108k against 124k requests per second for a short JSON response
     * (`Http2Protocol.java:103-110`).
     *
     * @param connector The connector to change, before it starts or between requests.
     */
    @JvmStatic
    fun applyRequestObjectIsolation(connector: Connector) {
        (connector.protocolHandler as? AbstractProtocol<*>)?.processorCache = 0
        connector.findUpgradeProtocols().filterIsInstance<Http2Protocol>().forEach { it.discardRequestsAndResponses = true }
    }
}
