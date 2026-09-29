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

import com.connectrpc.Code
import com.connectrpc.StreamResult
import com.connectrpc.StreamType
import com.connectrpc.http.UnaryHTTPRequest
import com.connectrpc.server.http.InMemoryHTTPClient
import io.ktor.http.Url
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okio.Buffer
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.io.IOException

class InMemoryHTTPClientTest {
    /**
     * A response write that fails: the exchange is aborted, and the client
     * learns the stream failed instead of seeing it end as if it were whole.
     */
    @Test
    fun abortedResponseEndsTheStreamWithAnError() = runBlocking<Unit> {
        val server = server(serverStream { request, _, stream -> stream.send(request) })
        val ended = CompletableDeferred<StreamResult.Complete<Buffer>>()
        val request = UnaryHTTPRequest(
            Url("http://in-memory/$SVC/ServerStream"),
            "application/connect+proto",
            null,
            emptyMap(),
            spec("ServerStream", StreamType.SERVER),
            Buffer(),
        )
        val stream = InMemoryHTTPClient(server).stream(request, duplex = false) { result ->
            when (result) {
                is StreamResult.Message -> throw IOException("client stopped reading")
                is StreamResult.Complete -> ended.complete(result)
                is StreamResult.Headers -> Unit
            }
        }
        stream.send(Buffer().write(env(0, "a")))
        stream.sendClose()
        val cause = withTimeout(5_000) { ended.await() }.cause
        assertThat(cause?.code).isEqualTo(Code.CANCELED)
    }
}
