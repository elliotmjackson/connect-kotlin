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

import okio.Buffer
import okio.BufferedSource
import okio.buffer
import okio.source
import java.io.IOException
import java.net.Socket

/** An HTTP/1.x response as it came off the wire. */
internal class RawResponse(
    val status: Int,
    /** Header fields in wire order, names as sent. */
    val headers: List<Pair<String, String>>,
    val body: ByteArray,
) {
    fun values(name: String): List<String> = headers.filter { it.first.equals(name, ignoreCase = true) }.map { it.second }

    fun header(name: String): String? = values(name).singleOrNull()
}

/** How [rawExchange] frames the request body. */
internal sealed interface Framing {
    /** `Content-Length`, body in one write. */
    object Length : Framing

    /** `Transfer-Encoding: chunked`, [size]-byte chunks, each flushed on its own. */
    class Chunked(val size: Int) : Framing
}

/**
 * Sends one HTTP/1.x request on a fresh connection exactly as given (no
 * normalization of the target or header names) and reads the whole response.
 * `Host` and `Connection: close` are added.
 */
internal fun rawExchange(
    port: Int,
    requestLine: String,
    headers: List<String> = emptyList(),
    body: ByteArray? = null,
    framing: Framing = Framing.Length,
): RawResponse = Socket("127.0.0.1", port).use { socket ->
    socket.soTimeout = 10_000
    val out = socket.getOutputStream()
    val head = StringBuilder("$requestLine\r\nHost: localhost\r\nConnection: close\r\n")
    headers.forEach { head.append(it).append("\r\n") }
    when {
        body == null -> Unit
        framing is Framing.Chunked -> head.append("Transfer-Encoding: chunked\r\n")
        else -> head.append("Content-Length: ${body.size}\r\n")
    }
    out.write(head.append("\r\n").toString().toByteArray(Charsets.ISO_8859_1))
    if (body != null && framing is Framing.Chunked) {
        out.flush()
        for (start in body.indices step framing.size) {
            val end = minOf(start + framing.size, body.size)
            out.write("${Integer.toHexString(end - start)}\r\n".toByteArray())
            out.write(body, start, end - start)
            out.write("\r\n".toByteArray())
            out.flush()
        }
        out.write("0\r\n\r\n".toByteArray())
    } else if (body != null) {
        out.write(body)
    }
    out.flush()
    readResponse(socket.getInputStream().source().buffer())
}

private fun readResponse(source: BufferedSource): RawResponse {
    val status = source.readUtf8LineStrict().split(' ')[1].toInt()
    val headers = generateSequence { source.readUtf8LineStrict().takeIf { it.isNotEmpty() } }
        .map { it.substringBefore(':') to it.substringAfter(':').trim() }
        .toList()
    val length = headers.firstOrNull { it.first.equals("Content-Length", ignoreCase = true) }?.second?.toLong()
    val chunked = headers.any { it.first.equals("Transfer-Encoding", ignoreCase = true) && it.second.equals("chunked", ignoreCase = true) }
    val body = when {
        length != null -> source.readByteArray(length)

        chunked -> {
            val buffer = Buffer()
            while (true) {
                val size = source.readUtf8LineStrict().substringBefore(';').trim().toLong(16)
                if (size == 0L) break
                source.readFully(buffer, size)
                source.readUtf8LineStrict()
            }
            buffer.readByteArray()
        }

        else -> source.readByteArray()
    }
    return RawResponse(status, headers, body)
}

/**
 * A cleartext HTTP/2 connection with prior knowledge (RFC 9113 §3.3) that
 * opens request streams and reads nothing until told to, and never grants
 * the server more flow-control window than the initial 65,535 bytes
 * (RFC 9113 §6.9.2). Closing it closes the TCP connection.
 */
internal class RawHttp2Connection(port: Int) : AutoCloseable {
    private val socket = Socket("127.0.0.1", port)
    private var nextStreamId = 1

    init {
        socket.getOutputStream().write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
        frame(type = 0x4, flags = 0, streamId = 0, payload = Buffer()) // SETTINGS
    }

    /**
     * Opens a stream for a POST to [path] whose request body stays open.
     *
     * @return The stream id.
     */
    fun openStream(path: String, contentType: String): Int {
        val streamId = nextStreamId
        nextStreamId += 2
        // Tomcat rejects pseudo-header names sent as literals, so they are sent by their
        // static-table index (RFC 7541 Appendix A): `:method: POST` (3) and `:scheme: http`
        // (6) whole (§6.1), `:authority` (1) and `:path` (4) with a literal value (§6.2.2).
        // Values are under 127 bytes and not Huffman-coded (§5.2).
        val block = Buffer().writeByte(0x83).writeByte(0x86)
        block.writeByte(0x01).writeByte("localhost".length).writeUtf8("localhost")
        block.writeByte(0x04).writeByte(path.length).writeUtf8(path)
        block.writeByte(0).writeByte("content-type".length).writeUtf8("content-type").writeByte(contentType.length).writeUtf8(contentType)
        frame(type = 0x1, flags = 0x4, streamId = streamId, payload = block) // HEADERS, END_HEADERS
        return streamId
    }

    /**
     * Reads what the server sent until a RST_STREAM frame for [streamId]
     * (RFC 9113 §6.4) and returns its error code; null if the connection
     * ends first or nothing arrives for [timeoutMs].
     */
    fun awaitReset(streamId: Int, timeoutMs: Int): Int? {
        socket.soTimeout = timeoutMs
        val source = socket.getInputStream().source().buffer()
        try {
            while (true) {
                val length = (source.readByte().toInt() and 0xff shl 16) or (source.readShort().toInt() and 0xffff)
                val type = source.readByte().toInt()
                source.readByte() // flags
                val id = source.readInt() and 0x7fffffff
                if (type == 0x3 && id == streamId) return source.readInt()
                source.skip(length.toLong())
            }
        } catch (e: IOException) {
            return null
        }
    }

    private fun frame(type: Int, flags: Int, streamId: Int, payload: Buffer) {
        val size = payload.size.toInt()
        val frame = Buffer().writeByte(size ushr 16).writeByte(size ushr 8).writeByte(size).writeByte(type).writeByte(flags).writeInt(streamId)
        frame.writeAll(payload)
        socket.getOutputStream().write(frame.readByteArray())
        socket.getOutputStream().flush()
    }

    override fun close() = socket.close()
}
