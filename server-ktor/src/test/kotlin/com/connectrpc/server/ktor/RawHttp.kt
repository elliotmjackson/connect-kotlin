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

package com.connectrpc.server.ktor

import okio.Buffer
import okio.BufferedSource
import okio.buffer
import okio.source
import java.io.IOException
import java.net.InetSocketAddress
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
 * sends request streams' HEADERS frames as told, and reads what the server
 * sends only through [awaitFrame]. Closing it closes the TCP connection.
 *
 * @param receiveBufferSize The socket's receive buffer, set before it connects.
 * @param window When set, the flow-control window the client grants each stream
 *     (SETTINGS_INITIAL_WINDOW_SIZE) and the connection (WINDOW_UPDATE), so the
 *     server's writes are held back by the socket rather than by flow control.
 */
internal class RawHttp2Connection(port: Int, receiveBufferSize: Int? = null, window: Int? = null) : AutoCloseable {
    private val socket = Socket()
    private var nextStreamId = 1

    init {
        receiveBufferSize?.let { socket.receiveBufferSize = it }
        socket.connect(InetSocketAddress("127.0.0.1", port))
        socket.getOutputStream().write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
        val settings = Buffer()
        // SETTINGS_INITIAL_WINDOW_SIZE (RFC 9113 §6.5.2).
        window?.let { settings.writeShort(0x4).writeInt(it) }
        frame(type = 0x4, flags = 0, streamId = 0, payload = settings)
        // The connection window starts at 65,535 bytes whatever the settings (RFC 9113 §6.9.2).
        window?.let { frame(type = 0x8, flags = 0, streamId = 0, payload = Buffer().writeInt(it - 65_535)) }
    }

    /**
     * Opens a stream for a POST to [path]; its request body stays open unless
     * [endStream], which ends the request with its HEADERS frame.
     *
     * @return The stream id.
     */
    fun openStream(path: String, contentType: String, endStream: Boolean = false): Int {
        val streamId = nextStreamId
        nextStreamId += 2
        headers(streamId, listOf(":method" to "POST", ":scheme" to "http", ":authority" to "localhost", ":path" to path, "content-type" to contentType), endStream)
        return streamId
    }

    /**
     * Sends trailers without END_STREAM on [streamId], which makes the request
     * malformed (RFC 9113 §8.1): the server resets the stream.
     */
    fun sendTrailersWithoutEndStream(streamId: Int) = headers(streamId, listOf("x-trailer" to "1"), endStream = false)

    /**
     * Reads frames until one of [type] on [streamId] arrives, and returns
     * true; false when the connection ends first or no frame arrives for
     * [timeoutMs].
     */
    fun awaitFrame(type: Int, streamId: Int, timeoutMs: Int): Boolean {
        socket.soTimeout = timeoutMs
        val input = socket.getInputStream().source().buffer()
        try {
            while (true) {
                val size = (input.readByte().toInt() and 0xff shl 16) or (input.readShort().toInt() and 0xffff)
                val frameType = input.readByte().toInt()
                input.readByte() // flags
                val frameStream = input.readInt() and 0x7fffffff
                input.skip(size.toLong())
                if (frameType == type && frameStream == streamId) return true
            }
        } catch (e: IOException) {
            return false
        }
    }

    /**
     * Reads frames until a DATA or HEADERS frame with END_STREAM (RFC 9113
     * §6.1, §6.2) arrives on [streamId], and returns the stream's first
     * header block fragment and its DATA payloads; null when an RST_STREAM
     * for it arrives, the connection ends, or no frame arrives for
     * [timeoutMs]. Frames are assumed unpadded.
     */
    fun awaitResponse(streamId: Int, timeoutMs: Int): Http2Response? {
        socket.soTimeout = timeoutMs
        val input = socket.getInputStream().source().buffer()
        var headerBlock: ByteArray? = null
        val data = Buffer()
        try {
            while (true) {
                val size = (input.readByte().toInt() and 0xff shl 16) or (input.readShort().toInt() and 0xffff)
                val frameType = input.readByte().toInt()
                val flags = input.readByte().toInt()
                val frameStream = input.readInt() and 0x7fffffff
                val payload = input.readByteArray(size.toLong())
                if (frameStream != streamId) continue
                when (frameType) {
                    0x0 -> data.write(payload)
                    0x1 -> if (headerBlock == null) headerBlock = payload
                    0x3 -> return null
                }
                if ((frameType == 0x0 || frameType == 0x1) && flags and 0x1 != 0) {
                    return Http2Response(headerBlock ?: ByteArray(0), data.readByteArray())
                }
            }
        } catch (e: IOException) {
            return null
        }
    }

    /** A HEADERS frame with END_HEADERS, and END_STREAM when [endStream]. */
    private fun headers(streamId: Int, fields: List<Pair<String, String>>, endStream: Boolean) {
        val block = Buffer()
        for ((name, value) in fields) {
            // Literal field without indexing, new name, no Huffman coding
            // (RFC 7541 §6.2.2, §5.2); every string here is under 127 bytes.
            block.writeByte(0).writeByte(name.length).writeUtf8(name).writeByte(value.length).writeUtf8(value)
        }
        frame(type = 0x1, flags = if (endStream) 0x5 else 0x4, streamId = streamId, payload = block)
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

/**
 * A response read by [RawHttp2Connection.awaitResponse]: the HPACK-encoded
 * (RFC 7541) block of its first HEADERS frame, and its DATA payloads.
 */
internal class Http2Response(val headerBlock: ByteArray, val data: ByteArray)
