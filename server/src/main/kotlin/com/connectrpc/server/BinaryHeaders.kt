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

import java.util.Base64

/**
 * Encodes [value] for a metadata field whose name ends in `-bin`: standard
 * base64 without padding (protocol.md:196-203; PROTOCOL-HTTP2.md:68;
 * connect-go `EncodeBinaryHeader`).
 */
fun encodeBinaryHeader(value: ByteArray): String = Base64.getEncoder().withoutPadding().encodeToString(value)

/**
 * Decodes the values of a `-bin` metadata field, as received in
 * [HandlerContext.requestHeaders]. A field value may join several binary
 * values with `,` (PROTOCOL-HTTP2.md:72-74: implementations must split on "," before
 * decoding); each is base64, padded or unpadded (PROTOCOL-HTTP2.md:68 and
 * protocol.md:196-203: implementations must accept both). Surrounding
 * whitespace is ignored. Throws [IllegalArgumentException] if a part is not
 * base64.
 */
fun decodeBinaryHeaders(value: String): List<ByteArray> = value.split(',').map { Base64.getDecoder().decode(it.trim()) }
