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

package com.connectrpc.extensions

import com.connectrpc.ConnectErrorDetail
import com.connectrpc.ConnectException
import com.google.protobuf.Message
import okio.ByteString.Companion.toByteString

/**
 * Returns a copy of this exception with [messages] added to its error details,
 * after any it already has. Servers send them to the client, which reads them
 * back with [ConnectException.unpackedDetails].
 *
 * Each detail's type is the message's fully-qualified name, which protocol.md
 * ("Error", error details) requires, as connect-go
 * `connectproto.NewErrorDetail` does (`connectproto/errordetail.go:29`).
 *
 * @param messages The messages to add, in order.
 * @return The new exception.
 */
fun ConnectException.withDetails(vararg messages: Message): ConnectException = withErrorDetails(
    JavaErrorParser,
    details + messages.map { ConnectErrorDetail(it.descriptorForType.fullName, it.toByteArray().toByteString()) },
)
