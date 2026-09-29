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

package com.connectrpc.server.internal

import com.connectrpc.Code
import com.connectrpc.ConnectException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Parses `Connect-Timeout-Ms`: a positive integer of at most 10 ASCII digits
 * (protocol.md "Timeout-Milliseconds"). Null when absent or empty (connect-go
 * treats an empty value as no timeout, `connecthttp/protocol_connect.go:119-122`);
 * anything else is `invalid_argument`, as in connect-go (`:123-133`).
 */
internal fun parseConnectTimeout(header: String?): Duration? {
    val value = header?.trim(' ', '\t')?.ifEmpty { null } ?: return null
    if (value.length > 10 || !value.all { it in '0'..'9' }) {
        throw ConnectException(Code.INVALID_ARGUMENT, "invalid Connect-Timeout-Ms \"${sanitizeValue(value).take(32)}\": want 1-10 digits")
    }
    val ms = value.toLong()
    if (ms == 0L) throw ConnectException(Code.INVALID_ARGUMENT, "invalid Connect-Timeout-Ms \"0\": want a positive integer")
    return ms.milliseconds
}

/**
 * Parses `grpc-timeout`: a positive integer of at most 8 ASCII digits followed
 * by a unit `H M S m u n` (PROTOCOL-HTTP2.md "Timeout"). Null when
 * absent or empty (connect-go `errNoTimeout`, `connecthttp/protocol_grpc.go:867-870`);
 * anything else is `invalid_argument`, as in connect-go (`:871-889`).
 */
internal fun parseGrpcTimeout(header: String?): Duration? {
    val value = header?.trim(' ', '\t')?.ifEmpty { null } ?: return null
    val digits = value.dropLast(1)
    if (digits.isEmpty() || digits.length > 8 || !digits.all { it in '0'..'9' }) {
        throw ConnectException(Code.INVALID_ARGUMENT, "invalid grpc-timeout \"${sanitizeValue(value).take(32)}\": want 1-8 digits and a unit")
    }
    val amount = digits.toLong()
    if (amount == 0L) throw ConnectException(Code.INVALID_ARGUMENT, "invalid grpc-timeout \"$value\": want a positive integer")
    return when (value.last()) {
        'H' -> amount.hours
        'M' -> amount.minutes
        'S' -> amount.seconds
        'm' -> amount.milliseconds
        'u' -> amount.microseconds
        'n' -> amount.nanoseconds
        else -> throw ConnectException(Code.INVALID_ARGUMENT, "invalid grpc-timeout \"${sanitizeValue(value)}\": unknown unit")
    }
}
