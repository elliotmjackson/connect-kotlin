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

package com.connectrpc.okhttp

import com.connectrpc.Code
import okhttp3.internal.http2.ErrorCode
import okhttp3.internal.http2.StreamResetException
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class CodeFromExceptionTest {
    @Test
    fun serverCancelResetIsCanceled() {
        val code = codeFromException(callCanceled = false, StreamResetException(ErrorCode.CANCEL))
        assertThat(code).isEqualTo(Code.CANCELED)
    }

    @Test
    fun otherServerResetIsUnknown() {
        val code = codeFromException(callCanceled = false, StreamResetException(ErrorCode.INTERNAL_ERROR))
        assertThat(code).isEqualTo(Code.UNKNOWN)
    }
}
