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

import com.connectrpc.protocgen.connect.internal.parseGeneratorParameter
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class ParseGeneratorParameterTest {
    @Test
    fun splitsCommaSeparatedPairsAndNormalizesKeys() {
        val cases = mapOf(
            "" to emptyMap(),
            "a=1" to mapOf("a" to "1"),
            "a=1,b=2" to mapOf("a" to "1", "b" to "2"),
            // A key without `=` has an empty value.
            "flag" to mapOf("flag" to ""),
            // Empty segments are skipped.
            ",a=1,,b=2," to mapOf("a" to "1", "b" to "2"),
            // Only the first `=` separates key from value.
            "a=x=y" to mapOf("a" to "x=y"),
            "a=" to mapOf("a" to ""),
            "a=1,a=2" to mapOf("a" to "2"),
            // snake_case and a capitalized first letter map to lowerCamelCase.
            "generate_server_handler=true" to mapOf("generateServerHandler" to "true"),
            "GenerateClient=false" to mapOf("generateClient" to "false"),
            "generateClient=false" to mapOf("generateClient" to "false"),
            // A digit capitalizes the letter that follows it.
            "v2beta=1" to mapOf("v2Beta" to "1"),
        )
        assertThat(cases.mapValues { parseGeneratorParameter(it.key) }).containsExactlyInAnyOrderEntriesOf(cases)
    }
}
