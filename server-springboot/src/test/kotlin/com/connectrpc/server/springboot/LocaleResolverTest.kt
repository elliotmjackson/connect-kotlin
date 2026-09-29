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

import com.connectrpc.MethodSpec
import com.connectrpc.StreamType
import com.connectrpc.server.HandlerContext
import com.connectrpc.server.HandlerRegistry
import com.connectrpc.server.UnaryHandler
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import kotlinx.coroutines.delay
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.springframework.boot.SpringBootConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.i18n.LocaleContext
import org.springframework.context.i18n.LocaleContextHolder
import org.springframework.context.i18n.SimpleTimeZoneAwareLocaleContext
import org.springframework.web.servlet.DispatcherServlet
import org.springframework.web.servlet.LocaleContextResolver
import org.springframework.web.servlet.LocaleResolver
import org.springframework.web.servlet.i18n.AcceptHeaderLocaleResolver
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.TimeUnit

/**
 * Handlers see the locale of the application's `localeResolver` bean, as Spring MVC
 * controllers do, not the request's `Accept-Language` or the JVM default.
 */
class LocaleResolverTest {
    companion object {
        @ClassRule
        @JvmField
        val server = SpringBootServer(TestApp::class.java)

        @ClassRule
        @JvmField
        val plainResolverServer = SpringBootServer(PlainResolverApp::class.java)
    }

    private val client = OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()

    @Test
    fun missingAcceptLanguageGetsResolverDefault() {
        assertThat(call(acceptLanguage = null)).isEqualTo("locale=es-MX;timeZone=America/Mexico_City")
    }

    @Test
    fun supportedAcceptLanguageIsResolved() {
        assertThat(call(acceptLanguage = "en-US")).isEqualTo("locale=en-US;timeZone=America/Mexico_City")
    }

    @Test
    fun plainLocaleResolverIsApplied() {
        assertThat(call(acceptLanguage = null, port = plainResolverServer.port)).isEqualTo("locale=es-MX;timeZone=${TimeZone.getDefault().id}")
    }

    private fun call(acceptLanguage: String?, port: Int = server.port): String {
        val request = Request.Builder()
            .url("http://127.0.0.1:$port/test.v1.TestService/Locale")
            .apply { if (acceptLanguage != null) header("Accept-Language", acceptLanguage) }
            .post(ByteArray(0).toRequestBody("application/proto".toMediaType()))
            .build()
        client.newCall(request).execute().use { response ->
            assertThat(response.code).isEqualTo(200)
            return response.body!!.string()
        }
    }

    /** Resolves like `AcceptHeaderLocaleResolver` and adds a time zone, which only a [LocaleContextResolver] carries. */
    class MexicoLocaleResolver : LocaleContextResolver {
        private val locales = AcceptHeaderLocaleResolver().apply {
            // Neither the JVM default nor a header the tests send.
            setDefaultLocale(Locale.forLanguageTag("es-MX"))
            supportedLocales = listOf(Locale.forLanguageTag("es-MX"), Locale.forLanguageTag("en-US"))
        }

        override fun resolveLocaleContext(request: HttpServletRequest): LocaleContext = SimpleTimeZoneAwareLocaleContext(locales.resolveLocale(request), TimeZone.getTimeZone("America/Mexico_City"))

        override fun setLocaleContext(request: HttpServletRequest, response: HttpServletResponse?, localeContext: LocaleContext?) = throw UnsupportedOperationException()
    }

    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class TestApp {
        @Bean(DispatcherServlet.LOCALE_RESOLVER_BEAN_NAME)
        open fun localeResolver(): LocaleResolver = MexicoLocaleResolver()

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = localeRegistry()
    }

    /** A resolver that is not a [LocaleContextResolver], so it carries no time zone. */
    @SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    open class PlainResolverApp {
        @Bean(DispatcherServlet.LOCALE_RESOLVER_BEAN_NAME)
        open fun localeResolver(): LocaleResolver = AcceptHeaderLocaleResolver().apply { setDefaultLocale(Locale.forLanguageTag("es-MX")) }

        @Bean
        open fun connectRpcRegistry(): HandlerRegistry = localeRegistry()
    }
}

private fun localeRegistry(): HandlerRegistry = HandlerRegistry.builder()
    .codec(TestSerializationStrategy)
    .register(
        object : UnaryHandler<TestMessage, TestMessage> {
            override val methodSpec = MethodSpec("test.v1.TestService/Locale", TestMessage::class, TestMessage::class, StreamType.UNARY)
            override suspend fun handle(request: TestMessage, ctx: HandlerContext): TestMessage {
                // After a suspension, so the value comes from the captured context.
                delay(10)
                return TestMessage(
                    "locale=${LocaleContextHolder.getLocale().toLanguageTag()};timeZone=${LocaleContextHolder.getTimeZone().id}",
                )
            }
        },
    )
    .build()
