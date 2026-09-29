import com.vanniktech.maven.publish.JavadocJar.Dokka
import com.vanniktech.maven.publish.KotlinJvm

plugins {
    kotlin("jvm")
    id("org.jetbrains.dokka")
    id("com.vanniktech.maven.publish.base")
}

kotlin {
    compilerOptions.allWarningsAsErrors.set(true)
}

tasks.test {
    // Gradle runs tests with assertions enabled, which turns on kotlinx-coroutines'
    // debug mode (1.11.0 `jvm/src/Debug.kt:19-24, 63-71`); applications run without
    // it, and it changes how cancellation exceptions are copied (`jvm/src/Exceptions.kt:54-63`).
    systemProperty("kotlinx.coroutines.debug", "off")
}

dependencies {
    api(project(":server"))
    api(libs.ktor.server.core)
    // gRPC trailers are written through Netty's HTTP/2 pipeline when the app runs on Netty.
    compileOnly(libs.ktor.server.netty)
    implementation(libs.kotlin.coroutines.core)

    testImplementation(libs.ktor.server.cors)
    testImplementation(libs.ktor.server.netty)

    testImplementation(libs.assertj)
    testImplementation(libs.junit)
    testImplementation(libs.kotlin.coroutines.test)
    testImplementation(libs.okhttp.core)
    testImplementation(libs.okhttp.tls)
}

mavenPublishing {
    configure(
        KotlinJvm(javadocJar = Dokka("dokkaGeneratePublicationHtml")),
    )
}

extensions.getByType<PublishingExtension>().apply {
    publications
        .filterIsInstance<MavenPublication>()
        .forEach { publication ->
            publication.artifactId = "connect-kotlin-server-ktor"
        }
}
