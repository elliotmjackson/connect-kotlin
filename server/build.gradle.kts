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
    api(project(":library"))
    api(libs.kotlin.coroutines.core)
    api(libs.okio.core)

    testImplementation(libs.assertj)
    testImplementation(libs.junit)
    testImplementation(libs.kotlin.coroutines.test)
    // Independent decoders for the hand-written error JSON and google.rpc.Status encoders.
    testImplementation(libs.moshiKotlin)
    testImplementation(libs.protobuf.java)
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
            publication.artifactId = "connect-kotlin-server"
        }
}
