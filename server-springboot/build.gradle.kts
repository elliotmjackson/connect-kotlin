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
    api(libs.spring.boot.autoconfigure)
    // Provided by the application's servlet container (Tomcat, Jetty); see
    // https://docs.spring.io/spring-boot/4.1/reference/features/developing-auto-configuration.html#features.developing-auto-configuration.custom-starter.autoconfigure-module
    compileOnly(libs.jakarta.servlet.api)
    // Context carried into handlers when present (RequestThreadContext); versions from Boot's BOM.
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly(libs.spring.web)
    compileOnly(libs.spring.security.core)
    compileOnly(libs.spring.webmvc)
    compileOnly(libs.spring.boot.tomcat)
    compileOnly(libs.slf4j.api)
    implementation(libs.kotlin.coroutines.core)

    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.assertj)
    testImplementation(libs.junit)
    testImplementation(libs.kotlin.coroutines.test)
    testImplementation(libs.okhttp.core)
    testImplementation(project(":okhttp"))
    testImplementation(libs.spring.boot.starter.webmvc)
    testImplementation(libs.spring.boot.starter.micrometer.metrics)
    testImplementation(libs.spring.security.web)
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
            publication.artifactId = "connect-kotlin-server-springboot"
        }
}
