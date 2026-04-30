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

dependencies {
    api(project(":server"))
    api(libs.ktor.server.core)
    api(libs.ktor.server.netty)
    implementation(libs.kotlin.coroutines.core)

    testImplementation(libs.assertj)
    testImplementation(libs.junit)
    testImplementation(libs.kotlin.coroutines.test)
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
