plugins {
    kotlin("jvm")
    application
}

kotlin {
    compilerOptions.allWarningsAsErrors.set(true)
}

application {
    mainClass.set("com.connectrpc.conformance.server.MainKt")
    // The JVM's default unified-logging output is `-Xlog:all=warning:stdout`
    // (java(1) "Default Configuration"); stdout carries the handshake to the
    // runner, so JVM warnings go to stderr instead.
    applicationDefaultJvmArgs = listOf("-Xlog:disable", "-Xlog:all=warning:stderr")
}

tasks {
    compileKotlin {
        compilerOptions {
            // Generated Kotlin code for protobuf uses OptIn annotation
            freeCompilerArgs.add("-opt-in=kotlin.RequiresOptIn")
        }
    }
    jar {
        manifest {
            attributes(mapOf("Main-Class" to application.mainClass.get()))
        }
        from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }) {
            exclude("META-INF/**/*")
        }
    }
}

sourceSets {
    main {
        java {
            srcDir("build/generated/sources/bufgen")
        }
    }
}

dependencies {
    implementation(project(":server"))
    implementation(project(":server-ktor"))
    implementation(libs.ktor.server.netty)
    implementation(project(":library"))
    implementation(project(":extensions:google-java"))
    implementation(libs.kotlin.coroutines.core)
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.kotlin)
    implementation(libs.okio.core)
    implementation(libs.okhttp.tls)
}
