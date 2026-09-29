plugins {
    kotlin("jvm")
    application
}

kotlin {
    compilerOptions.allWarningsAsErrors.set(true)
}

application {
    mainClass.set("com.connectrpc.conformance.server.springboot.MainKt")
    // connectconformance v1.0.5 gives each server 10 s to answer its start request
    // (server_runner.go serverResponseTimeout) and starts up to 4 at once (--max-servers).
    // C1-only compilation roughly halves the CPU time the JVM spends reaching the handshake
    // (C2 compiler threads dominate it), so concurrent starts on 2 CPUs finish in time;
    // SerialGC trims a further few percent over G1. The JVM's default unified-logging
    // output is `-Xlog:all=warning:stdout` (java(1) "Default Configuration"); stdout
    // carries the handshake to the runner, so JVM warnings go to stderr instead.
    applicationDefaultJvmArgs = listOf(
        "-XX:TieredStopAtLevel=1",
        "-XX:+UseSerialGC",
        "-Xlog:disable",
        "-Xlog:all=warning:stderr",
    )
}

tasks {
    compileKotlin {
        compilerOptions {
            freeCompilerArgs.add("-opt-in=kotlin.RequiresOptIn")
        }
    }
    jar {
        manifest {
            attributes(mapOf("Main-Class" to application.mainClass.get()))
        }
        from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) }) {
            exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
        }
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
}

dependencies {
    implementation(project(":server"))
    implementation(project(":server-springboot"))
    implementation(project(":conformance:server"))
    implementation(project(":library"))
    implementation(project(":extensions:google-java"))
    implementation(libs.kotlin.coroutines.core)
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.kotlin)
    implementation(libs.okio.core)
    implementation(libs.okhttp.tls)
    // A Connect-only server: without Spring MVC, paths outside the registered
    // services get Tomcat's non-JSON 404, which Connect and gRPC clients map
    // to unimplemented (https://connectrpc.com/docs/protocol#http-to-error-code).
    implementation(libs.spring.boot.starter.tomcat)
}
