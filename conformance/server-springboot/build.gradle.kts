plugins {
    kotlin("jvm")
    application
}

kotlin {
    compilerOptions.allWarningsAsErrors.set(true)
}

application {
    mainClass.set("com.connectrpc.conformance.server.springboot.MainKt")
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
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.boot.autoconfigure)
}
