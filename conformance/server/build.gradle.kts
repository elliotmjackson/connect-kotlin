plugins {
    kotlin("jvm")
    application
}

kotlin {
    compilerOptions.allWarningsAsErrors.set(true)
}

application {
    mainClass.set("com.connectrpc.conformance.server.MainKt")
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
    implementation(project(":library"))
    implementation(project(":extensions:google-java"))
    implementation(libs.kotlin.coroutines.core)
    implementation(libs.protobuf.java)
    implementation(libs.protobuf.kotlin)
    implementation(libs.okio.core)
    implementation(libs.okhttp.tls)
}
