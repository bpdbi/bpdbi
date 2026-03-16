plugins {
    java
}

group = "io.djb"
version = "0.1.0-SNAPSHOT"

subprojects {
    apply(plugin = "java")

    group = rootProject.group
    version = rootProject.version

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
    }
}
