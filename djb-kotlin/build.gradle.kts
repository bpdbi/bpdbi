plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.20"
    `java-library`
}

dependencies {
    api(project(":djb-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

    testImplementation(project(":djb-pg-client"))
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        optIn.addAll(
            "kotlin.uuid.ExperimentalUuidApi",
            "kotlin.time.ExperimentalTime",
            "kotlinx.serialization.ExperimentalSerializationApi"
        )
    }
}

tasks.test {
    useJUnitPlatform()
}
