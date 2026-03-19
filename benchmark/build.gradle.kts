plugins {
    java
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("me.champeau.jmh") version "0.7.3"
}

dependencies {
    jmh(project(":bpdbi-core"))
    jmh(project(":bpdbi-pg-client"))
    jmh(project(":bpdbi-kotlin"))
    jmh(project(":bpdbi-record-mapper"))
    jmh(project(":bpdbi-javabean-mapper"))
    jmh(project(":bpdbi-pool"))

    jmh("org.postgresql:postgresql:42.7.5")
    jmh("com.zaxxer:HikariCP:6.2.1")
    jmh("org.jdbi:jdbi3-core:3.47.0")
    jmh("org.jdbi:jdbi3-kotlin:3.47.0")
    jmh(kotlin("reflect"))
    jmh("org.hibernate.orm:hibernate-core:6.6.5.Final")

    jmh("io.vertx:vertx-pg-client:5.0.8")
    // Suppress "Cannot find annotation method" warnings from Vert.x codegen and Jdbi annotations
    jmh("io.vertx:vertx-codegen-api:5.0.8")
    jmh("io.vertx:vertx-codegen-json:5.0.8")
    jmh("com.google.errorprone:error_prone_annotations:2.36.0")

    jmh("org.openjdk.jmh:jmh-core:1.37")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.37")

    jmh("org.testcontainers:postgresql:1.20.4")
    jmh("org.testcontainers:toxiproxy:1.20.4")

    jmh("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

    jmh("org.slf4j:slf4j-nop:2.0.16")
}

kotlin {
    compilerOptions {
        optIn.addAll(
            "kotlinx.serialization.ExperimentalSerializationApi"
        )
    }
}

// Not a publishable artifact
tasks.jar { enabled = false }

// Relax -Werror for JMH-generated code
tasks.withType<JavaCompile> {
    options.compilerArgs.remove("-Werror")
}

jmh {
    fork = 1
    warmupIterations = 3
    warmupForks = 0
    iterations = 5
    timeOnIteration = "5s"
    warmup = "3s"
    benchmarkMode = listOf("thrpt")
    resultFormat = "JSON"
    resultsFile = project.layout.buildDirectory.file("reports/jmh/results.json")

    val jmhIncludes = project.findProperty("jmhIncludes") as String?
    if (jmhIncludes != null) includes = listOf(jmhIncludes)
    val jmhFork = project.findProperty("jmhFork") as String?
    if (jmhFork != null) fork = jmhFork.toInt()
    val jmhWarmupIterations = project.findProperty("jmhWarmupIterations") as String?
    if (jmhWarmupIterations != null) warmupIterations = jmhWarmupIterations.toInt()
    val jmhIterations = project.findProperty("jmhIterations") as String?
    if (jmhIterations != null) iterations = jmhIterations.toInt()
    val jmhResultFormat = project.findProperty("jmhResultFormat") as String?
    if (jmhResultFormat != null) resultFormat = jmhResultFormat

    // Forward latency setting to forked JVMs
    val latency = project.findProperty("benchLatencyMs") as String? ?: "0"
    jvmArgs = listOf(
        "-DbenchLatencyMs=$latency",
        "-Djava.util.logging.config.file=${project.file("src/jmh/resources/logging.properties")}"
    )
}
