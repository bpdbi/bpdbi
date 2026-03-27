plugins {
  java
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.serialization)
  alias(libs.plugins.jmh)
}

dependencies {
  jmh(project(":bpdbi-core"))
  jmh(libs.jspecify)
  jmh(project(":bpdbi-pg-client"))
  jmh(project(":bpdbi-kotlin"))
  jmh(project(":bpdbi-record-mapper"))
  jmh(project(":bpdbi-javabean-mapper"))
  jmh(project(":bpdbi-pool"))

  jmh(libs.postgresql.jdbc)
  jmh(libs.hikaricp)
  jmh(libs.jdbi.core)
  jmh(libs.jdbi.kotlin)
  jmh(kotlin("reflect"))
  jmh(libs.hibernate.core)
  jmh(libs.jooq)
  jmh(libs.sql2o)
  jmh(libs.spring.jdbc)

  jmh(libs.vertx.pg.client)
  // Suppress "Cannot find annotation method" warnings from Vert.x codegen and Jdbi annotations
  jmh(libs.vertx.codegen.api)
  jmh(libs.vertx.codegen.json)
  jmh(libs.error.prone.annotations)

  jmh(libs.jmh.core)
  jmh(libs.jmh.generator.annprocess)

  jmh(libs.testcontainers.postgresql)
  jmh(libs.testcontainers.toxiproxy)

  jmh(libs.kotlinx.serialization.json)
  jmh(libs.kotlinx.datetime)

  jmh(libs.slf4j.nop)
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
