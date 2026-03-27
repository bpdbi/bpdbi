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

// ---------------------------------------------------------------------------
// dependencySizes — print dependency sizes per benchmark contender (Markdown output)
// Usage: ./gradlew :benchmark:dependencySizes
//
// Each contender is defined by its root dependencies only; Gradle resolves
// the full transitive closure automatically via detached configurations.
// ---------------------------------------------------------------------------
tasks.register("dependencySizes") {
  description = "Print dependency jar sizes per benchmark contender"
  group = "reporting"
  doLast {
    fun humanSize(bytes: Long): String = "${(bytes + 512) / 1_024} KB"

    // Jars excluded from all contenders (shared Kotlin runtime, assumed present)
    val excludedPrefixes = listOf("kotlin-stdlib", "annotations-")

    // Resolve a detached configuration and return sorted (name, size) pairs
    fun resolve(deps: Array<Dependency>): List<Pair<String, Long>> {
      val detached = configurations.detachedConfiguration(*deps)
      detached.isTransitive = true
      return detached.resolve()
        .filter { it.isFile && it.name.endsWith(".jar") }
        .filterNot { jar -> excludedPrefixes.any { jar.name.startsWith(it) } }
        .map { it.name to it.length() }
        .sortedBy { it.first }
    }

    // Helper to create dependencies
    fun dep(notation: Any): Dependency = dependencies.create(notation)

    // Root dependencies per contender — Gradle resolves full transitive closure
    val bpdbiRaw = arrayOf(
      dep(project(":bpdbi-core")),
      dep(project(":bpdbi-pg-client")),
      dep(project(":bpdbi-pool")),
    )
    val jdbcRaw = arrayOf(
      dep(libs.postgresql.jdbc.get()),
      dep(libs.hikaricp.get()),
    )

    // Contender definitions: name → root deps (transitives resolved automatically)
    val contenders: List<Pair<String, Array<Dependency>>> = listOf(
      "bpdbi_raw"         to bpdbiRaw,
      "bpdbi_record"      to bpdbiRaw + dep(project(":bpdbi-record-mapper")),
      "bpdbi_bean"        to bpdbiRaw + dep(project(":bpdbi-javabean-mapper")),
      "bpdbi_kotlin"      to bpdbiRaw + dep(project(":bpdbi-kotlin")),
      "jdbc_raw"          to jdbcRaw,
      "jdbc_jdbi"         to jdbcRaw + dep(libs.jdbi.core.get()),
      "jdbc_jdbi_kotlin"  to jdbcRaw + dep(libs.jdbi.core.get()) + dep(libs.jdbi.kotlin.get()),
      "jdbc_hibernate"    to jdbcRaw + dep(libs.hibernate.core.get()),
      "jdbc_jooq"         to jdbcRaw + dep(libs.jooq.get()),
      "jdbc_sql2o"        to jdbcRaw + dep(libs.sql2o.get()),
      "jdbc_spring"       to jdbcRaw + dep(libs.spring.jdbc.get()),
      "vertx_raw"         to arrayOf(dep(libs.vertx.pg.client.get())),
    )

    // Resolve each contender: name → (total, jar list)
    val results = contenders.map { (name, deps) ->
      val jars = resolve(deps)
      Triple(name, jars.sumOf { it.second }, jars)
    }

    // --- Markdown output ---

    println("Benchmark contender dependency size analysis")
    println("============================================")
    println()
    println("Each contender's full dependency set (beyond benchmark/test infra")
    println("like JMH, Testcontainers, Kotlin stdlib/reflect).")
    println()
    println("NOTE: the JDBC API (~300 KB) is part of the JDK standard library and not listed,")
    println("but is used by all contenders except the bpdbi_* contenders and Vert.x.")
    println()
    println("NOTE: the scram/stringprep jars (~113 KB) in bpdbi and Vert.x contenders provide")
    println("SCRAM-SHA-256 authentication (default since Postgres 10). pgjdbc includes the same")
    println("libraries but shades it into its own jar.")
    println()
    println("NOTE: kotlin-stdlib (~1.6 MB) is excluded from all contenders. It is shared runtime")
    println("infrastructure for any Kotlin project, similar to the JDK standard library for Java.")

    for ((name, total, jars) in results) {
      println()
      println("## $name — ${humanSize(total)}")
      println()
      println("```")
      for ((jar, size) in jars) println("    %8s  %s".format(humanSize(size), jar))
      println("```")
    }

    // Summary table sorted ascending
    println()
    println("## Summary (sorted ascending)")
    println()
    println("```")
    println("%-19s %26s".format("Benchmark contender", "Total size of dependencies"))
    println("%-19s %26s".format("-".repeat(19), "-".repeat(26)))
    for ((name, total, _) in results.sortedBy { it.second }) {
      println("%-19s %26s".format(name, humanSize(total)))
    }
    println("```")
  }
}
