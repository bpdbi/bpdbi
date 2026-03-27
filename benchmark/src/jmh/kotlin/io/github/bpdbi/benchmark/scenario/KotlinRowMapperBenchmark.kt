package io.github.bpdbi.benchmark.scenario

import io.github.bpdbi.benchmark.infra.DatabaseState
import io.github.bpdbi.benchmark.model.ProductKotlin
import io.github.bpdbi.benchmark.model.UserKotlin
import io.github.bpdbi.kotlin.queryAs
import io.github.bpdbi.kotlin.queryOneAs
import org.jdbi.v3.core.kotlin.mapTo
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
open class KotlinRowMapperBenchmark {

    @Param("1", "50", "500")
    var userId: Int = 0

    @Param("0", "1", "2")
    var categoryIdx: Int = 0

    // --- Scenario 1: Single row lookup ---

    @Benchmark
    fun bpdbi_kotlin_single(db: DatabaseState, bh: Blackhole) {
        val sql = "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = \$1"
        db.bpdbiPool().acquire().use { conn ->
            val user = conn.queryOneAs<UserKotlin>(sql, userId)
            bh.consume(user)
        }
    }

    // --- Scenario 2: Multi-row fetch ---

    @Benchmark
    fun bpdbi_kotlin_multi(db: DatabaseState, bh: Blackhole) {
        val sql = "SELECT id, name, description, price::float8, category, stock FROM products WHERE category = \$1"
        val category = DatabaseState.categoryForParam(categoryIdx)
        db.bpdbiPool().acquire().use { conn ->
            val products = conn.queryAs<ProductKotlin>(sql, category)
            bh.consume(products)
        }
    }

    // --- JDBI Kotlin mapper (reflection-based) ---

    @Benchmark
    fun jdbc_jdbi_kotlin_single(db: DatabaseState, bh: Blackhole) {
        val sql = "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = ?"
        val user = db.jdbi().withHandle<UserKotlin?, RuntimeException> { h ->
            h.createQuery(sql)
                .bind(0, userId)
                .mapTo<UserKotlin>()
                .findFirst()
                .orElse(null)
        }
        bh.consume(user)
    }

    @Benchmark
    fun jdbc_jdbi_kotlin_multi(db: DatabaseState, bh: Blackhole) {
        val sql = "SELECT id, name, description, price, category, stock FROM products WHERE category = ?"
        val category = DatabaseState.categoryForParam(categoryIdx)
        val products = db.jdbi().withHandle<List<ProductKotlin>, RuntimeException> { h ->
            h.createQuery(sql)
                .bind(0, category)
                .mapTo<ProductKotlin>()
                .list()
        }
        bh.consume(products)
    }
}
