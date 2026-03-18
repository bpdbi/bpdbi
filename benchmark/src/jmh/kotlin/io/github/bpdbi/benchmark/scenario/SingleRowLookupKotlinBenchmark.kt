package io.github.bpdbi.benchmark.scenario

import io.github.bpdbi.benchmark.infra.DatabaseState
import io.github.bpdbi.benchmark.model.UserKotlin
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
open class SingleRowLookupKotlinBenchmark {

    private val sql =
        "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = \$1"

    @Param("1", "50", "500")
    var userId: Int = 0

    private val jdbcSql =
        "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = ?"

    @Benchmark
    fun bpdbi_kotlin(db: DatabaseState, bh: Blackhole) {
        db.bpdbiPool().acquire().use { conn ->
            val user = conn.queryOneAs<UserKotlin>(sql, userId)
            bh.consume(user)
        }
    }

    @Benchmark
    fun jdbc_jdbi_kotlin(db: DatabaseState, bh: Blackhole) {
        val user = db.jdbi().withHandle<UserKotlin?, RuntimeException> { h ->
            h.createQuery(jdbcSql)
                .bind(0, userId)
                .mapTo<UserKotlin>()
                .findFirst()
                .orElse(null)
        }
        bh.consume(user)
    }
}
