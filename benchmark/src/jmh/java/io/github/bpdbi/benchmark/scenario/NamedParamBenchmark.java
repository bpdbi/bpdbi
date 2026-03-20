package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks named parameter parsing overhead. Compares bpdbi positional ($1) vs named (:name)
 * parameters, and Jdbi named parameters for reference.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class NamedParamBenchmark {

  private static final String POSITIONAL_SQL =
      "SELECT id, username, email, full_name, bio, active, created_at"
          + " FROM users WHERE id = $1 AND active = $2";
  private static final String NAMED_SQL =
      "SELECT id, username, email, full_name, bio, active, created_at"
          + " FROM users WHERE id = :id AND active = :active";
  private static final String JDBC_NAMED_SQL =
      "SELECT id, username, email, full_name, bio, active, created_at"
          + " FROM users WHERE id = :id AND active = :active";

  private static final Map<String, Object> NAMED_PARAMS = Map.of("id", 1, "active", true);

  // --- bpdbi: positional parameters (baseline) ---

  @Benchmark
  public void bpdbi_positional(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var row = conn.query(POSITIONAL_SQL, 1, true).first();
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
      bh.consume(row.getString("full_name"));
      bh.consume(row.getString("bio"));
      bh.consume(row.getBoolean("active"));
      bh.consume(row.getLocalDateTime("created_at"));
    }
  }

  // --- bpdbi: named parameters (exercises NamedParamParser) ---

  @Benchmark
  public void bpdbi_named(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var row = conn.query(NAMED_SQL, NAMED_PARAMS).first();
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
      bh.consume(row.getString("full_name"));
      bh.consume(row.getString("bio"));
      bh.consume(row.getBoolean("active"));
      bh.consume(row.getLocalDateTime("created_at"));
    }
  }

  // --- Jdbi: named parameters ---

  @Benchmark
  public void jdbc_jdbi_named(DatabaseState db, Blackhole bh) {
    var user =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(JDBC_NAMED_SQL)
                        .bind("id", 1)
                        .bind("active", true)
                        .map(
                            (rs, ctx) ->
                                new Object[] {
                                  rs.getInt("id"),
                                  rs.getString("username"),
                                  rs.getString("email"),
                                  rs.getString("full_name"),
                                  rs.getString("bio"),
                                  rs.getBoolean("active"),
                                  rs.getTimestamp("created_at")
                                })
                        .findFirst()
                        .orElse(null));
    bh.consume(user);
  }
}
