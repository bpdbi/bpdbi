package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.vertx.sqlclient.Tuple;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class PreparedStatementReuseBenchmark {

  private static final int ITERATIONS = 100;
  private static final String SQL = "SELECT id, username, email FROM users WHERE id = $1";
  private static final String JDBC_SQL = "SELECT id, username, email FROM users WHERE id = ?";

  // --- bpdbi ---

  @Benchmark
  public void bpdbi_prepared(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var ps = conn.prepare(SQL)) {
        for (int i = 1; i <= ITERATIONS; i++) {
          var row = ps.query(i).first();
          bh.consume(row.getInteger("id"));
          bh.consume(row.getString("username"));
          bh.consume(row.getString("email"));
        }
      }
    }
  }

  // --- JDBC: prepareStatement() once, loop ---

  @Benchmark
  public void jdbc_prepared(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      for (int i = 1; i <= ITERATIONS; i++) {
        ps.setInt(1, i);
        try (var rs = ps.executeQuery()) {
          if (rs.next()) {
            bh.consume(rs.getInt("id"));
            bh.consume(rs.getString("username"));
            bh.consume(rs.getString("email"));
          }
        }
      }
    }
  }

  // --- Jdbi: query loop ---

  @Benchmark
  public void jdbc_jdbi_loop(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useHandle(
            h -> {
              for (int i = 1; i <= ITERATIONS; i++) {
                var row =
                    h.createQuery(JDBC_SQL)
                        .bind(0, i)
                        .map(
                            (rs, ctx) ->
                                new Object[] {
                                  rs.getInt("id"), rs.getString("username"), rs.getString("email")
                                })
                        .findFirst()
                        .orElse(null);
                bh.consume(row);
              }
            });
  }

  // --- Vert.x ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_loop(DatabaseState db, Blackhole bh) {
    for (int i = 1; i <= ITERATIONS; i++) {
      var rows =
          db.vertxPool()
              .preparedQuery(SQL)
              .execute(Tuple.of(i))
              .toCompletionStage()
              .toCompletableFuture()
              .join();
      for (var row : rows) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
      }
    }
  }
}
