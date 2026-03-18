package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
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
 * Benchmarks pipelined lookups: enqueue N single-row SELECTs and flush once. This is where
 * pipelining provides the most benefit over sequential execution, especially with network latency.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class PipelinedLookupBenchmark {

  private static final int BATCH_SIZE = 20;
  private static final String SQL = "SELECT id, username, email FROM users WHERE id = $1";
  private static final String JDBC_SQL = "SELECT id, username, email FROM users WHERE id = ?";

  // --- bpdbi: pipelined (enqueue + flush) ---

  @Benchmark
  public void bpdbi_pipelined(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      for (int i = 1; i <= BATCH_SIZE; i++) {
        conn.enqueue(SQL, i);
      }
      var results = conn.flush();
      for (var rs : results) {
        var row = rs.first();
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
      }
    }
  }

  // --- bpdbi: sequential (query in loop) ---

  @Benchmark
  public void bpdbi_sequential(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      for (int i = 1; i <= BATCH_SIZE; i++) {
        var row = conn.query(SQL, i).first();
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
      }
    }
  }

  // --- JDBC: sequential (one query at a time) ---

  @Benchmark
  public void jdbc_sequential(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      for (int i = 1; i <= BATCH_SIZE; i++) {
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

  // --- Vert.x: sequential via pool ---

  @Benchmark
  public void vertx_sequential(DatabaseState db, Blackhole bh) {
    for (int i = 1; i <= BATCH_SIZE; i++) {
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

  // --- Vert.x: batch (pipelined) ---

  @Benchmark
  public void vertx_batch(DatabaseState db, Blackhole bh) {
    var tuples = new ArrayList<Tuple>(BATCH_SIZE);
    for (int i = 1; i <= BATCH_SIZE; i++) {
      tuples.add(Tuple.of(i));
    }
    var result =
        db.vertxPool()
            .preparedQuery(SQL)
            .executeBatch(tuples)
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    while (result != null) {
      for (var row : result) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
      }
      result = result.next();
    }
  }
}
