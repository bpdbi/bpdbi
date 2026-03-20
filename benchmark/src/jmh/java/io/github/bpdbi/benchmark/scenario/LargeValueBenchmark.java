package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks reading rows with large text values (~10KB per row). Stresses the decode/copy path and
 * highlights the streaming-vs-buffered memory tradeoff with substantial payloads.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class LargeValueBenchmark {

  private static final int ROW_COUNT = 200;
  private static final int TEXT_SIZE = 10_000; // ~10KB per text column
  private static final String SQL = "SELECT id, title, body FROM bench_large_text";
  private static final String JDBC_SQL = "SELECT id, title, body FROM bench_large_text";

  @Setup(Level.Trial)
  public void seedTable(DatabaseState db) {
    var filler = "x".repeat(TEXT_SIZE);
    try (var conn = db.newBpdbiConnection()) {
      conn.query(
          "CREATE TABLE IF NOT EXISTS bench_large_text ("
              + "id SERIAL PRIMARY KEY, title VARCHAR(200) NOT NULL, body TEXT NOT NULL)");
      conn.query("TRUNCATE bench_large_text RESTART IDENTITY");
      for (int i = 1; i <= ROW_COUNT; i++) {
        conn.enqueue(
            "INSERT INTO bench_large_text (title, body) VALUES ($1, $2)", "Title " + i, filler);
        if (i % 50 == 0) conn.flush();
      }
      conn.flush();
    }
  }

  // --- bpdbi: buffered (materializes all rows) ---

  @Benchmark
  public void bpdbi_buffered(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var rs = conn.query(SQL);
      for (var row : rs) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("title"));
        bh.consume(row.getString("body"));
      }
    }
  }

  // --- bpdbi: stream (constant memory) ---

  @Benchmark
  public void bpdbi_stream(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var rows = conn.stream(SQL)) {
        for (var row : rows) {
          bh.consume(row.getInteger("id"));
          bh.consume(row.getString("title"));
          bh.consume(row.getString("body"));
        }
      }
    }
  }

  // --- bpdbi: queryStream callback ---

  @Benchmark
  public void bpdbi_queryStream(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      conn.queryStream(
          SQL,
          row -> {
            bh.consume(row.getInteger("id"));
            bh.consume(row.getString("title"));
            bh.consume(row.getString("body"));
          });
    }
  }

  // --- JDBC: fetch all ---

  @Benchmark
  public void jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          bh.consume(rs.getInt("id"));
          bh.consume(rs.getString("title"));
          bh.consume(rs.getString("body"));
        }
      }
    }
  }

  // --- JDBC: streaming with fetchSize ---

  @Benchmark
  public void jdbc_fetchSize(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
        ps.setFetchSize(50);
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            bh.consume(rs.getInt("id"));
            bh.consume(rs.getString("title"));
            bh.consume(rs.getString("body"));
          }
        }
      }
      jdbcConn.commit();
    }
  }

  // --- Vert.x: collect all rows ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_raw(DatabaseState db, Blackhole bh) {
    var rows =
        db.vertxPool()
            .preparedQuery(SQL)
            .execute()
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    for (var row : rows) {
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("title"));
      bh.consume(row.getString("body"));
    }
  }
}
