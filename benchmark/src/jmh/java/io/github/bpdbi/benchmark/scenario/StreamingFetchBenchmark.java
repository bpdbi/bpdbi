package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
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
 * Benchmarks streaming/cursor-based reading of large result sets. Measures throughput of iterating
 * over all users (1000 rows) using different consumption patterns.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class StreamingFetchBenchmark {

  private static final String SQL =
      "SELECT id, username, email, full_name, bio, active, created_at FROM users";
  private static final String JDBC_SQL =
      "SELECT id, username, email, full_name, bio, active, created_at FROM users";

  // --- bpdbi: buffered query (materializes all rows) ---

  @Benchmark
  public void bpdbi_buffered(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var rs = conn.query(SQL);
      for (var row : rs) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
        bh.consume(row.getString("full_name"));
        bh.consume(row.getString("bio"));
        bh.consume(row.getBoolean("active"));
        bh.consume(row.getLocalDateTime("created_at"));
      }
    }
  }

  // --- bpdbi: streaming (constant memory, no materialization) ---

  @Benchmark
  public void bpdbi_stream(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var rows = conn.stream(SQL)) {
        for (var row : rows) {
          bh.consume(row.getInteger("id"));
          bh.consume(row.getString("username"));
          bh.consume(row.getString("email"));
          bh.consume(row.getString("full_name"));
          bh.consume(row.getString("bio"));
          bh.consume(row.getBoolean("active"));
          bh.consume(row.getLocalDateTime("created_at"));
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
            bh.consume(row.getString("username"));
            bh.consume(row.getString("email"));
            bh.consume(row.getString("full_name"));
            bh.consume(row.getString("bio"));
            bh.consume(row.getBoolean("active"));
            bh.consume(row.getLocalDateTime("created_at"));
          });
    }
  }

  // --- JDBC: ResultSet iteration (fetch all at once) ---

  @Benchmark
  public void jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          bh.consume(rs.getInt("id"));
          bh.consume(rs.getString("username"));
          bh.consume(rs.getString("email"));
          bh.consume(rs.getString("full_name"));
          bh.consume(rs.getString("bio"));
          bh.consume(rs.getBoolean("active"));
          bh.consume(rs.getTimestamp("created_at"));
        }
      }
    }
  }

  // --- JDBC: server-side cursor via setFetchSize ---

  @Benchmark
  public void jdbc_fetchSize(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
        ps.setFetchSize(100);
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            bh.consume(rs.getInt("id"));
            bh.consume(rs.getString("username"));
            bh.consume(rs.getString("email"));
            bh.consume(rs.getString("full_name"));
            bh.consume(rs.getString("bio"));
            bh.consume(rs.getBoolean("active"));
            bh.consume(rs.getTimestamp("created_at"));
          }
        }
      }
      jdbcConn.commit();
    }
  }

  // --- Jdbi: streaming via ResultIterator ---

  @Benchmark
  public void jdbc_jdbi_stream(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useHandle(
            h -> {
              try (var iter =
                  h.createQuery(JDBC_SQL)
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
                      .iterator()) {
                while (iter.hasNext()) {
                  bh.consume(iter.next());
                }
              }
            });
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
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
      bh.consume(row.getString("full_name"));
      bh.consume(row.getString("bio"));
      bh.consume(row.getBoolean("active"));
      bh.consume(row.getLocalDateTime("created_at"));
    }
  }
}
