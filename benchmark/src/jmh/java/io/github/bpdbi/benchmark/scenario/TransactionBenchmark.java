package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.vertx.sqlclient.Tuple;
import java.math.BigDecimal;
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
 * Benchmarks transaction overhead: BEGIN/COMMIT round-trips, read-only transactions, pipelined
 * inserts within a transaction, and inTransaction() convenience API.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class TransactionBenchmark {

  private static final String SELECT_SQL = "SELECT id, username, email FROM users WHERE id = $1";
  private static final String JDBC_SELECT_SQL =
      "SELECT id, username, email FROM users WHERE id = ?";
  private static final String INSERT_SQL =
      "INSERT INTO bench_orders (user_id, total, status) VALUES ($1, $2, $3)";
  private static final String JDBC_INSERT_SQL =
      "INSERT INTO bench_orders (user_id, total, status) VALUES (?, ?, ?)";
  private static final String DELETE_SQL = "DELETE FROM bench_orders WHERE user_id = $1";

  // --- bpdbi: read-only transaction (BEGIN + SELECT + COMMIT) ---

  @Benchmark
  public void bpdbi_readOnly(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var tx = conn.begin()) {
        var row = tx.query(SELECT_SQL, 1).first();
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("username"));
        bh.consume(row.getString("email"));
        tx.commit();
      }
    }
  }

  // --- bpdbi: pipelined read-only transaction (BEGIN + SELECT + COMMIT in one flush) ---

  @Benchmark
  public void bpdbi_pipelinedReadOnly(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      conn.enqueue("BEGIN");
      int idx = conn.enqueue(SELECT_SQL, 1);
      conn.enqueue("COMMIT");
      var results = conn.flush();
      var row = results.get(idx).first();
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
    }
  }

  // --- bpdbi: inTransaction (convenience API) ---

  @Benchmark
  public void bpdbi_inTransaction(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var row =
          conn.inTransaction(
              tx -> {
                return tx.query(SELECT_SQL, 1).first();
              });
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
    }
  }

  // --- bpdbi: pipelined writes in transaction ---

  @Benchmark
  public void bpdbi_pipelinedInserts(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var tx = conn.begin()) {
        for (int i = 1; i <= 10; i++) {
          tx.enqueue(INSERT_SQL, i, new BigDecimal("99.99"), "pending");
        }
        bh.consume(tx.flush());
        tx.enqueue(DELETE_SQL, 1);
        tx.enqueue(DELETE_SQL, 2);
        tx.enqueue(DELETE_SQL, 3);
        tx.enqueue(DELETE_SQL, 4);
        tx.enqueue(DELETE_SQL, 5);
        tx.enqueue(DELETE_SQL, 6);
        tx.enqueue(DELETE_SQL, 7);
        tx.enqueue(DELETE_SQL, 8);
        tx.enqueue(DELETE_SQL, 9);
        tx.enqueue(DELETE_SQL, 10);
        bh.consume(tx.flush());
        tx.commit();
      }
    }
  }

  // --- bpdbi: sequential writes in transaction (no pipelining) ---

  @Benchmark
  public void bpdbi_sequentialInserts(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      try (var tx = conn.begin()) {
        for (int i = 1; i <= 10; i++) {
          bh.consume(tx.query(INSERT_SQL, i, new BigDecimal("99.99"), "pending"));
        }
        for (int i = 1; i <= 10; i++) {
          bh.consume(tx.query(DELETE_SQL, i));
        }
        tx.commit();
      }
    }
  }

  // --- JDBC: transaction with autoCommit=false ---

  @Benchmark
  public void jdbc_readOnly(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_SELECT_SQL)) {
        ps.setInt(1, 1);
        try (var rs = ps.executeQuery()) {
          if (rs.next()) {
            bh.consume(rs.getInt("id"));
            bh.consume(rs.getString("username"));
            bh.consume(rs.getString("email"));
          }
        }
      }
      jdbcConn.commit();
    }
  }

  // --- JDBC: sequential inserts in transaction ---

  @Benchmark
  public void jdbc_sequentialInserts(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_INSERT_SQL)) {
        for (int i = 1; i <= 10; i++) {
          ps.setInt(1, i);
          ps.setBigDecimal(2, new BigDecimal("99.99"));
          ps.setString(3, "pending");
          bh.consume(ps.executeUpdate());
        }
      }
      try (var ps = jdbcConn.prepareStatement("DELETE FROM bench_orders WHERE user_id = ?")) {
        for (int i = 1; i <= 10; i++) {
          ps.setInt(1, i);
          bh.consume(ps.executeUpdate());
        }
      }
      jdbcConn.commit();
    }
  }

  // --- JDBC + Jdbi: read-only transaction ---

  @Benchmark
  public void jdbc_jdbi_readOnly(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useTransaction(
            h -> {
              var row =
                  h.createQuery(JDBC_SELECT_SQL)
                      .bind(0, 1)
                      .map(
                          (rs, ctx) -> {
                            bh.consume(rs.getInt("id"));
                            bh.consume(rs.getString("username"));
                            bh.consume(rs.getString("email"));
                            return true;
                          })
                      .findFirst();
              bh.consume(row);
            });
  }

  // --- JDBC + Jdbi: sequential inserts in transaction ---

  @Benchmark
  public void jdbc_jdbi_sequentialInserts(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useTransaction(
            h -> {
              for (int i = 1; i <= 10; i++) {
                bh.consume(
                    h.createUpdate(JDBC_INSERT_SQL)
                        .bind(0, i)
                        .bind(1, new BigDecimal("99.99"))
                        .bind(2, "pending")
                        .execute());
              }
              for (int i = 1; i <= 10; i++) {
                bh.consume(
                    h.createUpdate("DELETE FROM bench_orders WHERE user_id = ?")
                        .bind(0, i)
                        .execute());
              }
            });
  }

  // --- Vert.x: read-only transaction ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_readOnly(DatabaseState db, Blackhole bh) {
    var row =
        db.vertxPool()
            .withTransaction(
                conn ->
                    conn.preparedQuery(SELECT_SQL)
                        .execute(Tuple.of(1))
                        .map(rs -> rs.iterator().next()))
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    bh.consume(row.getInteger("id"));
    bh.consume(row.getString("username"));
    bh.consume(row.getString("email"));
  }
}
