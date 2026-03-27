package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.vertx.sqlclient.Tuple;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class BulkInsertBenchmark {

  private static final int BATCH_SIZE = 50;
  private static final String SQL =
      "INSERT INTO bench_orders (user_id, total, status) VALUES ($1, $2, $3)";
  private static final String JDBC_SQL =
      "INSERT INTO bench_orders (user_id, total, status) VALUES (?, ?, ?)";

  private List<Object[]> paramSets;

  @Setup(Level.Trial)
  public void prepareParams() {
    var rng = new Random(99);
    paramSets = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      paramSets.add(
          new Object[] {
            rng.nextInt(1000) + 1,
            BigDecimal.valueOf(rng.nextDouble() * 500 + 10).setScale(2, RoundingMode.HALF_UP),
            "pending"
          });
    }
  }

  @Setup(Level.Iteration)
  public void truncateTable(DatabaseState db) {
    try (var conn = db.newBpdbiConnection()) {
      conn.query("TRUNCATE bench_orders RESTART IDENTITY");
    }
  }

  // --- bpdbi ---

  @Benchmark
  public void bpdbi_executeMany(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.executeMany(SQL, paramSets));
    }
  }

  /** executeMany inside a transaction — matches JDBC's setAutoCommit(false) + batch + commit. */
  @Benchmark
  public void bpdbi_executeManyTx(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      conn.inTransaction(tx -> tx.executeMany(SQL, paramSets));
    }
  }

  // --- JDBC batch ---

  @Benchmark
  public void jdbc_batch(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
        for (var params : paramSets) {
          ps.setInt(1, (int) params[0]);
          ps.setBigDecimal(2, (BigDecimal) params[1]);
          ps.setString(3, (String) params[2]);
          ps.addBatch();
        }
        bh.consume(ps.executeBatch());
      }
      jdbcConn.commit();
    }
  }

  // --- Jdbi batch ---

  @Benchmark
  public void jdbc_jdbi_batch(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useHandle(
            h -> {
              var batch = h.prepareBatch(JDBC_SQL);
              for (var params : paramSets) {
                batch
                    .bind(0, (int) params[0])
                    .bind(1, (BigDecimal) params[1])
                    .bind(2, (String) params[2])
                    .add();
              }
              bh.consume(batch.execute());
            });
  }

  // --- Hibernate persist loop ---

  @Benchmark
  public void jdbc_hibernate_persist(DatabaseState db, Blackhole bh) {
    try (var session = db.sessionFactory().openSession()) {
      var tx = session.beginTransaction();
      for (var params : paramSets) {
        session
            .createNativeMutationQuery(JDBC_SQL)
            .setParameter(1, params[0])
            .setParameter(2, params[1])
            .setParameter(3, params[2])
            .executeUpdate();
      }
      tx.commit();
    }
  }

  // --- jOOQ ---

  @Benchmark
  public void jdbc_jooq_batch(DatabaseState db, Blackhole bh) {
    db.jooq()
        .transaction(
            cfg -> {
              var batch = org.jooq.impl.DSL.using(cfg).batch(JDBC_SQL);
              for (var params : paramSets) {
                batch.bind(params);
              }
              bh.consume(batch.execute());
            });
  }

  // --- Sql2o ---

  private static final String SQL2O_SQL =
      "INSERT INTO bench_orders (user_id, total, status) VALUES (:p1, :p2, :p3)";

  @Benchmark
  public void jdbc_sql2o_batch(DatabaseState db, Blackhole bh) {
    try (var con = db.sql2o().beginTransaction()) {
      var query = con.createQuery(SQL2O_SQL);
      for (var params : paramSets) {
        query.withParams(params).addToBatch();
      }
      bh.consume(query.executeBatch());
      con.commit();
    }
  }

  // --- Spring JdbcTemplate ---

  @Benchmark
  public void jdbc_spring_batch(DatabaseState db, Blackhole bh) {
    db.jdbcTemplate()
        .batchUpdate(
            JDBC_SQL,
            paramSets,
            paramSets.size(),
            (ps, params) -> {
              ps.setInt(1, (int) params[0]);
              ps.setBigDecimal(2, (BigDecimal) params[1]);
              ps.setString(3, (String) params[2]);
            });
  }

  // --- Vert.x ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_batch(DatabaseState db, Blackhole bh) {
    var tuples = new ArrayList<Tuple>(BATCH_SIZE);
    for (var params : paramSets) {
      tuples.add(Tuple.of(params[0], params[1], params[2]));
    }
    var result =
        db.vertxPool()
            .preparedQuery(SQL)
            .executeBatch(tuples)
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    bh.consume(result);
  }
}
