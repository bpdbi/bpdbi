package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.github.bpdbi.benchmark.model.OrderSummaryBean;
import io.github.bpdbi.benchmark.model.OrderSummaryRecord;
import io.github.bpdbi.mapper.javabean.JavaBeanRowMapper;
import io.github.bpdbi.mapper.record.RecordRowMapper;
import io.vertx.sqlclient.Tuple;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
public class JoinQueryBenchmark {

  private static final String SQL =
      """
      SELECT o.id, o.total, o.status, o.created_at, u.username, COUNT(oi.id) as item_count
      FROM orders o
      JOIN users u ON u.id = o.user_id
      JOIN order_items oi ON oi.order_id = o.id
      WHERE o.user_id = $1
      GROUP BY o.id, o.total, o.status, o.created_at, u.username
      ORDER BY o.created_at DESC
      LIMIT 20""";

  private static final String JDBC_SQL =
      """
      SELECT o.id, o.total, o.status, o.created_at, u.username, COUNT(oi.id) as item_count
      FROM orders o
      JOIN users u ON u.id = o.user_id
      JOIN order_items oi ON oi.order_id = o.id
      WHERE o.user_id = ?
      GROUP BY o.id, o.total, o.status, o.created_at, u.username
      ORDER BY o.created_at DESC
      LIMIT 20""";

  @Param({"1", "50", "500"})
  int userId;

  // --- bpdbi ---

  @Benchmark
  public void bpdbi_raw(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var rs = conn.query(SQL, userId);
      for (var row : rs) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getBigDecimal("total"));
        bh.consume(row.getString("status"));
        bh.consume(row.getLocalDateTime("created_at"));
        bh.consume(row.getString("username"));
        bh.consume(row.getLong("item_count"));
      }
    }
  }

  @Benchmark
  public void bpdbi_record(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.query(SQL, userId).mapTo(RecordRowMapper.of(OrderSummaryRecord.class)));
    }
  }

  @Benchmark
  public void bpdbi_bean(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.query(SQL, userId).mapTo(JavaBeanRowMapper.of(OrderSummaryBean.class)));
    }
  }

  // --- JDBC raw ---

  @Benchmark
  public void jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      ps.setInt(1, userId);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          bh.consume(rs.getInt("id"));
          bh.consume(rs.getBigDecimal("total"));
          bh.consume(rs.getString("status"));
          bh.consume(rs.getTimestamp("created_at"));
          bh.consume(rs.getString("username"));
          bh.consume(rs.getLong("item_count"));
        }
      }
    }
  }

  // --- JDBC + Jdbi ---

  @Benchmark
  public void jdbc_jdbi_raw(DatabaseState db, Blackhole bh) {
    var orders =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(JDBC_SQL)
                        .bind(0, userId)
                        .map(
                            (rs, ctx) -> {
                              var o = new OrderSummaryBean();
                              o.setId(rs.getInt("id"));
                              o.setTotal(rs.getBigDecimal("total"));
                              o.setStatus(rs.getString("status"));
                              o.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                              o.setUsername(rs.getString("username"));
                              o.setItemCount(rs.getLong("item_count"));
                              return o;
                            })
                        .list());
    bh.consume(orders);
  }

  // --- Hibernate (native query, since this is a join + aggregate) ---

  @Benchmark
  public void jdbc_hibernate(DatabaseState db, Blackhole bh) {
    try (var session = db.sessionFactory().openSession()) {
      var results =
          session
              .createNativeQuery(JDBC_SQL, Object[].class)
              .setParameter(1, userId)
              .getResultList();
      bh.consume(results);
    }
  }

  // --- jOOQ ---

  @Benchmark
  public void jdbc_jooq(DatabaseState db, Blackhole bh) {
    var results = db.jooq().fetch(JDBC_SQL, userId);
    bh.consume(results);
  }

  // --- Sql2o ---

  @Benchmark
  public void jdbc_sql2o(DatabaseState db, Blackhole bh) {
    try (var con = db.sql2o().open()) {
      var results =
          con.createQuery(JDBC_SQL).withParams(userId).executeAndFetch(OrderSummaryBean.class);
      bh.consume(results);
    }
  }

  // --- Spring JdbcTemplate ---

  @Benchmark
  public void jdbc_spring(DatabaseState db, Blackhole bh) {
    var results =
        db.jdbcTemplate()
            .query(
                JDBC_SQL,
                (rs, rowNum) -> {
                  var o = new OrderSummaryBean();
                  o.setId(rs.getInt("id"));
                  o.setTotal(rs.getBigDecimal("total"));
                  o.setStatus(rs.getString("status"));
                  o.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                  o.setUsername(rs.getString("username"));
                  o.setItemCount(rs.getLong("item_count"));
                  return o;
                },
                userId);
    bh.consume(results);
  }

  // --- Vert.x ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_raw(DatabaseState db, Blackhole bh) {
    var rows =
        db.vertxPool()
            .preparedQuery(SQL)
            .execute(Tuple.of(userId))
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    for (var row : rows) {
      bh.consume(row.getInteger("id"));
      bh.consume(row.getBigDecimal("total"));
      bh.consume(row.getString("status"));
      bh.consume(row.getLocalDateTime("created_at"));
      bh.consume(row.getString("username"));
      bh.consume(row.getLong("item_count"));
    }
  }
}
