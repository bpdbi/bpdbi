package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.github.bpdbi.benchmark.model.UserBean;
import io.github.bpdbi.benchmark.model.UserEntity;
import io.github.bpdbi.benchmark.model.UserRecord;
import io.github.bpdbi.mapper.javabean.JavaBeanRowMapper;
import io.github.bpdbi.mapper.record.RecordRowMapper;
import io.vertx.sqlclient.Tuple;
import java.util.concurrent.TimeUnit;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
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
public class SingleRowLookupBenchmark {

  private static final String SQL =
      "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = $1";
  private static final String JDBC_SQL =
      "SELECT id, username, email, full_name, bio, active, created_at FROM users WHERE id = ?";

  @Param({"1", "50", "500"})
  int userId;

  // --- bpdbi ---

  @Benchmark
  public void bpdbi_raw(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var row = conn.query(SQL, userId).first();
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
      bh.consume(row.getString("full_name"));
      bh.consume(row.getString("bio"));
      bh.consume(row.getBoolean("active"));
      bh.consume(row.getLocalDateTime("created_at"));
    }
  }

  @Benchmark
  public void bpdbi_record(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var user = conn.query(SQL, userId).mapFirst(RecordRowMapper.of(UserRecord.class));
      bh.consume(user);
    }
  }

  @Benchmark
  public void bpdbi_bean(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var user = conn.query(SQL, userId).mapFirst(JavaBeanRowMapper.of(UserBean.class));
      bh.consume(user);
    }
  }

  // --- JDBC raw ---

  @Benchmark
  public void jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      ps.setInt(1, userId);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
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

  // --- JDBC + Jdbi ---

  @Benchmark
  public void jdbc_jdbi_raw(DatabaseState db, Blackhole bh) {
    var user =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(JDBC_SQL)
                        .bind(0, userId)
                        .map(
                            (rs, ctx) -> {
                              var u = new UserBean();
                              u.setId(rs.getInt("id"));
                              u.setUsername(rs.getString("username"));
                              u.setEmail(rs.getString("email"));
                              u.setFullName(rs.getString("full_name"));
                              u.setBio(rs.getString("bio"));
                              u.setActive(rs.getBoolean("active"));
                              u.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                              return u;
                            })
                        .findFirst()
                        .orElse(null));
    bh.consume(user);
  }

  @Benchmark
  public void jdbc_jdbi_bean(DatabaseState db, Blackhole bh) {
    var user =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(JDBC_SQL)
                        .bind(0, userId)
                        .map(BeanMapper.of(UserBean.class))
                        .findFirst()
                        .orElse(null));
    bh.consume(user);
  }

  // --- Hibernate ---

  @Benchmark
  public void jdbc_hibernate(DatabaseState db, Blackhole bh) {
    try (var session = db.sessionFactory().openSession()) {
      var user = session.find(UserEntity.class, userId);
      bh.consume(user);
    }
  }

  // --- jOOQ ---

  @Benchmark
  public void jdbc_jooq(DatabaseState db, Blackhole bh) {
    var user = db.jooq().fetchOne(JDBC_SQL, userId);
    bh.consume(user);
  }

  // --- Sql2o ---

  @Benchmark
  public void jdbc_sql2o(DatabaseState db, Blackhole bh) {
    try (var con = db.sql2o().open()) {
      var user = con.createQuery(JDBC_SQL).withParams(userId).executeAndFetchFirst(UserBean.class);
      bh.consume(user);
    }
  }

  // --- Spring JdbcTemplate ---

  @Benchmark
  public void jdbc_spring(DatabaseState db, Blackhole bh) {
    var user =
        db.jdbcTemplate()
            .queryForObject(
                JDBC_SQL,
                (rs, rowNum) -> {
                  var u = new UserBean();
                  u.setId(rs.getInt("id"));
                  u.setUsername(rs.getString("username"));
                  u.setEmail(rs.getString("email"));
                  u.setFullName(rs.getString("full_name"));
                  u.setBio(rs.getString("bio"));
                  u.setActive(rs.getBoolean("active"));
                  u.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                  return u;
                },
                userId);
    bh.consume(user);
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
      bh.consume(row.getString("username"));
      bh.consume(row.getString("email"));
      bh.consume(row.getString("full_name"));
      bh.consume(row.getString("bio"));
      bh.consume(row.getBoolean("active"));
      bh.consume(row.getLocalDateTime("created_at"));
    }
  }
}
