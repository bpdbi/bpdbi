package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.github.bpdbi.benchmark.model.EventBean;
import io.github.bpdbi.benchmark.model.EventRecord;
import io.github.bpdbi.mapper.javabean.JavaBeanRowMapper;
import io.github.bpdbi.mapper.record.RecordRowMapper;
import io.vertx.sqlclient.Tuple;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
import org.jooq.impl.DSL;
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
 * Benchmarks INSERT with many bindings of mixed types (string, int, long, UUID, BigDecimal,
 * boolean, LocalDateTime, null). 15 bound parameters per row, 50 rows per batch. Measures parameter
 * encoding cost across drivers.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@State(Scope.Thread)
public class ManyBindingsBenchmark {

  private static final int BATCH_SIZE = 50;

  private static final String INSERT_SQL =
      "INSERT INTO events (event_uuid, event_type, user_id, sequence_num, amount, discount,"
          + " processed, flagged, source, category, notes, reference_code, correlation_id,"
          + " created_at, updated_at)"
          + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";

  private static final String JDBC_INSERT_SQL =
      "INSERT INTO events (event_uuid, event_type, user_id, sequence_num, amount, discount,"
          + " processed, flagged, source, category, notes, reference_code, correlation_id,"
          + " created_at, updated_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String SELECT_SQL =
      "SELECT id, event_uuid, event_type, user_id, sequence_num, amount, discount,"
          + " processed, flagged, source, category, notes, reference_code, correlation_id,"
          + " created_at, updated_at"
          + " FROM events WHERE user_id = $1 LIMIT 20";

  private static final String JDBC_SELECT_SQL =
      "SELECT id, event_uuid, event_type, user_id, sequence_num, amount, discount,"
          + " processed, flagged, source, category, notes, reference_code, correlation_id,"
          + " created_at, updated_at"
          + " FROM events WHERE user_id = ? LIMIT 20";

  private static final String[] EVENT_TYPES = {
    "click", "purchase", "signup", "logout", "page_view", "search"
  };
  private static final String[] SOURCES = {"web", "mobile", "api", "webhook", "import"};
  private static final String[] CATEGORIES = {
    "marketing", "billing", "auth", "analytics", "system"
  };

  private List<Object[]> paramSets;
  private int selectUserId;

  @Setup(Level.Trial)
  public void prepareParams() {
    var rng = new Random(77);
    paramSets = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      var now = LocalDateTime.of(2025, 6, 1, 10, 0).plusMinutes(i);
      boolean hasNotes = rng.nextBoolean();
      boolean hasRef = rng.nextBoolean();
      paramSets.add(
          new Object[] {
            UUID.randomUUID(), // event_uuid
            EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)], // event_type
            rng.nextInt(1000) + 1, // user_id
            (long) i + 1000L, // sequence_num
            BigDecimal.valueOf(rng.nextDouble() * 999 + 1).setScale(2, RoundingMode.HALF_UP),
            BigDecimal.valueOf(rng.nextDouble() * 50).setScale(2, RoundingMode.HALF_UP),
            rng.nextBoolean(), // processed
            rng.nextBoolean(), // flagged
            SOURCES[rng.nextInt(SOURCES.length)], // source
            CATEGORIES[rng.nextInt(CATEGORIES.length)], // category
            hasNotes ? "Note for event " + i : null, // notes (nullable)
            hasRef ? "REF-" + rng.nextInt(100000) : null, // reference_code (nullable)
            UUID.randomUUID(), // correlation_id
            now, // created_at
            now.plusSeconds(rng.nextInt(3600)) // updated_at
          });
    }
    selectUserId = (int) paramSets.get(0)[2];
  }

  @Setup(Level.Iteration)
  public void seedTable(DatabaseState db) {
    try (var conn = db.newBpdbiConnection()) {
      conn.query("TRUNCATE events RESTART IDENTITY");
      for (var params : paramSets) {
        conn.enqueue(INSERT_SQL, params);
      }
      conn.flush();
    }
  }

  // ===== INSERT benchmarks =====

  // --- bpdbi ---

  @Benchmark
  public void insert_bpdbi_executeMany(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.executeMany(INSERT_SQL, paramSets));
    }
  }

  // --- JDBC batch ---

  @Benchmark
  public void insert_jdbc_batch(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection()) {
      jdbcConn.setAutoCommit(false);
      try (var ps = jdbcConn.prepareStatement(JDBC_INSERT_SQL)) {
        for (var params : paramSets) {
          ps.setObject(1, params[0]); // UUID
          ps.setString(2, (String) params[1]);
          ps.setInt(3, (int) params[2]);
          ps.setLong(4, (long) params[3]);
          ps.setBigDecimal(5, (BigDecimal) params[4]);
          ps.setBigDecimal(6, (BigDecimal) params[5]);
          ps.setBoolean(7, (boolean) params[6]);
          ps.setBoolean(8, (boolean) params[7]);
          ps.setString(9, (String) params[8]);
          ps.setString(10, (String) params[9]);
          ps.setString(11, (String) params[10]); // nullable
          ps.setString(12, (String) params[11]); // nullable
          ps.setObject(13, params[12]); // UUID
          ps.setObject(14, params[13]); // LocalDateTime
          ps.setObject(15, params[14]); // LocalDateTime
          ps.addBatch();
        }
        bh.consume(ps.executeBatch());
      }
      jdbcConn.commit();
    }
  }

  // --- Jdbi batch (named parameters) ---

  @Benchmark
  public void insert_jdbc_jdbi_batch(DatabaseState db, Blackhole bh) {
    db.jdbi()
        .useHandle(
            h -> {
              var batch =
                  h.prepareBatch(
                      "INSERT INTO events (event_uuid, event_type, user_id, sequence_num, amount,"
                          + " discount, processed, flagged, source, category, notes,"
                          + " reference_code, correlation_id, created_at, updated_at)"
                          + " VALUES (:eventUuid, :eventType, :userId, :sequenceNum, :amount,"
                          + " :discount, :processed, :flagged, :source, :category, :notes,"
                          + " :referenceCode, :correlationId, :createdAt, :updatedAt)");
              for (var params : paramSets) {
                batch
                    .bind("eventUuid", params[0])
                    .bind("eventType", (String) params[1])
                    .bind("userId", (int) params[2])
                    .bind("sequenceNum", (long) params[3])
                    .bind("amount", (BigDecimal) params[4])
                    .bind("discount", (BigDecimal) params[5])
                    .bind("processed", (boolean) params[6])
                    .bind("flagged", (boolean) params[7])
                    .bind("source", (String) params[8])
                    .bind("category", (String) params[9])
                    .bind("notes", (String) params[10])
                    .bind("referenceCode", (String) params[11])
                    .bind("correlationId", params[12])
                    .bind("createdAt", params[13])
                    .bind("updatedAt", params[14])
                    .add();
              }
              bh.consume(batch.execute());
            });
  }

  // --- Hibernate native mutation ---

  @Benchmark
  public void insert_jdbc_hibernate(DatabaseState db, Blackhole bh) {
    try (var session = db.sessionFactory().openSession()) {
      var tx = session.beginTransaction();
      for (var params : paramSets) {
        session
            .createNativeMutationQuery(JDBC_INSERT_SQL)
            .setParameter(1, params[0])
            .setParameter(2, params[1])
            .setParameter(3, params[2])
            .setParameter(4, params[3])
            .setParameter(5, params[4])
            .setParameter(6, params[5])
            .setParameter(7, params[6])
            .setParameter(8, params[7])
            .setParameter(9, params[8])
            .setParameter(10, params[9])
            .setParameter(11, params[10])
            .setParameter(12, params[11])
            .setParameter(13, params[12])
            .setParameter(14, params[13])
            .setParameter(15, params[14])
            .executeUpdate();
      }
      tx.commit();
    }
  }

  // --- jOOQ batch ---

  @Benchmark
  public void insert_jdbc_jooq(DatabaseState db, Blackhole bh) {
    db.jooq()
        .transaction(
            cfg -> {
              var ctx = DSL.using(cfg);
              for (var params : paramSets) {
                ctx.execute(JDBC_INSERT_SQL, params);
              }
            });
  }

  // --- Sql2o ---

  @Benchmark
  public void insert_jdbc_sql2o(DatabaseState db, Blackhole bh) {
    try (var con = db.sql2o().beginTransaction()) {
      for (var params : paramSets) {
        con.createQuery(JDBC_INSERT_SQL).withParams(params).executeUpdate();
      }
      con.commit();
    }
  }

  // --- Spring JdbcTemplate batch ---

  @Benchmark
  public void insert_jdbc_spring(DatabaseState db, Blackhole bh) {
    db.jdbcTemplate()
        .batchUpdate(
            JDBC_INSERT_SQL,
            paramSets,
            paramSets.size(),
            (ps, params) -> {
              ps.setObject(1, params[0]); // UUID
              ps.setString(2, (String) params[1]);
              ps.setInt(3, (int) params[2]);
              ps.setLong(4, (long) params[3]);
              ps.setBigDecimal(5, (BigDecimal) params[4]);
              ps.setBigDecimal(6, (BigDecimal) params[5]);
              ps.setBoolean(7, (boolean) params[6]);
              ps.setBoolean(8, (boolean) params[7]);
              ps.setString(9, (String) params[8]);
              ps.setString(10, (String) params[9]);
              ps.setString(11, (String) params[10]);
              ps.setString(12, (String) params[11]);
              ps.setObject(13, params[12]); // UUID
              ps.setObject(14, params[13]); // LocalDateTime
              ps.setObject(15, params[14]); // LocalDateTime
            });
  }

  // --- Vert.x batch ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void insert_vertx_batch(DatabaseState db, Blackhole bh) {
    var tuples = new ArrayList<Tuple>(BATCH_SIZE);
    for (var params : paramSets) {
      tuples.add(
          Tuple.of(
              params[0],
              params[1],
              params[2],
              params[3],
              params[4],
              params[5],
              params[6],
              params[7],
              params[8],
              params[9],
              params[10],
              params[11],
              params[12],
              params[13],
              params[14]));
    }
    var result =
        db.vertxPool()
            .preparedQuery(INSERT_SQL)
            .executeBatch(tuples)
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    bh.consume(result);
  }

  // ===== SELECT benchmarks (read back wide rows) =====

  // --- bpdbi raw ---

  @Benchmark
  public void select_bpdbi_raw(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var rs = conn.query(SELECT_SQL, selectUserId);
      for (var row : rs) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getUUID("event_uuid"));
        bh.consume(row.getString("event_type"));
        bh.consume(row.getInteger("user_id"));
        bh.consume(row.getLong("sequence_num"));
        bh.consume(row.getBigDecimal("amount"));
        bh.consume(row.getBigDecimal("discount"));
        bh.consume(row.getBoolean("processed"));
        bh.consume(row.getBoolean("flagged"));
        bh.consume(row.getString("source"));
        bh.consume(row.getString("category"));
        bh.consume(row.getString("notes"));
        bh.consume(row.getString("reference_code"));
        bh.consume(row.getUUID("correlation_id"));
        bh.consume(row.getLocalDateTime("created_at"));
        bh.consume(row.getLocalDateTime("updated_at"));
      }
    }
  }

  // --- bpdbi record ---

  @Benchmark
  public void select_bpdbi_record(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var events =
          conn.query(SELECT_SQL, selectUserId).mapTo(RecordRowMapper.of(EventRecord.class));
      bh.consume(events);
    }
  }

  // --- bpdbi bean ---

  @Benchmark
  public void select_bpdbi_bean(DatabaseState db, Blackhole bh) {
    try (var conn = db.bpdbiPool().acquire()) {
      var events =
          conn.query(SELECT_SQL, selectUserId).mapTo(JavaBeanRowMapper.of(EventBean.class));
      bh.consume(events);
    }
  }

  // --- JDBC raw ---

  @Benchmark
  public void select_jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SELECT_SQL)) {
      ps.setInt(1, selectUserId);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          bh.consume(rs.getInt("id"));
          bh.consume(rs.getObject("event_uuid", UUID.class));
          bh.consume(rs.getString("event_type"));
          bh.consume(rs.getInt("user_id"));
          bh.consume(rs.getLong("sequence_num"));
          bh.consume(rs.getBigDecimal("amount"));
          bh.consume(rs.getBigDecimal("discount"));
          bh.consume(rs.getBoolean("processed"));
          bh.consume(rs.getBoolean("flagged"));
          bh.consume(rs.getString("source"));
          bh.consume(rs.getString("category"));
          bh.consume(rs.getString("notes"));
          bh.consume(rs.getString("reference_code"));
          bh.consume(rs.getObject("correlation_id", UUID.class));
          bh.consume(rs.getTimestamp("created_at"));
          bh.consume(rs.getTimestamp("updated_at"));
        }
      }
    }
  }

  // --- Jdbi (named parameters, BeanMapper) ---

  @Benchmark
  public void select_jdbc_jdbi_bean(DatabaseState db, Blackhole bh) {
    var events =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(
                            "SELECT id, event_uuid, event_type, user_id, sequence_num, amount,"
                                + " discount, processed, flagged, source, category, notes,"
                                + " reference_code, correlation_id, created_at, updated_at"
                                + " FROM events WHERE user_id = :userId LIMIT 20")
                        .bind("userId", selectUserId)
                        .map(BeanMapper.of(EventBean.class))
                        .list());
    bh.consume(events);
  }

  // --- Hibernate native query ---

  @Benchmark
  public void select_jdbc_hibernate(DatabaseState db, Blackhole bh) {
    try (var session = db.sessionFactory().openSession()) {
      var results =
          session
              .createNativeQuery(JDBC_SELECT_SQL, Object[].class)
              .setParameter(1, selectUserId)
              .getResultList();
      bh.consume(results);
    }
  }

  // --- jOOQ select ---

  @Benchmark
  public void select_jdbc_jooq(DatabaseState db, Blackhole bh) {
    var results = db.jooq().fetch(JDBC_SELECT_SQL, selectUserId);
    bh.consume(results);
  }

  // --- Sql2o select ---

  @Benchmark
  public void select_jdbc_sql2o(DatabaseState db, Blackhole bh) {
    try (var con = db.sql2o().open()) {
      var events =
          con.createQuery(JDBC_SELECT_SQL)
              .withParams(selectUserId)
              .executeAndFetch(EventBean.class);
      bh.consume(events);
    }
  }

  // --- Spring JdbcTemplate select ---

  @Benchmark
  public void select_jdbc_spring(DatabaseState db, Blackhole bh) {
    var events =
        db.jdbcTemplate()
            .query(
                JDBC_SELECT_SQL,
                (rs, rowNum) -> {
                  var e = new EventBean();
                  e.setId(rs.getInt("id"));
                  e.setEventUuid(rs.getObject("event_uuid", UUID.class));
                  e.setEventType(rs.getString("event_type"));
                  e.setUserId(rs.getInt("user_id"));
                  e.setSequenceNum(rs.getLong("sequence_num"));
                  e.setAmount(rs.getBigDecimal("amount"));
                  e.setDiscount(rs.getBigDecimal("discount"));
                  e.setProcessed(rs.getBoolean("processed"));
                  e.setFlagged(rs.getBoolean("flagged"));
                  e.setSource(rs.getString("source"));
                  e.setCategory(rs.getString("category"));
                  e.setNotes(rs.getString("notes"));
                  e.setReferenceCode(rs.getString("reference_code"));
                  e.setCorrelationId(rs.getObject("correlation_id", UUID.class));
                  e.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
                  e.setUpdatedAt(rs.getTimestamp("updated_at").toLocalDateTime());
                  return e;
                },
                selectUserId);
    bh.consume(events);
  }

  // --- Vert.x ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void select_vertx_raw(DatabaseState db, Blackhole bh) {
    var rows =
        db.vertxPool()
            .preparedQuery(SELECT_SQL)
            .execute(Tuple.of(selectUserId))
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    for (var row : rows) {
      bh.consume(row.getInteger("id"));
      bh.consume(row.getUUID("event_uuid"));
      bh.consume(row.getString("event_type"));
      bh.consume(row.getInteger("user_id"));
      bh.consume(row.getLong("sequence_num"));
      bh.consume(row.getBigDecimal("amount"));
      bh.consume(row.getBigDecimal("discount"));
      bh.consume(row.getBoolean("processed"));
      bh.consume(row.getBoolean("flagged"));
      bh.consume(row.getString("source"));
      bh.consume(row.getString("category"));
      bh.consume(row.getString("notes"));
      bh.consume(row.getString("reference_code"));
      bh.consume(row.getUUID("correlation_id"));
      bh.consume(row.getLocalDateTime("created_at"));
      bh.consume(row.getLocalDateTime("updated_at"));
    }
  }
}
