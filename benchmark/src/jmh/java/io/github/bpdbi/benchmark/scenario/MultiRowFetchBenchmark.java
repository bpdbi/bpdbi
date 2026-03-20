package io.github.bpdbi.benchmark.scenario;

import io.github.bpdbi.benchmark.infra.DatabaseState;
import io.github.bpdbi.benchmark.model.ProductBean;
import io.github.bpdbi.benchmark.model.ProductEntity;
import io.github.bpdbi.benchmark.model.ProductRecord;
import io.github.bpdbi.mapper.JavaBeanRowMapper;
import io.github.bpdbi.mapper.RecordRowMapper;
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
public class MultiRowFetchBenchmark {

  private static final String SQL =
      "SELECT id, name, description, price, category, stock FROM products WHERE category = $1";
  private static final String JDBC_SQL =
      "SELECT id, name, description, price, category, stock FROM products WHERE category = ?";

  @Param({"0", "1", "2"})
  int categoryIdx;

  // --- bpdbi ---

  @Benchmark
  public void bpdbi_raw(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    try (var conn = db.bpdbiPool().acquire()) {
      var rs = conn.query(SQL, category);
      for (var row : rs) {
        bh.consume(row.getInteger("id"));
        bh.consume(row.getString("name"));
        bh.consume(row.getString("description"));
        bh.consume(row.getBigDecimal("price"));
        bh.consume(row.getString("category"));
        bh.consume(row.getInteger("stock"));
      }
    }
  }

  @Benchmark
  public void bpdbi_record(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.query(SQL, category).mapTo(RecordRowMapper.of(ProductRecord.class)));
    }
  }

  @Benchmark
  public void bpdbi_bean(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    try (var conn = db.bpdbiPool().acquire()) {
      bh.consume(conn.query(SQL, category).mapTo(JavaBeanRowMapper.of(ProductBean.class)));
    }
  }

  // --- JDBC raw ---

  @Benchmark
  public void jdbc_raw(DatabaseState db, Blackhole bh) throws Exception {
    var category = DatabaseState.categoryForParam(categoryIdx);
    try (var jdbcConn = db.hikariDataSource().getConnection();
        var ps = jdbcConn.prepareStatement(JDBC_SQL)) {
      ps.setString(1, category);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          bh.consume(rs.getInt("id"));
          bh.consume(rs.getString("name"));
          bh.consume(rs.getString("description"));
          bh.consume(rs.getBigDecimal("price"));
          bh.consume(rs.getString("category"));
          bh.consume(rs.getInt("stock"));
        }
      }
    }
  }

  // --- JDBC + Jdbi ---

  @Benchmark
  public void jdbc_jdbi_raw(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    var products =
        db.jdbi()
            .withHandle(
                h ->
                    h.createQuery(JDBC_SQL)
                        .bind(0, category)
                        .map(
                            (rs, ctx) -> {
                              var p = new ProductBean();
                              p.setId(rs.getInt("id"));
                              p.setName(rs.getString("name"));
                              p.setDescription(rs.getString("description"));
                              p.setPrice(rs.getBigDecimal("price"));
                              p.setCategory(rs.getString("category"));
                              p.setStock(rs.getInt("stock"));
                              return p;
                            })
                        .list());
    bh.consume(products);
  }

  // --- Hibernate ---

  @Benchmark
  public void jdbc_hibernate(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    try (var session = db.sessionFactory().openSession()) {
      var cb = session.getCriteriaBuilder();
      var cq = cb.createQuery(ProductEntity.class);
      var root = cq.from(ProductEntity.class);
      cq.where(cb.equal(root.get("category"), category));
      var products = session.createQuery(cq).getResultList();
      bh.consume(products);
    }
  }

  // --- Vert.x ---

  // @Benchmark  // Vert.x disabled: not a meaningful comparison
  public void vertx_raw(DatabaseState db, Blackhole bh) {
    var category = DatabaseState.categoryForParam(categoryIdx);
    var rows =
        db.vertxPool()
            .preparedQuery(SQL)
            .execute(Tuple.of(category))
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    for (var row : rows) {
      bh.consume(row.getInteger("id"));
      bh.consume(row.getString("name"));
      bh.consume(row.getString("description"));
      bh.consume(row.getBigDecimal("price"));
      bh.consume(row.getString("category"));
      bh.consume(row.getInteger("stock"));
    }
  }
}
