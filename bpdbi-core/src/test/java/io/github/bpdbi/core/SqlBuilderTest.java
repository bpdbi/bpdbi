package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.bpdbi.core.test.AbstractStubConnection;
import io.github.bpdbi.core.test.TestRows;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

class SqlBuilderTest {

  static class RecordingConnection extends AbstractStubConnection {

    final List<String> sqls = new ArrayList<>();
    final List<Object> paramSets = new ArrayList<>();

    @Override
    public @NonNull RowSet query(@NonNull String sql) {
      sqls.add(sql);
      paramSets.add(null);
      return new RowSet(
          List.of(TestRows.row(new String[] {"v"}, new String[] {"1"})), List.of(), 0);
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
      sqls.add(sql);
      paramSets.add(params);
      return new RowSet(
          List.of(TestRows.row(new String[] {"v"}, new String[] {"1"})), List.of(), 0);
    }

    @Override
    public int enqueue(@NonNull String sql) {
      sqls.add("enqueue:" + sql);
      return 0;
    }

    @Override
    public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
      sqls.add("enqueue:" + sql);
      paramSets.add(params);
      return 0;
    }
  }

  @Test
  void queryWithoutParams() {
    var conn = new RecordingConnection();
    conn.sql("SELECT 1").query();
    assertEquals("SELECT 1", conn.sqls.getFirst());
    assertNull(conn.paramSets.getFirst());
  }

  @Test
  void queryWithNamedParams() {
    var conn = new RecordingConnection();
    conn.sql("SELECT * FROM t WHERE a = :a AND b = :b").bind("a", 1).bind("b", "x").query();

    assertEquals("SELECT * FROM t WHERE a = :a AND b = :b", conn.sqls.getFirst());
    @SuppressWarnings("unchecked")
    var params = (Map<String, Object>) conn.paramSets.getFirst();
    assertEquals(1, params.get("a"));
    assertEquals("x", params.get("b"));
  }

  @Test
  void enqueueWithoutParams() {
    var conn = new RecordingConnection();
    conn.sql("BEGIN").enqueue();
    assertEquals("enqueue:BEGIN", conn.sqls.getFirst());
  }

  @Test
  void enqueueWithNamedParams() {
    var conn = new RecordingConnection();
    conn.sql("INSERT INTO t (v) VALUES (:v)").bind("v", 42).enqueue();
    assertEquals("enqueue:INSERT INTO t (v) VALUES (:v)", conn.sqls.getFirst());
  }

  @Test
  void mapTo() {
    var conn = new RecordingConnection();
    List<String> result = conn.sql("SELECT v FROM t").mapTo(row -> row.getString(0));
    assertEquals(List.of("1"), result);
  }

  @Test
  void mapFirst() {
    var conn = new RecordingConnection();
    String result = conn.sql("SELECT v FROM t").mapFirst(row -> row.getString(0));
    assertEquals("1", result);
  }

  @Test
  void mapFirstOrNull() {
    var conn = new RecordingConnection();
    String result = conn.sql("SELECT v FROM t").mapFirstOrNull(row -> row.getString(0));
    assertEquals("1", result);
  }

  @Test
  void mapFirstOrNullEmpty() {
    var conn =
        new RecordingConnection() {
          @Override
          public @NonNull RowSet query(@NonNull String sql) {
            return new RowSet(List.of(), List.of(), 0);
          }
        };
    String result = conn.sql("SELECT v FROM t WHERE 1=0").mapFirstOrNull(row -> row.getString(0));
    assertNull(result);
  }

  @Test
  void bindNullValue() {
    var conn = new RecordingConnection();
    conn.sql("INSERT INTO t (v) VALUES (:v)").bind("v", null).query();

    @SuppressWarnings("unchecked")
    var params = (Map<String, Object>) conn.paramSets.getFirst();
    assertNull(params.get("v"));
  }

  @Test
  void sqlBuilderFromTransaction() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    assertNotNull(tx.sql("SELECT 1"));
    tx.sql("SELECT :x").bind("x", 1).query();
    tx.commit();
  }
}
