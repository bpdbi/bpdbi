package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.DbException;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for BaseConnection pipeline logic using a mock encoder/decoder. No Docker or database
 * required.
 */
class BaseConnectionTest {

  /** Minimal concrete BaseConnection that records what was sent and returns canned responses. */
  static class FakeConnection extends BaseConnection {

    final List<ExtendedQuery> extendedQueries = new ArrayList<>();
    boolean terminated = false;
    boolean transportClosed = false;

    final List<RowSet> extendedResponses = new ArrayList<>();

    static class ExtendedQuery {

      final String sql;
      final String[] params;

      ExtendedQuery(String sql, String[] params) {
        this.sql = sql;
        this.params = params;
      }
    }

    @Override
    protected void flushToNetwork() {}

    @Override
    protected @NonNull RowSet executeExtendedQuery(@NonNull String sql, String[] params) {
      extendedQueries.add(new ExtendedQuery(sql, params));
      return extendedResponses.isEmpty()
          ? new RowSet(List.of(), List.of(), 0)
          : extendedResponses.removeFirst();
    }

    @Override
    protected void sendTerminate() {
      terminated = true;
    }

    @Override
    protected void closeTransport() {
      transportClosed = true;
    }

    @Override
    protected @NonNull String placeholderPrefix() {
      return "$";
    }

    @Override
    protected void closeCachedStatement(PreparedStatementCache.CachedStatement stmt) {}

    @Override
    public @NonNull PreparedStatement prepare(@NonNull String sql) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull Cursor cursor(@NonNull String sql, Object... params) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void ping() {}

    @Override
    public @NonNull Map<String, String> parameters() {
      return Map.of();
    }

    @Override
    protected void executeExtendedQueryStreaming(
        @NonNull String sql, String[] params, @NonNull Consumer<Row> consumer) {}

    @Override
    protected @NonNull RowStream createExtendedQueryRowStream(
        @NonNull String sql, String[] params) {
      return new RowStream(() -> null, () -> {});
    }
  }

  // ===== Enqueue and flush =====

  @Test
  void emptyFlushReturnsEmptyList() {
    var conn = new FakeConnection();
    var results = conn.flush();
    assertTrue(results.isEmpty());
  }

  @Test
  void singleQueryNoParams() {
    var conn = new FakeConnection();
    int idx = conn.enqueue("SELECT 1");
    assertEquals(0, idx);

    var results = conn.flush();
    assertEquals(1, results.size());
    assertEquals(1, conn.extendedQueries.size());
    assertEquals("SELECT 1", conn.extendedQueries.getFirst().sql);
    assertArrayEquals(new String[0], conn.extendedQueries.getFirst().params);
  }

  @Test
  void multipleQueriesNoParams() {
    var conn = new FakeConnection();
    conn.enqueue("SELECT 1");
    conn.enqueue("SELECT 2");
    conn.enqueue("SELECT 3");

    var results = conn.flush();
    assertEquals(3, results.size());
    assertEquals(3, conn.extendedQueries.size());
  }

  @Test
  void singleQueryWithParams() {
    var conn = new FakeConnection();
    conn.enqueue("SELECT $1", 42);

    var results = conn.flush();
    assertEquals(1, results.size());
    assertEquals(1, conn.extendedQueries.size());
    assertEquals("SELECT $1", conn.extendedQueries.getFirst().sql);
    assertArrayEquals(new String[] {"42"}, conn.extendedQueries.getFirst().params);
  }

  @Test
  void mixedQueriesWithAndWithoutParams() {
    var conn = new FakeConnection();
    conn.enqueue("SET search_path TO myschema");
    conn.enqueue("SET timeout TO 5");
    conn.enqueue("SELECT $1", 42);
    conn.enqueue("SELECT 99");

    var results = conn.flush();
    assertEquals(4, results.size());
    assertEquals(4, conn.extendedQueries.size());
  }

  @Test
  void enqueueReturnsCorrectIndices() {
    var conn = new FakeConnection();
    assertEquals(0, conn.enqueue("A"));
    assertEquals(1, conn.enqueue("B"));
    assertEquals(2, conn.enqueue("C", 1));
    assertEquals(3, conn.enqueue("D"));
    conn.flush();
  }

  @Test
  void flushClearsPendingQueue() {
    var conn = new FakeConnection();
    conn.enqueue("SELECT 1");
    conn.flush();
    // Second flush should be empty
    var results = conn.flush();
    assertTrue(results.isEmpty());
  }

  // ===== query() = enqueue + flush =====

  @Test
  void queryIsSingleStatementFlush() {
    var conn = new FakeConnection();
    conn.query("SELECT 1");
    assertEquals(1, conn.extendedQueries.size());
  }

  @Test
  void queryWithParamsUsesExtended() {
    var conn = new FakeConnection();
    conn.query("SELECT $1", 42);
    assertEquals(1, conn.extendedQueries.size());
  }

  @Test
  void queryFlushesAnyPriorEnqueued() {
    var conn = new FakeConnection();
    conn.enqueue("SET search_path TO myschema");
    conn.enqueue("SET timeout TO 5");
    // query() should flush the 2 pending + itself
    conn.query("SELECT 1");
    assertEquals(3, conn.extendedQueries.size());
  }

  @Test
  void queryThrowsOnError() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(new DbException("ERROR", "42P01", "table not found")));
    assertThrows(DbException.class, () -> conn.query("SELECT * FROM nope"));
  }

  // ===== SQL in exceptions =====

  @Test
  void queryErrorContainsSql() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(new DbException("ERROR", "42P01", "table not found")));
    var ex = assertThrows(DbException.class, () -> conn.query("SELECT * FROM nope"));
    assertEquals("SELECT * FROM nope", ex.sql());
  }

  @Test
  void queryWithParamsErrorContainsSql() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(new DbException("ERROR", "22P02", "invalid input")));
    var ex = assertThrows(DbException.class, () -> conn.query("SELECT $1::int", "not_a_number"));
    assertEquals("SELECT $1::int", ex.sql());
  }

  @Test
  void pipelineErrorContainsSql() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 0));
    conn.extendedResponses.add(new RowSet(new DbException("ERROR", "42P01", "table not found")));
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 0));

    conn.enqueue("SELECT 1");
    conn.enqueue("SELECT * FROM nonexistent");
    conn.enqueue("SELECT 3");
    var results = conn.flush();

    var error = results.get(1).getError();
    assertNotNull(error);
    assertEquals("SELECT * FROM nonexistent", error.sql());
  }

  @Test
  void pipelineErrorSqlAvailableOnRowSetAccess() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(new DbException("ERROR", "42P01", "table not found")));

    conn.enqueue("SELECT * FROM missing_table");
    var results = conn.flush();

    // When accessing rows on an error RowSet, the thrown exception should include the SQL
    var ex = assertThrows(DbException.class, () -> results.get(0).size());
    assertEquals("SELECT * FROM missing_table", ex.sql());
  }

  @Test
  void errorWithoutSqlReturnsNull() {
    var ex = new DbException("ERROR", "42P01", "table not found");
    assertNull(ex.sql());
  }

  // ===== Named parameters =====

  @Test
  void namedParamsRewrittenToPositional() {
    var conn = new FakeConnection();
    conn.enqueue("SELECT $1 WHERE name = :name", Map.of("name", "Alice"));
    conn.flush();
    assertEquals(1, conn.extendedQueries.size());
  }

  // ===== executeMany =====

  @Test
  void executeManyEnqueuesAll() {
    var conn = new FakeConnection();
    var results =
        conn.executeMany(
            "INSERT INTO t VALUES ($1)",
            List.of(new Object[] {"a"}, new Object[] {"b"}, new Object[] {"c"}));
    assertEquals(3, results.size());
    assertEquals(3, conn.extendedQueries.size());
  }

  @Test
  void executeManyEmpty() {
    var conn = new FakeConnection();
    var results = conn.executeMany("INSERT INTO t VALUES ($1)", List.of());
    assertTrue(results.isEmpty());
  }

  // ===== close =====

  @Test
  void closeSendsTerminateAndClosesTransport() {
    var conn = new FakeConnection();
    conn.close();
    assertTrue(conn.terminated);
    assertTrue(conn.transportClosed);
  }

  // ===== Pipeline ordering =====

  @Test
  void resultOrderMatchesEnqueueOrder() {
    var conn = new FakeConnection();
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 1));
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 2));
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 3));
    conn.extendedResponses.add(new RowSet(List.of(), List.of(), 4));

    conn.enqueue("A");
    conn.enqueue("B");
    conn.enqueue("C", 1);
    conn.enqueue("D");

    var results = conn.flush();
    assertEquals(4, results.size());
    assertEquals(1, results.get(0).rowsAffected());
    assertEquals(2, results.get(1).rowsAffected());
    assertEquals(3, results.get(2).rowsAffected());
    assertEquals(4, results.get(3).rowsAffected());
  }

  // ===== All queries go through extended protocol =====

  @Test
  void allQueriesUseExtendedProtocol() {
    var conn = new FakeConnection();
    conn.enqueue("S1");
    conn.enqueue("S2");
    conn.enqueue("E1", 1);
    conn.enqueue("S3");
    conn.enqueue("E2", 2);
    conn.enqueue("S4");
    conn.enqueue("S5");

    conn.flush();

    assertEquals(7, conn.extendedQueries.size());
  }

  // ===== BinderRegistry integration =====

  @Test
  void customBinder() {
    var conn = new FakeConnection();
    conn.binderRegistry().register(java.util.UUID.class, uuid -> uuid.toString().toUpperCase());
    var uuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    conn.enqueue("INSERT INTO t VALUES ($1)", uuid);
    conn.flush();
    assertEquals("550E8400-E29B-41D4-A716-446655440000", conn.extendedQueries.getFirst().params[0]);
  }

  @Test
  void nullParamEncodedAsNull() {
    var conn = new FakeConnection();
    conn.enqueue("INSERT INTO t VALUES ($1)", (Object) null);
    conn.flush();
    assertNull(conn.extendedQueries.getFirst().params[0]);
  }
}
