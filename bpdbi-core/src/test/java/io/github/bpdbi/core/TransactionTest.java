package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpdbi.core.test.AbstractStubConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Transaction using a recording Connection stub. No database required. Verifies SQL
 * delegation and BEGIN/COMMIT/ROLLBACK/SAVEPOINT behavior.
 */
class TransactionTest {

  /** Records all SQL passed to query() and enqueue() for verification. */
  static class RecordingConnection extends AbstractStubConnection {

    final List<String> queries = new ArrayList<>();

    @Override
    public @NonNull RowSet query(@NonNull String sql) {
      queries.add(sql);
      return new RowSet(List.of(), List.of(), 0);
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql, Object... params) {
      queries.add(sql);
      return new RowSet(List.of(), List.of(), 0);
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
      queries.add(sql);
      return new RowSet(List.of(), List.of(), 0);
    }

    @Override
    public int enqueue(@NonNull String sql) {
      queries.add("enqueue:" + sql);
      return 0;
    }

    @Override
    public int enqueue(@NonNull String sql, Object... params) {
      queries.add("enqueue:" + sql);
      return 0;
    }

    @Override
    public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
      queries.add("enqueue:" + sql);
      return 0;
    }

    @Override
    public @NonNull List<RowSet> flush() {
      queries.add("flush");
      return List.of();
    }

    @Override
    public @NonNull List<RowSet> executeMany(
        @NonNull String sql, @NonNull List<Object[]> paramSets) {
      queries.add("executeMany:" + sql);
      return List.of();
    }

    @Override
    public void ping() {
      queries.add("ping");
    }
  }

  // ===== Root transaction lifecycle =====

  @Test
  void constructorEnqueuesBEGIN() {
    var conn = new RecordingConnection();
    new Transaction(conn);
    // BEGIN is enqueued (not flushed) — it pipelines with the first actual query
    assertEquals(List.of("enqueue:BEGIN"), conn.queries);
  }

  @Test
  void beginPipelinesWithFirstQuery() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    assertEquals(1, conn.queries.size());
    // First query triggers flush; BEGIN + SELECT go in one batch
    tx.query("SELECT 1");
    assertEquals("enqueue:BEGIN", conn.queries.get(0));
    assertEquals("SELECT 1", conn.queries.get(1));
  }

  @Test
  void commitSendsCOMMIT() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.commit();
    assertEquals(List.of("enqueue:BEGIN", "COMMIT"), conn.queries);
  }

  @Test
  void rollbackSendsROLLBACK() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.rollback();
    assertEquals(List.of("enqueue:BEGIN", "ROLLBACK"), conn.queries);
  }

  @Test
  void closeWithoutCommitAutoRollback() {
    var conn = new RecordingConnection();
    try (var tx = new Transaction(conn)) {
      tx.query("INSERT INTO t VALUES (1)");
    }
    assertEquals(List.of("enqueue:BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK"), conn.queries);
  }

  @Test
  void closeAfterCommitDoesNotRollback() {
    var conn = new RecordingConnection();
    try (var tx = new Transaction(conn)) {
      tx.commit();
    }
    assertEquals(List.of("enqueue:BEGIN", "COMMIT"), conn.queries);
  }

  // ===== Nested transactions (savepoints) =====

  @Test
  void nestedTransactionEnqueuesSavepoint() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.begin();
    assertEquals("enqueue:SAVEPOINT _bpdbi_sp_0", conn.queries.get(1));
  }

  @Test
  void nestedCommitReleaseSavepoint() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    var nested = tx.begin();
    nested.commit();
    assertEquals("RELEASE SAVEPOINT _bpdbi_sp_0", conn.queries.get(2));
  }

  @Test
  void nestedRollbackRollsBackToSavepoint() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    var nested = tx.begin();
    nested.rollback();
    assertEquals("ROLLBACK TO SAVEPOINT _bpdbi_sp_0", conn.queries.get(2));
  }

  @Test
  void nestedCloseWithoutCommitAutoRollback() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    try (var nested = tx.begin()) {
      nested.query("INSERT INTO t VALUES (1)");
    }
    assertEquals("ROLLBACK TO SAVEPOINT _bpdbi_sp_0", conn.queries.getLast());
  }

  @Test
  void multipleNestedTransactionsIncrementCounter() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    var n1 = tx.begin();
    n1.commit();
    var n2 = tx.begin();
    n2.commit();
    assertEquals("enqueue:SAVEPOINT _bpdbi_sp_0", conn.queries.get(1));
    assertEquals("enqueue:SAVEPOINT _bpdbi_sp_1", conn.queries.get(3));
  }

  // ===== Operations after finish throw =====

  @Test
  void queryAfterCommitThrows() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.commit();
    assertThrows(IllegalStateException.class, () -> tx.query("SELECT 1"));
  }

  @Test
  void queryAfterRollbackThrows() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.rollback();
    assertThrows(IllegalStateException.class, () -> tx.query("SELECT 1"));
  }

  // ===== Close swallows exceptions =====

  @SuppressWarnings("try") // tx intentionally unused — testing auto-close behavior
  @Test
  void closeSwallowsExceptionOnBrokenConnection() {
    var conn =
        new RecordingConnection() {
          @Override
          public @NonNull RowSet query(@NonNull String sql) {
            if (sql.startsWith("ROLLBACK")) {
              throw new DbConnectionException("connection lost");
            }
            return super.query(sql);
          }
        };
    try (var tx = new Transaction(conn)) {
      // no commit → close will attempt ROLLBACK → exception swallowed
    }
    // no exception propagated
  }

  // ===== Delegation =====

  @Test
  void delegatesMethods() {
    var conn = new RecordingConnection();
    var tx = new Transaction(conn);
    tx.enqueue("A");
    tx.enqueue("B", 1);
    tx.enqueue("C", Map.of("k", "v"));
    tx.flush();
    tx.ping();
    tx.executeMany("D", List.of());

    assertEquals("enqueue:A", conn.queries.get(1));
    assertEquals("enqueue:B", conn.queries.get(2));
    assertEquals("enqueue:C", conn.queries.get(3));
    assertEquals("flush", conn.queries.get(4));
    assertEquals("ping", conn.queries.get(5));
    assertEquals("executeMany:D", conn.queries.get(6));
  }
}
