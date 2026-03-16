package io.djb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A database transaction with auto-rollback support. Implements {@link Connection} so it can be
 * passed to any code expecting a connection. Use with try-with-resources for automatic rollback on
 * failure.
 *
 * <pre>{@code
 * try (var tx = conn.begin()) {
 *     tx.query("INSERT INTO t VALUES (1)");
 *     tx.query("INSERT INTO t VALUES (2)");
 *     tx.commit();
 * } // auto-rollback if commit() was not called
 * }</pre>
 */
public final class Transaction implements Connection {

  private final Connection conn;
  private final String savepointName;  // null = root transaction, non-null = nested (savepoint)
  private final AtomicInteger savepointCounter;
  private boolean finished = false;

  /**
   * Create a root transaction (sends BEGIN).
   */
  public Transaction(Connection conn) {
    this.conn = conn;
    this.savepointName = null;
    this.savepointCounter = new AtomicInteger(0);
    conn.query("BEGIN");
  }

  /**
   * Create a nested transaction (sends SAVEPOINT).
   */
  private Transaction(
      Connection conn, String savepointName,
      AtomicInteger savepointCounter
  ) {
    this.conn = conn;
    this.savepointName = savepointName;
    this.savepointCounter = savepointCounter;
    conn.query("SAVEPOINT " + savepointName);
  }

  private boolean isNested() {
    return savepointName != null;
  }

  @Override
  public RowSet query(String sql) {
    checkNotFinished();
    return conn.query(sql);
  }

  @Override
  public RowSet query(String sql, Object... params) {
    checkNotFinished();
    return conn.query(sql, params);
  }

  @Override
  public RowSet query(String sql, Map<String, Object> params) {
    checkNotFinished();
    return conn.query(sql, params);
  }

  @Override
  public int enqueue(String sql) {
    checkNotFinished();
    return conn.enqueue(sql);
  }

  @Override
  public int enqueue(String sql, Object... params) {
    checkNotFinished();
    return conn.enqueue(sql, params);
  }

  @Override
  public int enqueue(String sql, Map<String, Object> params) {
    checkNotFinished();
    return conn.enqueue(sql, params);
  }

  @Override
  public List<RowSet> flush() {
    checkNotFinished();
    return conn.flush();
  }

  @Override
  public PreparedStatement prepare(String sql) {
    checkNotFinished();
    return conn.prepare(sql);
  }

  @Override
  public Cursor cursor(String sql, Object... params) {
    checkNotFinished();
    return conn.cursor(sql, params);
  }

  /**
   * Begin a nested transaction backed by a savepoint. The savepoint is automatically rolled back on
   * close if not committed.
   */
  @Override
  public Transaction begin() {
    checkNotFinished();
    String sp = "_djb_sp_" + savepointCounter.getAndIncrement();
    return new Transaction(conn, sp, savepointCounter);
  }

  @Override
  public List<RowSet> executeMany(
      String sql,
      List<Object[]> paramSets
  ) {
    checkNotFinished();
    return conn.executeMany(sql, paramSets);
  }

  @Override
  public void ping() {
    checkNotFinished();
    conn.ping();
  }

  @Override
  public Map<String, String> parameters() {
    checkNotFinished();
    return conn.parameters();
  }

  @Override
  public void queryStream(String sql, Consumer<Row> consumer) {
    checkNotFinished();
    conn.queryStream(sql, consumer);
  }

  @Override
  public void queryStream(
      String sql,
      Consumer<Row> consumer,
      Object... params
  ) {
    checkNotFinished();
    conn.queryStream(sql, consumer, params);
  }

  @Override
  public RowStream stream(String sql, Object... params) {
    checkNotFinished();
    return conn.stream(sql, params);
  }

  @Override
  public BinderRegistry binderRegistry() {
    return conn.binderRegistry();
  }

  @Override
  public void setBinderRegistry(BinderRegistry registry) {
    conn.setBinderRegistry(registry);
  }

  @Override
  public ColumnMapperRegistry mapperRegistry() {
    return conn.mapperRegistry();
  }

  @Override
  public void setColumnMapperRegistry(ColumnMapperRegistry registry) {
    conn.setColumnMapperRegistry(registry);
  }

  @Override
  public JsonMapper jsonMapper() {
    return conn.jsonMapper();
  }

  @Override
  public void setJsonMapper(JsonMapper mapper) {
    conn.setJsonMapper(mapper);
  }

  /**
   * Commit this transaction. For a root transaction, sends COMMIT. For a nested transaction, sends
   * RELEASE SAVEPOINT.
   */
  public void commit() {
    checkNotFinished();
    if (isNested()) {
      conn.query("RELEASE SAVEPOINT " + savepointName);
    } else {
      conn.query("COMMIT");
    }
    finished = true;
  }

  /**
   * Roll back this transaction. For a root transaction, sends ROLLBACK. For a nested transaction,
   * sends ROLLBACK TO SAVEPOINT.
   */
  public void rollback() {
    checkNotFinished();
    if (isNested()) {
      conn.query("ROLLBACK TO SAVEPOINT " + savepointName);
    } else {
      conn.query("ROLLBACK");
    }
    finished = true;
  }

  @Override
  public void close() {
    if (!finished) {
      try {
        if (isNested()) {
          conn.query("ROLLBACK TO SAVEPOINT " + savepointName);
        } else {
          conn.query("ROLLBACK");
        }
      } catch (Exception e) {
        // best effort — connection may already be broken
      }
      finished = true;
    }
  }

  private void checkNotFinished() {
    if (finished) {
      throw new IllegalStateException("Transaction already committed or rolled back");
    }
  }
}
