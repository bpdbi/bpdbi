package io.github.bpdbi.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
  private final String savepointName; // null = root transaction, non-null = nested (savepoint)
  private final AtomicInteger savepointCounter;
  private boolean finished = false;

  /** Create a root transaction (sends BEGIN). */
  public Transaction(@NonNull Connection conn) {
    this.conn = conn;
    this.savepointName = null;
    this.savepointCounter = new AtomicInteger(0);
    conn.query("BEGIN");
  }

  /** Create a nested transaction (sends SAVEPOINT). */
  private Transaction(Connection conn, String savepointName, AtomicInteger savepointCounter) {
    this.conn = conn;
    this.savepointName = savepointName;
    this.savepointCounter = savepointCounter;
    conn.query("SAVEPOINT " + savepointName);
  }

  private boolean isNested() {
    return savepointName != null;
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql) {
    checkNotFinished();
    return conn.query(sql);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @Nullable Object... params) {
    checkNotFinished();
    return conn.query(sql, params);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
    checkNotFinished();
    return conn.query(sql, params);
  }

  @Override
  public int enqueue(@NonNull String sql) {
    checkNotFinished();
    return conn.enqueue(sql);
  }

  @Override
  public int enqueue(@NonNull String sql, @Nullable Object... params) {
    checkNotFinished();
    return conn.enqueue(sql, params);
  }

  @Override
  public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
    checkNotFinished();
    return conn.enqueue(sql, params);
  }

  @Override
  public @NonNull List<RowSet> flush() {
    checkNotFinished();
    return conn.flush();
  }

  @Override
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    checkNotFinished();
    return conn.prepare(sql);
  }

  @Override
  public @NonNull Cursor cursor(@NonNull String sql, @Nullable Object... params) {
    checkNotFinished();
    return conn.cursor(sql, params);
  }

  /**
   * Begin a nested transaction backed by a savepoint. The savepoint is automatically rolled back on
   * close if not committed.
   */
  @Override
  public @NonNull Transaction begin() {
    checkNotFinished();
    String sp = "_djb_sp_" + savepointCounter.getAndIncrement();
    return new Transaction(conn, sp, savepointCounter);
  }

  @Override
  public @NonNull List<RowSet> executeMany(@NonNull String sql, @NonNull List<Object[]> paramSets) {
    checkNotFinished();
    return conn.executeMany(sql, paramSets);
  }

  @Override
  public void ping() {
    checkNotFinished();
    conn.ping();
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    checkNotFinished();
    return conn.parameters();
  }

  @Override
  public void queryStream(@NonNull String sql, @NonNull Consumer<Row> consumer) {
    checkNotFinished();
    conn.queryStream(sql, consumer);
  }

  @Override
  public void queryStream(
      @NonNull String sql, @NonNull Consumer<Row> consumer, @Nullable Object... params) {
    checkNotFinished();
    conn.queryStream(sql, consumer, params);
  }

  @Override
  public @NonNull RowStream stream(@NonNull String sql, @Nullable Object... params) {
    checkNotFinished();
    return conn.stream(sql, params);
  }

  @Override
  public @NonNull BinderRegistry binderRegistry() {
    return conn.binderRegistry();
  }

  @Override
  public void setBinderRegistry(@NonNull BinderRegistry registry) {
    conn.setBinderRegistry(registry);
  }

  @Override
  public @NonNull ColumnMapperRegistry mapperRegistry() {
    return conn.mapperRegistry();
  }

  @Override
  public void setColumnMapperRegistry(@NonNull ColumnMapperRegistry registry) {
    conn.setColumnMapperRegistry(registry);
  }

  @Override
  public @Nullable JsonMapper jsonMapper() {
    return conn.jsonMapper();
  }

  @Override
  public void setJsonMapper(@Nullable JsonMapper mapper) {
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
