package io.github.bpdbi.pool;

import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.JsonMapper;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import io.github.bpdbi.core.SqlBuilder;
import io.github.bpdbi.core.Transaction;
import io.github.bpdbi.core.TypeRegistry;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A wrapper around a real {@link Connection} that redirects {@link #close()} to return the
 * connection to the pool instead of closing it. This prevents connection slot loss when callers use
 * try-with-resources or call {@code close()} directly.
 *
 * <p>Also tracks creation time and acquisition time for pool lifecycle management and leak
 * detection.
 */
final class PooledConnection implements Connection {

  private final Connection delegate;
  private final ConnectionPool pool;
  final long createdAtNanos;
  volatile long acquiredAtNanos;
  volatile long returnedAtNanos;

  /**
   * Per-connection max lifetime (in nanos) with random variance applied. Prevents all connections
   * created around the same time from expiring simultaneously (thundering herd). A value of 0 means
   * no lifetime limit.
   */
  final long effectiveMaxLifetimeNanos;

  PooledConnection(
      Connection delegate,
      ConnectionPool pool,
      long createdAtNanos,
      long effectiveMaxLifetimeNanos) {
    this.delegate = delegate;
    this.pool = pool;
    this.createdAtNanos = createdAtNanos;
    this.effectiveMaxLifetimeNanos = effectiveMaxLifetimeNanos;
  }

  /** Return the underlying real connection (for tests and pool internals). */
  Connection delegate() {
    return delegate;
  }

  /** Close the underlying connection for real (used by pool eviction/shutdown). */
  void closeDelegate() {
    try {
      delegate.close();
    } catch (Exception e) {
      // best effort
    }
  }

  /**
   * Returns this connection to the pool instead of closing it. If the pool is closed, the
   * underlying connection is closed for real.
   */
  @Override
  public void close() {
    pool.release(this);
  }

  // --- Delegation: all Connection methods forward to the underlying connection ---

  @Override
  public @NonNull RowSet query(@NonNull String sql) {
    return delegate.query(sql);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @Nullable Object... params) {
    return delegate.query(sql, params);
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
    return delegate.query(sql, params);
  }

  @Override
  public int enqueue(@NonNull String sql) {
    return delegate.enqueue(sql);
  }

  @Override
  public int enqueue(@NonNull String sql, @Nullable Object... params) {
    return delegate.enqueue(sql, params);
  }

  @Override
  public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
    return delegate.enqueue(sql, params);
  }

  @Override
  public @NonNull List<RowSet> flush() {
    return delegate.flush();
  }

  @Override
  public @NonNull List<RowSet> executeMany(@NonNull String sql, @NonNull List<Object[]> paramSets) {
    return delegate.executeMany(sql, paramSets);
  }

  @Override
  public @NonNull List<RowSet> executeManyNamed(
      @NonNull String sql, @NonNull List<Map<String, Object>> paramSets) {
    return delegate.executeManyNamed(sql, paramSets);
  }

  @Override
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    return delegate.prepare(sql);
  }

  @Override
  public @NonNull Cursor cursor(@NonNull String sql, @Nullable Object... params) {
    return delegate.cursor(sql, params);
  }

  /**
   * Delegates to the real connection so that the Transaction wraps the delegate, not this
   * PooledConnection. This is critical for useTransaction()/inTransaction() correctness: the
   * default interface method calls this.begin(), so if we didn't delegate, the Transaction would
   * hold a reference to the PooledConnection and tx.close() could return it to the pool
   * mid-transaction.
   */
  @Override
  public @NonNull Transaction begin() {
    return delegate.begin();
  }

  @Override
  public void ping() {
    delegate.ping();
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    return delegate.parameters();
  }

  @Override
  public void queryStream(@NonNull String sql, @NonNull Consumer<Row> consumer) {
    delegate.queryStream(sql, consumer);
  }

  @Override
  public void queryStream(
      @NonNull String sql, @NonNull Object[] params, @NonNull Consumer<Row> consumer) {
    delegate.queryStream(sql, params, consumer);
  }

  @Override
  public @NonNull RowStream stream(@NonNull String sql, @Nullable Object... params) {
    return delegate.stream(sql, params);
  }

  @Override
  public @NonNull SqlBuilder sql(@NonNull String sql) {
    return delegate.sql(sql);
  }

  @Override
  public @NonNull TypeRegistry typeRegistry() {
    return delegate.typeRegistry();
  }

  @Override
  public void setTypeRegistry(@NonNull TypeRegistry registry) {
    delegate.setTypeRegistry(registry);
  }

  @Override
  public @Nullable JsonMapper jsonMapper() {
    return delegate.jsonMapper();
  }

  @Override
  public void setJsonMapper(@Nullable JsonMapper mapper) {
    delegate.setJsonMapper(mapper);
  }
}
