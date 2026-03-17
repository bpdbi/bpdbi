package io.djb.pool;

import io.djb.BinderRegistry;
import io.djb.ColumnMapperRegistry;
import io.djb.Connection;
import io.djb.Cursor;
import io.djb.JsonMapper;
import io.djb.PreparedStatement;
import io.djb.Row;
import io.djb.RowSet;
import io.djb.RowStream;
import io.djb.Transaction;
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
  final long createdAt;
  volatile long acquiredAt;
  volatile long returnedAt;

  PooledConnection(Connection delegate, ConnectionPool pool, long createdAt) {
    this.delegate = delegate;
    this.pool = pool;
    this.createdAt = createdAt;
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

  // --- Delegation ---

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
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    return delegate.prepare(sql);
  }

  @Override
  public @NonNull Cursor cursor(@NonNull String sql, @Nullable Object... params) {
    return delegate.cursor(sql, params);
  }

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
  public void queryStream(@NonNull String sql, @NonNull Consumer<Row> consumer, @Nullable Object... params) {
    delegate.queryStream(sql, consumer, params);
  }

  @Override
  public @NonNull RowStream stream(@NonNull String sql, @Nullable Object... params) {
    return delegate.stream(sql, params);
  }

  @Override
  public @NonNull BinderRegistry binderRegistry() {
    return delegate.binderRegistry();
  }

  @Override
  public void setBinderRegistry(@NonNull BinderRegistry registry) {
    delegate.setBinderRegistry(registry);
  }

  @Override
  public @NonNull ColumnMapperRegistry mapperRegistry() {
    return delegate.mapperRegistry();
  }

  @Override
  public void setColumnMapperRegistry(@NonNull ColumnMapperRegistry registry) {
    delegate.setColumnMapperRegistry(registry);
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
