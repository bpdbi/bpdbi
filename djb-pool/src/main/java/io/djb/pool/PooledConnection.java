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

/**
 * A wrapper around a real {@link Connection} that redirects {@link #close()} to return the
 * connection to the pool instead of closing it. This prevents connection slot loss when callers use
 * try-with-resources or call {@code close()} directly.
 *
 * <p>Also tracks creation time and acquisition time for pool lifecycle management
 * and leak detection.
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

  /**
   * Return the underlying real connection (for tests and pool internals).
   */
  Connection delegate() {
    return delegate;
  }

  /**
   * Close the underlying connection for real (used by pool eviction/shutdown).
   */
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
  public RowSet query(String sql) {
    return delegate.query(sql);
  }

  @Override
  public RowSet query(String sql, Object... params) {
    return delegate.query(sql, params);
  }

  @Override
  public RowSet query(String sql, Map<String, Object> params) {
    return delegate.query(sql, params);
  }

  @Override
  public int enqueue(String sql) {
    return delegate.enqueue(sql);
  }

  @Override
  public int enqueue(String sql, Object... params) {
    return delegate.enqueue(sql, params);
  }

  @Override
  public int enqueue(String sql, Map<String, Object> params) {
    return delegate.enqueue(sql, params);
  }

  @Override
  public List<RowSet> flush() {
    return delegate.flush();
  }

  @Override
  public List<RowSet> executeMany(
      String sql,
      List<Object[]> paramSets
  ) {
    return delegate.executeMany(sql, paramSets);
  }

  @Override
  public PreparedStatement prepare(String sql) {
    return delegate.prepare(sql);
  }

  @Override
  public Cursor cursor(String sql, Object... params) {
    return delegate.cursor(sql, params);
  }

  @Override
  public Transaction begin() {
    return delegate.begin();
  }

  @Override
  public void ping() {
    delegate.ping();
  }

  @Override
  public Map<String, String> parameters() {
    return delegate.parameters();
  }

  @Override
  public void queryStream(String sql, Consumer<Row> consumer) {
    delegate.queryStream(sql, consumer);
  }

  @Override
  public void queryStream(
      String sql,
      Consumer<Row> consumer,
      Object... params
  ) {
    delegate.queryStream(sql, consumer, params);
  }

  @Override
  public RowStream stream(String sql, Object... params) {
    return delegate.stream(sql, params);
  }

  @Override
  public BinderRegistry binderRegistry() {
    return delegate.binderRegistry();
  }

  @Override
  public void setBinderRegistry(BinderRegistry registry) {
    delegate.setBinderRegistry(registry);
  }

  @Override
  public ColumnMapperRegistry mapperRegistry() {
    return delegate.mapperRegistry();
  }

  @Override
  public void setColumnMapperRegistry(ColumnMapperRegistry registry) {
    delegate.setColumnMapperRegistry(registry);
  }

  @Override
  public JsonMapper jsonMapper() {
    return delegate.jsonMapper();
  }

  @Override
  public void setJsonMapper(JsonMapper mapper) {
    delegate.setJsonMapper(mapper);
  }
}
