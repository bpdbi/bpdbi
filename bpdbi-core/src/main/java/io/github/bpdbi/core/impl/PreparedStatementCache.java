package io.github.bpdbi.core.impl;

import io.github.bpdbi.core.ColumnDescriptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * LRU cache for prepared statements, keyed by SQL string. Based on vertx-sql-client's
 * LruCache/PreparedStatementCache design.
 *
 * <p>Not thread-safe — each connection has its own cache instance.
 */
@SuppressWarnings("serial")
public class PreparedStatementCache
    extends LinkedHashMap<String, PreparedStatementCache.CachedStatement> {

  private final int capacity;
  private List<CachedStatement> removed;

  public PreparedStatementCache(int capacity) {
    super(capacity, 0.75f, true);
    this.capacity = capacity;
  }

  /**
   * Cache a statement. Returns the list of evicted statements that the caller must close
   * server-side.
   */
  public @NonNull List<CachedStatement> cache(@NonNull String sql, @NonNull CachedStatement stmt) {
    put(sql, stmt);
    if (removed != null) {
      List<CachedStatement> evicted = removed;
      removed = null;
      return evicted;
    }
    return Collections.emptyList();
  }

  /**
   * Evict the least recently used entry.
   *
   * @return the evicted statement, or null if empty
   */
  public @Nullable CachedStatement evict() {
    Iterator<CachedStatement> it = values().iterator();
    if (it.hasNext()) {
      CachedStatement value = it.next();
      it.remove();
      return value;
    }
    return null;
  }

  public boolean isFull() {
    return size() >= capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<String, CachedStatement> eldest) {
    boolean evict = size() > capacity;
    if (evict) {
      if (removed == null) {
        removed = new ArrayList<>();
      }
      removed.add(eldest.getValue());
      return true;
    }
    return false;
  }

  @Override
  public void clear() {
    super.clear();
    removed = null;
  }

  /**
   * A cached prepared statement handle. Holds the server-side state needed to re-execute without
   * re-preparing.
   */
  @SuppressWarnings("ArrayRecordComponent")
  // internal record; nullable arrays are intentional (null = not applicable for this DB)
  public record CachedStatement(
      @NonNull String sql,
      @Nullable String pgStatementName, // PG: named statement, null for MySQL
      int mysqlStatementId, // MySQL: server-assigned ID, -1 for PG
      ColumnDescriptor @Nullable [] columns,
      int @Nullable [] columnTypes, // MySQL binary column types, null for PG
      @Nullable List<String> parameterNames // named param ordering, null if positional
      ) {

    /** Convenience constructor without parameterNames (positional params only). */
    public CachedStatement(
        @NonNull String sql,
        @Nullable String pgStatementName,
        int mysqlStatementId,
        ColumnDescriptor @Nullable [] columns,
        int @Nullable [] columnTypes) {
      this(sql, pgStatementName, mysqlStatementId, columns, columnTypes, null);
    }
  }
}
