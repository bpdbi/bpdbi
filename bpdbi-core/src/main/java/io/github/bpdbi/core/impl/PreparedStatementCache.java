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
  private final int maxTotalSqlBytes;
  private int totalSqlBytes;
  private List<CachedStatement> removed;

  public PreparedStatementCache(int capacity) {
    this(capacity, 0);
  }

  /**
   * Create a cache with entry count and total SQL byte limits.
   *
   * @param capacity max number of cached statements
   * @param maxTotalSqlBytes max total SQL bytes across all entries; 0 = no byte limit
   */
  public PreparedStatementCache(int capacity, int maxTotalSqlBytes) {
    super(capacity, 0.75f, true);
    this.capacity = capacity;
    this.maxTotalSqlBytes = maxTotalSqlBytes;
  }

  /**
   * Check if a SQL string should be rejected from caching because it would consume more than half
   * the total byte budget. This prevents one huge statement from evicting many smaller ones.
   */
  public boolean isOversized(@NonNull String sql) {
    return maxTotalSqlBytes > 0 && sql.length() * 2 > maxTotalSqlBytes;
  }

  /** Current total SQL bytes stored in the cache. */
  public int totalSqlBytes() {
    return totalSqlBytes;
  }

  /**
   * Cache a statement. Returns the list of evicted statements that the caller must close
   * server-side. Returns a list containing only the given statement itself (unconsumed) if the SQL
   * is rejected as oversized.
   */
  public @NonNull List<CachedStatement> cache(@NonNull String sql, @NonNull CachedStatement stmt) {
    if (isOversized(sql)) {
      return List.of(stmt);
    }
    put(sql, stmt);
    totalSqlBytes += sql.length();
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
    Iterator<Map.Entry<String, CachedStatement>> it = entrySet().iterator();
    if (it.hasNext()) {
      Map.Entry<String, CachedStatement> entry = it.next();
      it.remove();
      totalSqlBytes -= entry.getKey().length();
      return entry.getValue();
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
      totalSqlBytes -= eldest.getKey().length();
      return true;
    }
    return false;
  }

  @Override
  public CachedStatement remove(Object key) {
    CachedStatement removed = super.remove(key);
    if (removed != null && key instanceof String sql) {
      totalSqlBytes -= sql.length();
    }
    return removed;
  }

  @Override
  public void clear() {
    super.clear();
    removed = null;
    totalSqlBytes = 0;
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
      byte @Nullable [] pgStatementNameCString, // pre-encoded null-terminated C string for the wire
      int mysqlStatementId, // MySQL: server-assigned ID, -1 for PG
      ColumnDescriptor @Nullable [] columns,
      int @Nullable [] columnTypes, // MySQL binary column types, null for PG
      @Nullable List<String> parameterNames // named param ordering, null if positional
      ) {

    /** Convenience constructor without parameterNames (positional params only). */
    public CachedStatement(
        @NonNull String sql,
        @Nullable String pgStatementName,
        byte @Nullable [] pgStatementNameCString,
        int mysqlStatementId,
        ColumnDescriptor @Nullable [] columns,
        int @Nullable [] columnTypes) {
      this(
          sql,
          pgStatementName,
          pgStatementNameCString,
          mysqlStatementId,
          columns,
          columnTypes,
          null);
    }
  }
}
