package io.djb.impl;

import io.djb.ColumnDescriptor;

import java.util.*;

import org.jspecify.annotations.Nullable;

/**
 * LRU cache for prepared statements, keyed by SQL string.
 * Based on vertx-sql-client's LruCache/PreparedStatementCache design.
 *
 * <p>Not thread-safe — each connection has its own cache instance.
 */
public class PreparedStatementCache extends LinkedHashMap<String, PreparedStatementCache.CachedStatement> {

    private final int capacity;
    private @Nullable List<CachedStatement> removed;

    public PreparedStatementCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    /**
     * Cache a statement. Returns the list of evicted statements that the caller
     * must close server-side.
     */
    public List<CachedStatement> cache(String sql, CachedStatement stmt) {
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
     * A cached prepared statement handle. Holds the server-side state needed
     * to re-execute without re-preparing.
     */
    public record CachedStatement(
        String sql,
        @Nullable String pgStatementName,   // PG: named statement, null for MySQL
        int mysqlStatementId,     // MySQL: server-assigned ID, -1 for PG
        ColumnDescriptor @Nullable [] columns,
        int @Nullable [] columnTypes         // MySQL binary column types, null for PG
    ) {}
}
