package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PreparedStatementCacheTest {

  private static PreparedStatementCache.CachedStatement stmt(String sql) {
    return new PreparedStatementCache.CachedStatement(sql, sql, null, -1, null, null);
  }

  @Test
  void cacheAndRetrieve() {
    var cache = new PreparedStatementCache(10);
    cache.cache("SELECT 1", stmt("SELECT 1"));
    assertEquals(1, cache.size());
    assertNotNull(cache.get("SELECT 1"));
    assertEquals("SELECT 1", cache.get("SELECT 1").sql());
  }

  @Test
  void cacheHitUpdatesAccessOrder() {
    var cache = new PreparedStatementCache(3);
    cache.cache("a", stmt("a"));
    cache.cache("b", stmt("b"));
    cache.cache("c", stmt("c"));

    // Access "a" to move it to most-recently-used
    cache.get("a");

    // Adding "d" should evict "b" (least recently used), not "a"
    var evicted = cache.cache("d", stmt("d"));
    assertEquals(1, evicted.size());
    assertEquals("b", evicted.get(0).sql());
    assertNotNull(cache.get("a"));
    assertNull(cache.get("b"));
  }

  @Test
  void evictsWhenCapacityExceeded() {
    var cache = new PreparedStatementCache(2);
    cache.cache("a", stmt("a"));
    cache.cache("b", stmt("b"));

    var evicted = cache.cache("c", stmt("c"));
    assertEquals(1, evicted.size());
    assertEquals("a", evicted.get(0).sql());
    assertEquals(2, cache.size());
  }

  @Test
  void cacheReturnsEmptyListWhenNoEviction() {
    var cache = new PreparedStatementCache(10);
    var evicted = cache.cache("a", stmt("a"));
    assertTrue(evicted.isEmpty());
  }

  @Test
  void evictRemovesLeastRecentlyUsed() {
    var cache = new PreparedStatementCache(10);
    cache.cache("a", stmt("a"));
    cache.cache("b", stmt("b"));

    var evicted = cache.evict();
    assertNotNull(evicted);
    assertEquals("a", evicted.sql());
    assertEquals(1, cache.size());
  }

  @Test
  void evictEmptyCacheReturnsNull() {
    var cache = new PreparedStatementCache(10);
    assertNull(cache.evict());
  }

  @Test
  void isFull() {
    var cache = new PreparedStatementCache(2);
    assertFalse(cache.isFull());
    cache.cache("a", stmt("a"));
    assertFalse(cache.isFull());
    cache.cache("b", stmt("b"));
    assertTrue(cache.isFull());
  }

  @Test
  void clearResetsCache() {
    var cache = new PreparedStatementCache(10);
    cache.cache("a", stmt("a"));
    cache.cache("b", stmt("b"));
    assertEquals(2, cache.size());

    cache.clear();
    assertEquals(0, cache.size());
    assertNull(cache.get("a"));
  }

  @Test
  void capacityOfOne() {
    var cache = new PreparedStatementCache(1);
    cache.cache("a", stmt("a"));
    assertTrue(cache.isFull());

    var evicted = cache.cache("b", stmt("b"));
    assertEquals(1, evicted.size());
    assertEquals("a", evicted.get(0).sql());
    assertEquals(1, cache.size());
    assertNotNull(cache.get("b"));
  }

  @Test
  void multipleEvictionsInSequence() {
    var cache = new PreparedStatementCache(2);
    cache.cache("a", stmt("a"));
    cache.cache("b", stmt("b"));

    var e1 = cache.evict();
    assertNotNull(e1);
    assertEquals("a", e1.sql());
    var e2 = cache.evict();
    assertNotNull(e2);
    assertEquals("b", e2.sql());
    assertNull(cache.evict());
  }

  @Test
  void oversizedSqlIsRejected() {
    // maxTotalSqlBytes = 100; SQL > 50 chars is rejected (> 50% of budget)
    var cache = new PreparedStatementCache(10, 100);
    var longSql = "SELECT * FROM very_long_table_name WHERE a = $1 AND b = $2 AND c = $3";
    assertTrue(longSql.length() > 50);

    var rejected = cache.cache(longSql, stmt(longSql));
    // Rejected: returned list contains the statement itself
    assertEquals(1, rejected.size());
    assertEquals(longSql, rejected.get(0).sql());
    // Cache is still empty
    assertEquals(0, cache.size());
    assertTrue(cache.isOversized(longSql));
  }

  @Test
  void normalSqlNotOversized() {
    var cache = new PreparedStatementCache(10, 100);
    var shortSql = "SELECT 1";
    assertFalse(cache.isOversized(shortSql));

    var evicted = cache.cache(shortSql, stmt(shortSql));
    assertTrue(evicted.isEmpty());
    assertEquals(1, cache.size());
    assertEquals(shortSql.length(), cache.totalSqlBytes());
  }

  @Test
  void totalSqlBytesTracksEvictions() {
    var cache = new PreparedStatementCache(2, 1000);
    cache.cache("SELECT 1", stmt("SELECT 1")); // 8 bytes
    cache.cache("SELECT 2", stmt("SELECT 2")); // 8 bytes
    assertEquals(16, cache.totalSqlBytes());

    // Adding a third evicts the first
    cache.cache("SELECT 3", stmt("SELECT 3"));
    assertEquals(2, cache.size());
    assertEquals(16, cache.totalSqlBytes()); // 8 evicted, 8 added
  }

  @Test
  void noByteLimit() {
    // maxTotalSqlBytes = 0 means no byte limit
    var cache = new PreparedStatementCache(10, 0);
    var longSql = "x".repeat(10000);
    assertFalse(cache.isOversized(longSql));
  }

  @Test
  void replaceSameKey() {
    var cache = new PreparedStatementCache(2);
    cache.cache("a", stmt("a-v1"));
    cache.cache("a", stmt("a-v2"));
    assertEquals(1, cache.size());
    assertNotNull(cache.get("a"));
    assertEquals("a-v2", cache.get("a").sql());
  }
}
