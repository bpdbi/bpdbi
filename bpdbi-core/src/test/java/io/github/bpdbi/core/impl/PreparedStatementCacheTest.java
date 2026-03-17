package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PreparedStatementCacheTest {

  private static PreparedStatementCache.CachedStatement stmt(String sql) {
    return new PreparedStatementCache.CachedStatement(sql, sql, -1, null, null);
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
  void replaceSameKey() {
    var cache = new PreparedStatementCache(2);
    cache.cache("a", stmt("a-v1"));
    cache.cache("a", stmt("a-v2"));
    assertEquals(1, cache.size());
    assertNotNull(cache.get("a"));
    assertEquals("a-v2", cache.get("a").sql());
  }
}
