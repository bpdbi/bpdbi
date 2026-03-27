package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.RowSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Pipeline, batch execution, and pipeline error handling tests for the Postgres driver. */
class PgConnectionPipelineTest extends PgTestBase {

  // ===== Pipelined batch execution =====

  @Test
  void pipelinedBatchInserts() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_batch (id int, val text)");

      conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", 1, "a");
      conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", 2, "b");
      conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", 3, "c");
      conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", 4, "d");
      conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", 5, "e");
      List<RowSet> results = conn.flush();

      assertEquals(5, results.size());
      for (var rs : results) {
        assertNull(rs.getError());
        assertEquals(1, rs.rowsAffected());
      }

      var count = conn.query("SELECT count(*) FROM pipe_batch").first().getLong(0);
      assertEquals(5L, count);
    }
  }

  @Test
  void pipelinedBatchSelects() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_sel (id int, val text)");
      conn.query("INSERT INTO pipe_sel VALUES (1, 'one'), (2, 'two'), (3, 'three')");

      conn.enqueue("SELECT val FROM pipe_sel WHERE id = $1", 1);
      conn.enqueue("SELECT val FROM pipe_sel WHERE id = $1", 2);
      conn.enqueue("SELECT val FROM pipe_sel WHERE id = $1", 3);
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertEquals("one", results.get(0).first().getString(0));
      assertEquals("two", results.get(1).first().getString(0));
      assertEquals("three", results.get(2).first().getString(0));
    }
  }

  @Test
  void pipelinedBatchMixedSql() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_mix (id int, val text)");

      conn.enqueue("INSERT INTO pipe_mix VALUES ($1, $2)", 1, "a");
      conn.enqueue("SELECT $1::text", "hello");
      conn.enqueue("INSERT INTO pipe_mix VALUES ($1, $2)", 2, "b");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertNull(results.get(0).getError());
      assertEquals("hello", results.get(1).first().getString(0));
      assertNull(results.get(2).getError());
    }
  }

  @Test
  void pipelinedBatchErrorMidPipeline() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_err (id int PRIMARY KEY, val text)");
      conn.query("INSERT INTO pipe_err VALUES (1, 'existing')");

      conn.enqueue("INSERT INTO pipe_err VALUES ($1, $2)", 10, "ok");
      conn.enqueue("INSERT INTO pipe_err VALUES ($1, $2)", 1, "duplicate"); // PK violation
      conn.enqueue("INSERT INTO pipe_err VALUES ($1, $2)", 20, "skipped");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      // First insert succeeded
      assertNull(results.get(0).getError());
      assertEquals(1, results.get(0).rowsAffected());
      // Second insert failed (duplicate key)
      assertNotNull(results.get(1).getError());
      // Third was skipped by the server
      assertNotNull(results.get(2).getError());

      // Connection should still be usable after pipeline error
      var rs = conn.query("SELECT 'recovered'");
      assertEquals("recovered", rs.first().getString(0));
    }
  }

  @Test
  void pipelinedBatchSingleStatement() {
    try (var conn = connect()) {
      // Single parameterized statement should still work (falls back to sequential)
      conn.enqueue("SELECT $1::int", 42);
      List<RowSet> results = conn.flush();

      assertEquals(1, results.size());
      assertEquals(42, results.get(0).first().getInteger(0));
    }
  }

  @Test
  void pipelinedBatchWithParameterlessFallback() {
    try (var conn = connect()) {
      // Mix of parameterless and parameterized should fall back to sequential
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT $1::int", 2);
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertEquals(2, results.get(1).first().getInteger(0));
      assertEquals(3, results.get(2).first().getInteger(0));
    }
  }

  @Test
  void executeManyPipelined() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE exec_many_pipe (id int, name text)");

      List<Object[]> paramSets = new ArrayList<>();
      for (int i = 1; i <= 50; i++) {
        paramSets.add(new Object[] {i, "name" + i});
      }
      var results =
          conn.executeMany("INSERT INTO exec_many_pipe (id, name) VALUES ($1, $2)", paramSets);

      assertEquals(50, results.size());
      for (var rs : results) {
        assertNull(rs.getError());
        assertEquals(1, rs.rowsAffected());
      }

      var count = conn.query("SELECT count(*) FROM exec_many_pipe").first().getLong(0);
      assertEquals(50L, count);
    }
  }

  @Test
  void executeManyInTransaction() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE exec_many_tx (id int, val text)");

      try (var tx = conn.begin()) {
        var results =
            conn.executeMany(
                "INSERT INTO exec_many_tx (id, val) VALUES ($1, $2)",
                List.of(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"}));
        assertEquals(3, results.size());
        for (var rs : results) {
          assertNull(rs.getError());
        }
        tx.commit();
      }

      var count = conn.query("SELECT count(*) FROM exec_many_tx").first().getLong(0);
      assertEquals(3L, count);
    }
  }

  @Test
  void pipelinedBatchWithReturning() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_ret2 (id serial, name text)");

      conn.enqueue("INSERT INTO pipe_ret2 (name) VALUES ($1) RETURNING id", "Alice");
      conn.enqueue("INSERT INTO pipe_ret2 (name) VALUES ($1) RETURNING id", "Bob");
      conn.enqueue("INSERT INTO pipe_ret2 (name) VALUES ($1) RETURNING id", "Carol");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      long id1 = results.get(0).first().getLong("id");
      long id2 = results.get(1).first().getLong("id");
      long id3 = results.get(2).first().getLong("id");
      assertTrue(id1 > 0);
      assertTrue(id2 > id1);
      assertTrue(id3 > id2);
    }
  }

  // ===== Pipelining stress (ported from PipeliningQueryTestBase) =====

  @Test
  void pipelineStress1000SimpleQueries() {
    try (var conn = connect()) {
      for (int i = 0; i < 1000; i++) {
        conn.enqueue("SELECT " + i);
      }
      List<RowSet> results = conn.flush();

      assertEquals(1000, results.size());
      for (int i = 0; i < 1000; i++) {
        assertEquals(i, results.get(i).first().getInteger(0));
      }
    }
  }

  @Test
  void pipelineStress1000ParameterizedQueries() {
    try (var conn = connect()) {
      for (int i = 0; i < 1000; i++) {
        conn.enqueue("SELECT $1::int", i);
      }
      List<RowSet> results = conn.flush();

      assertEquals(1000, results.size());
      for (int i = 0; i < 1000; i++) {
        assertEquals(i, results.get(i).first().getInteger(0));
      }
    }
  }

  @Test
  void pipelineStress5000Mixed() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE stress5k (id int, val text)");
      // Mix of simple and parameterized queries in a single pipeline
      for (int i = 0; i < 5000; i++) {
        if (i % 3 == 0) {
          conn.enqueue("SELECT " + i);
        } else {
          conn.enqueue("INSERT INTO stress5k VALUES ($1, $2)", i, "v" + i);
        }
      }
      List<RowSet> results = conn.flush();

      assertEquals(5000, results.size());
      // Verify some results
      assertEquals(0, results.get(0).first().getInteger(0)); // simple SELECT 0
      assertFalse(results.get(1).getError() != null); // insert
      assertEquals(3, results.get(3).first().getInteger(0)); // simple SELECT 3

      // Verify all inserts landed
      var count = conn.query("SELECT count(*) FROM stress5k");
      // 5000 total, 1/3 are SELECTs (i%3==0), so ~3334 inserts
      assertTrue(count.first().getLong(0) > 3000);
    }
  }

  @Test
  void pipelineBatchInsert() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_batch (id int, val text)");
      conn.enqueue("BEGIN");
      for (int i = 0; i < 100; i++) {
        conn.enqueue("INSERT INTO pipe_batch VALUES ($1, $2)", i, "val-" + i);
      }
      conn.enqueue("COMMIT");
      conn.flush();

      var rs = conn.query("SELECT count(*) FROM pipe_batch");
      assertEquals(100L, rs.first().getLong(0));

      var data = conn.query("SELECT id, val FROM pipe_batch ORDER BY id");
      assertEquals(100, data.size());
      assertEquals(0, data.first().getInteger("id"));
      assertEquals("val-0", data.first().getString("val"));
    }
  }

  // ===== Additional pipeline error handling tests =====

  @Test
  void pipelineErrorAtStart() {
    try (var conn = connect()) {
      // Error at the start cancels all subsequent statements in the pipeline
      conn.enqueue("BAD SQL");
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT 2");
      List<RowSet> results = conn.flush();
      assertEquals(3, results.size());
      assertNotNull(results.get(0).getError());
      assertNotNull(results.get(1).getError());
      assertNotNull(results.get(2).getError());
    }
  }

  @Test
  void pipelineErrorAtEnd() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT 2");
      conn.enqueue("BAD SQL");
      List<RowSet> results = conn.flush();
      assertEquals(3, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertEquals(2, results.get(1).first().getInteger(0));
      assertTrue(results.get(2).getError() != null);
    }
  }

  @Test
  void pipelineAllErrors() {
    try (var conn = connect()) {
      conn.enqueue("BAD1");
      conn.enqueue("BAD2");
      conn.enqueue("BAD3");
      List<RowSet> results = conn.flush();
      assertEquals(3, results.size());
      assertTrue(results.get(0).getError() != null);
      assertTrue(results.get(1).getError() != null);
      assertTrue(results.get(2).getError() != null);
    }
  }

  @Test
  void pipelineErrorRecoveryThenQuery() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("BAD SQL");
      conn.enqueue("SELECT 3");
      conn.flush();

      // Connection should work normally after pipeline with errors
      var rs = conn.query("SELECT 42");
      assertEquals(42, rs.first().getInteger(0));
    }
  }

  @Test
  void pipelineTransactionErrorRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_tx_err (id int PRIMARY KEY)");
      conn.query("INSERT INTO pipe_tx_err VALUES (1)");

      conn.enqueue("BEGIN");
      conn.enqueue("INSERT INTO pipe_tx_err VALUES (2)");
      conn.enqueue("INSERT INTO pipe_tx_err VALUES (1)"); // duplicate PK
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertFalse(results.get(0).getError() != null); // BEGIN ok
      assertFalse(results.get(1).getError() != null); // insert ok
      assertTrue(results.get(2).getError() != null); // duplicate error

      conn.query("ROLLBACK");

      // Only original row should remain
      var rs = conn.query("SELECT count(*) FROM pipe_tx_err");
      assertEquals(1L, rs.first().getLong(0));
    }
  }

  @Test
  void pipelineErrorContainsSql() {
    try (var conn = connect()) {
      String badSql = "SELECT * FROM nonexistent_table_xyz";
      conn.enqueue("SELECT 1");
      conn.enqueue(badSql);
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      var error = results.get(1).getError();
      assertNotNull(error);
      assertEquals(badSql, error.sql());
    }
  }

  // ===== Pipeline error resilience (stress) =====

  @Test
  void pipelineRepeatedErrors128() {
    try (var conn = connect()) {
      for (int i = 0; i < 128; i++) {
        conn.enqueue("INVALID SQL NUMBER " + i);
      }
      List<RowSet> results = conn.flush();
      assertEquals(128, results.size());
      for (var rs : results) {
        assertNotNull(rs.getError());
      }
      // Connection should still be usable
      var rs = conn.query("SELECT 1 AS n");
      assertEquals(1, rs.first().getInteger("n"));
    }
  }

  @Test
  void pipelineMixedParamErrors() {
    try (var conn = connect()) {
      // With extended protocol, the first error cancels all subsequent statements.
      // Index 0 succeeds (even index), index 1 fails (odd index), 2-49 are all skipped.
      for (int i = 0; i < 50; i++) {
        if (i % 2 == 0) {
          conn.enqueue("SELECT $1::int AS n", i);
        } else {
          conn.enqueue("INVALID SQL " + i);
        }
      }
      List<RowSet> results = conn.flush();
      assertEquals(50, results.size());
      // First statement (i=0) succeeds
      assertEquals(0, results.get(0).first().getInteger("n"));
      // Second statement (i=1) fails
      assertNotNull(results.get(1).getError());
      // All subsequent statements are skipped
      for (int i = 2; i < 50; i++) {
        assertNotNull(results.get(i).getError());
      }
      // Connection still usable
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // ===== Large pipeline: triggers mid-pipeline Sync insertion =====

  @Test
  void pipelineLargeResponsesMidSync() {
    try (var conn = connect()) {
      // Generate enough large-response queries to exceed the internal buffer threshold
      // This should trigger mid-pipeline Sync insertion for safe response draining
      conn.query(
          "CREATE TEMP TABLE big_pipe AS SELECT generate_series(1, 500) AS n, repeat('x', 100) AS pad");

      for (int i = 0; i < 20; i++) {
        conn.enqueue("SELECT n, pad FROM big_pipe ORDER BY n");
      }
      List<RowSet> results = conn.flush();
      assertEquals(20, results.size());
      for (var rs : results) {
        assertNull(rs.getError());
        assertEquals(500, rs.size());
      }
      // Connection still works
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // ===== Batch execution =====

  @Test
  void executeManyInserts() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE batch_test (id int, name text)");

      var results =
          conn.executeMany(
              "INSERT INTO batch_test (id, name) VALUES ($1, $2)",
              List.of(
                  new Object[] {1, "Alice"}, new Object[] {2, "Bob"}, new Object[] {3, "Carol"}));

      assertEquals(3, results.size());
      for (var rs : results) {
        assertFalse(rs.getError() != null);
        assertEquals(1, rs.rowsAffected());
      }

      var all = conn.query("SELECT * FROM batch_test ORDER BY id");
      assertEquals(3, all.size());
      assertEquals("Alice", all.first().getString("name"));
    }
  }

  @Test
  void executeManyWithReturning() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE batch_ret (id serial, name text)");

      var results =
          conn.executeMany(
              "INSERT INTO batch_ret (name) VALUES ($1) RETURNING id",
              List.of(new Object[] {"Alice"}, new Object[] {"Bob"}));

      assertEquals(2, results.size());
      assertEquals(1, results.get(0).first().getInteger("id"));
      assertEquals(2, results.get(1).first().getInteger("id"));
    }
  }

  @Test
  void executeManyEmpty() {
    try (var conn = connect()) {
      var results = conn.executeMany("SELECT 1", List.of());
      assertTrue(results.isEmpty());
    }
  }

  // ===== executeMany with partial failures =====

  @Test
  void executeManyPartialFailure() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE batch_pf (id int PRIMARY KEY)");
      conn.query("INSERT INTO batch_pf VALUES (2)"); // pre-insert to cause duplicate

      var results =
          conn.executeMany(
              "INSERT INTO batch_pf VALUES ($1)",
              List.of(
                  new Object[] {1},
                  new Object[] {2}, // duplicate -- should error
                  new Object[] {3}));

      assertEquals(3, results.size());
      assertNull(results.get(0).getError()); // first should succeed
      assertNotNull(results.get(1).getError()); // duplicate key error
      // In PG pipeline mode, the error at index 1 may cause index 2 to also fail
      // (PG aborts the current transaction on error). Both outcomes are acceptable.

      // Connection still usable
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  @Test
  void executeManyAllFail() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE batch_af (id int PRIMARY KEY)");
      conn.query("INSERT INTO batch_af VALUES (1), (2), (3)");

      var results =
          conn.executeMany(
              "INSERT INTO batch_af VALUES ($1)",
              List.of(new Object[] {1}, new Object[] {2}, new Object[] {3}));

      assertEquals(3, results.size());
      // At least the first one should have an error
      assertNotNull(results.get(0).getError());

      // Connection still usable
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // ===== Large values and buffer boundaries =====

  @Test
  void largeTextParameter() {
    try (var conn = connect()) {
      String large = "A".repeat(100_000);
      var rs = conn.query("SELECT length($1::text) AS len", large);
      assertEquals(100_000, rs.first().getInteger("len"));
    }
  }

  @Test
  void largeByteaParameter() {
    try (var conn = connect()) {
      byte[] large = new byte[1_000_000];
      java.util.Arrays.fill(large, (byte) 0x42);
      conn.query("CREATE TEMP TABLE bytea_test (data bytea)");
      conn.query("INSERT INTO bytea_test VALUES ($1)", large);
      var rs = conn.query("SELECT length(data) AS len FROM bytea_test");
      assertEquals(1_000_000, rs.first().getInteger("len"));
    }
  }

  // ===== executeManyNamed =====

  @Test
  void executeManyNamed() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE emn (id int, val text)");
      var paramSets =
          List.of(
              Map.<String, Object>of("id", 1, "val", "a"),
              Map.<String, Object>of("id", 2, "val", "b"),
              Map.<String, Object>of("id", 3, "val", "c"));
      var results = conn.executeManyNamed("INSERT INTO emn VALUES (:id, :val)", paramSets);
      assertEquals(3, results.size());
      assertEquals(3L, conn.query("SELECT count(*) FROM emn").first().getLong(0));
    }
  }

  // ===== Prepared statement cache in pipelined batches =====

  /**
   * executeMany the same INSERT across two separate flush() calls. The second flush should reuse
   * the cached named statement (skip Parse). Verifies correctness — all rows should land.
   */
  @Test
  void executeManyReusesCache() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_cache (id int, val text)");

      String sql = "INSERT INTO pipe_cache (id, val) VALUES ($1, $2)";

      // First batch: cache miss — should prepare and cache the statement
      var results1 =
          conn.executeMany(sql, List.of(new Object[] {1, "a"}, new Object[] {2, "b"}));
      assertEquals(2, results1.size());
      for (var rs : results1) {
        assertNull(rs.getError());
      }

      // Second batch: cache hit — should skip Parse, reuse named statement
      var results2 =
          conn.executeMany(sql, List.of(new Object[] {3, "c"}, new Object[] {4, "d"}));
      assertEquals(2, results2.size());
      for (var rs : results2) {
        assertNull(rs.getError());
      }

      // All 4 rows should be present
      var count = conn.query("SELECT count(*) FROM pipe_cache").first().getLong(0);
      assertEquals(4L, count);
    }
  }

  /**
   * executeMany the same INSERT 1000 times across 20 separate flush() calls. Stresses the cache
   * path and verifies correctness at scale.
   */
  @Test
  void executeManyMultiFlushStress() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_stress (id int, val text)");

      String sql = "INSERT INTO pipe_stress (id, val) VALUES ($1, $2)";
      int total = 0;
      for (int batch = 0; batch < 20; batch++) {
        List<Object[]> paramSets = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
          paramSets.add(new Object[] {total + i, "v" + (total + i)});
        }
        var results = conn.executeMany(sql, paramSets);
        assertEquals(50, results.size());
        for (var rs : results) {
          assertNull(rs.getError());
        }
        total += 50;
      }

      var count = conn.query("SELECT count(*) FROM pipe_stress").first().getLong(0);
      assertEquals(1000L, count);
    }
  }

  /**
   * Enqueue a mix of different SQLs (some will be cached, some not) in one flush. All results
   * should be correct and cache entries should be created for each distinct SQL.
   */
  @Test
  void pipelineMixedSqlWithCache() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_mix_cache (id int, val text)");

      // Enqueue different SQL statements in one pipeline
      conn.enqueue("INSERT INTO pipe_mix_cache VALUES ($1, $2)", 1, "a");
      conn.enqueue("SELECT $1::text", "hello");
      conn.enqueue("INSERT INTO pipe_mix_cache VALUES ($1, $2)", 2, "b");
      conn.enqueue("SELECT $1::int + $2::int", 10, 20);
      List<RowSet> results = conn.flush();

      assertEquals(4, results.size());
      assertNull(results.get(0).getError());
      assertEquals("hello", results.get(1).first().getString(0));
      assertNull(results.get(2).getError());
      assertEquals(30, results.get(3).first().getInteger(0));

      // Second flush — same SQLs should hit cache
      conn.enqueue("INSERT INTO pipe_mix_cache VALUES ($1, $2)", 3, "c");
      conn.enqueue("SELECT $1::text", "world");
      conn.enqueue("SELECT $1::int + $2::int", 5, 7);
      List<RowSet> results2 = conn.flush();

      assertEquals(3, results2.size());
      assertNull(results2.get(0).getError());
      assertEquals("world", results2.get(1).first().getString(0));
      assertEquals(12, results2.get(2).first().getInteger(0));

      assertEquals(3L, conn.query("SELECT count(*) FROM pipe_mix_cache").first().getLong(0));
    }
  }

  /**
   * Pipeline with RETURNING: cache must correctly preserve column metadata so the second flush
   * can read result rows without a Describe.
   */
  @Test
  void executeManyWithReturningAndCache() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_ret_cache (id serial, name text)");

      String sql = "INSERT INTO pipe_ret_cache (name) VALUES ($1) RETURNING id, name";

      // First batch: cache miss
      var results1 = conn.executeMany(sql, List.of(new Object[] {"Alice"}, new Object[] {"Bob"}));
      assertEquals(2, results1.size());
      assertEquals(1, results1.get(0).first().getInteger("id"));
      assertEquals("Alice", results1.get(0).first().getString("name"));
      assertEquals(2, results1.get(1).first().getInteger("id"));

      // Second batch: cache hit — column metadata must be preserved
      var results2 =
          conn.executeMany(sql, List.of(new Object[] {"Carol"}, new Object[] {"Dave"}));
      assertEquals(2, results2.size());
      assertEquals(3, results2.get(0).first().getInteger("id"));
      assertEquals("Carol", results2.get(0).first().getString("name"));
      assertEquals(4, results2.get(1).first().getInteger("id"));
    }
  }

  /**
   * Pipeline >256 statements to trigger mid-pipeline Sync. Named statements should survive the
   * Sync boundary (unlike unnamed statements which are discarded).
   */
  @Test
  void pipelineMidSyncWithCache() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_sync_cache (id int)");

      // 500 statements should trigger at least one mid-pipeline Sync (threshold ~256)
      List<Object[]> paramSets = new ArrayList<>();
      for (int i = 0; i < 500; i++) {
        paramSets.add(new Object[] {i});
      }
      var results =
          conn.executeMany("INSERT INTO pipe_sync_cache (id) VALUES ($1)", paramSets);

      assertEquals(500, results.size());
      for (var rs : results) {
        assertNull(rs.getError());
      }

      var count = conn.query("SELECT count(*) FROM pipe_sync_cache").first().getLong(0);
      assertEquals(500L, count);
    }
  }

  /**
   * Stale plan retry: cache a SELECT, ALTER the table to change the result type, then re-execute
   * in a pipeline. The stale plan error should trigger eviction + retry.
   */
  @Test
  void pipelineStalePlanRetry() {
    try (var conn = connectWithCache()) {
      conn.query("CREATE TEMP TABLE pipe_stale (id int, name text)");
      conn.query("INSERT INTO pipe_stale VALUES (1, 'Alice')");

      String sql = "SELECT id, name FROM pipe_stale WHERE id = $1";

      // Prime the cache via single-query path
      var rs1 = conn.query(sql, 1);
      assertEquals("Alice", rs1.first().getString("name"));

      // ALTER TABLE to change result type — cached plan becomes stale
      conn.query("ALTER TABLE pipe_stale ALTER COLUMN name TYPE varchar(100)");

      // Pipeline should detect the stale plan, evict, retry, and succeed
      conn.enqueue(sql, 1);
      List<RowSet> results = conn.flush();

      assertEquals(1, results.size());
      // Should succeed after retry
      assertNull(results.get(0).getError());
      assertEquals("Alice", results.get(0).first().getString("name"));
    }
  }
}
