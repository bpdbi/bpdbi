package io.djb.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.djb.Connection;
import io.djb.DbException;
import io.djb.RowSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Shared connection test scenarios that work identically across Postgres and MySQL.
 * Database-specific tests remain in the driver-specific test classes.
 *
 * <p>Subclasses must implement {@link #connect()} and {@link #tempTableDDL(String, String)} to
 * provide database-specific connection and DDL logic. They should also set up and tear down a
 * Testcontainers container via {@code @BeforeAll}/{@code @AfterAll}.
 */
public abstract class AbstractConnectionTest {

  /**
   * Create a new connection to the test database.
   */
  protected abstract Connection connect();

  /**
   * Return DDL to create a temporary table. Postgres uses "CREATE TEMP TABLE", MySQL uses
   * "CREATE TEMPORARY TABLE"; column types may differ (e.g. "text" vs "VARCHAR(255)").
   */
  protected abstract String tempTableDDL(String name, String columns);

  // ===== Simple query protocol =====

  @Test
  void simpleSelect() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1 AS num, 'hello' AS greeting");
      assertEquals(1, rs.size());
      assertEquals(1, rs.first().getInteger("num"));
      assertEquals("hello", rs.first().getString("greeting"));
    }
  }

  @Test
  void simpleInsertUpdateDelete() {
    try (var conn = connect()) {
      conn.query(tempTableDDL("test_iud", "id int, val varchar(255)"));

      var insert = conn.query("INSERT INTO test_iud (id, val) VALUES (1, 'hello')");
      assertEquals(1, insert.rowsAffected());

      var update = conn.query("UPDATE test_iud SET val = 'world' WHERE id = 1");
      assertEquals(1, update.rowsAffected());

      var select = conn.query("SELECT val FROM test_iud WHERE id = 1");
      assertEquals("world", select.first().getString("val"));

      var delete = conn.query("DELETE FROM test_iud WHERE id = 1");
      assertEquals(1, delete.rowsAffected());

      var empty = conn.query("SELECT * FROM test_iud");
      assertEquals(0, empty.size());
    }
  }

  @Test
  void simpleQueryError() {
    try (var conn = connect()) {
      var ex = assertThrows(
          DbException.class,
          () -> conn.query("SELECT * FROM nonexistent_table_xyz")
      );
      assertNotNull(ex.getMessage());
    }
  }

  @Test
  void dataTypeBoolean() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT TRUE AS t, FALSE AS f");
      assertTrue(rs.first().getBoolean("t"));
      assertFalse(rs.first().getBoolean("f"));
    }
  }

  // ===== Pipelining =====

  @Test
  void pipelineSimpleStatements() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT 2");
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertEquals(2, results.get(1).first().getInteger(0));
      assertEquals(3, results.get(2).first().getInteger(0));
    }
  }

  @Test
  void pipelineWithIndexReturn() {
    try (var conn = connect()) {
      int first = conn.enqueue("SELECT 'first'");
      conn.enqueue("SELECT 'middle'");
      int last = conn.enqueue("SELECT 'last'");
      List<RowSet> results = conn.flush();

      assertEquals("first", results.get(first).first().getString(0));
      assertEquals("last", results.get(last).first().getString(0));
    }
  }

  @Test
  void pipelineTransaction() {
    try (var conn = connect()) {
      conn.query(tempTableDDL("pipe_tx", "id int, name varchar(255)"));

      conn.enqueue("BEGIN");
      conn.enqueue("INSERT INTO pipe_tx VALUES (1, 'Alice')");
      conn.enqueue("INSERT INTO pipe_tx VALUES (2, 'Bob')");
      conn.enqueue("COMMIT");
      List<RowSet> results = conn.flush();

      assertEquals(4, results.size());

      var rs = conn.query("SELECT count(*) AS cnt FROM pipe_tx");
      assertEquals(2L, rs.first().getLong("cnt"));
    }
  }

  @Test
  void pipelineErrorDoesNotPoisonSubsequent() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT * FROM nonexistent_table_xyz");
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertNotNull(results.get(1).getError());
      assertEquals(3, results.get(2).first().getInteger(0));
    }
  }

  @Test
  void pipelineEmptyFlush() {
    try (var conn = connect()) {
      assertTrue(conn.flush().isEmpty());
    }
  }

  // ===== Transactions =====

  @Test
  void transactionCommit() {
    try (var conn = connect()) {
      conn.query(tempTableDDL("tx_test", "id int"));
      conn.query("BEGIN");
      conn.query("INSERT INTO tx_test VALUES (1)");
      conn.query("INSERT INTO tx_test VALUES (2)");
      conn.query("COMMIT");

      var rs = conn.query("SELECT count(*) AS cnt FROM tx_test");
      assertEquals(2L, rs.first().getLong("cnt"));
    }
  }

  @Test
  void transactionRollback() {
    try (var conn = connect()) {
      conn.query(tempTableDDL("tx_rb", "id int"));
      conn.query("INSERT INTO tx_rb VALUES (1)");
      conn.query("BEGIN");
      conn.query("INSERT INTO tx_rb VALUES (2)");
      conn.query("ROLLBACK");

      var rs = conn.query("SELECT count(*) AS cnt FROM tx_rb");
      assertEquals(1L, rs.first().getLong("cnt"));
    }
  }

  // ===== Error recovery =====

  @Test
  void connectionUsableAfterError() {
    try (var conn = connect()) {
      assertThrows(DbException.class, () -> conn.query("SELECT * FROM nonexistent_table_xyz"));
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  // ===== NULL tests =====

  @Test
  void selectMultipleNulls() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT NULL AS a, NULL AS b, NULL AS c");
      var row = rs.first();
      assertTrue(row.isNull("a"));
      assertTrue(row.isNull("b"));
      assertTrue(row.isNull("c"));
    }
  }

  // ===== Pipelining stress =====

  @Test
  void pipelineStress200SimpleQueries() {
    try (var conn = connect()) {
      for (int i = 0; i < 200; i++) {
        conn.enqueue("SELECT " + i);
      }
      List<RowSet> results = conn.flush();

      assertEquals(200, results.size());
      for (int i = 0; i < 200; i++) {
        assertEquals(i, results.get(i).first().getInteger(0));
      }
    }
  }
}
