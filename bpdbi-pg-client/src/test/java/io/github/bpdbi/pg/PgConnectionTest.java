package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.DbException;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowMapper;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.test.AbstractConnectionTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Integration tests for Connection using Testcontainers Postgres. Ported from vertx-sql-client's
 * PipeliningQueryTestBase, TransactionTestBase, and PgConnectionTestBase.
 */
class PgConnectionTest extends AbstractConnectionTest {

  @SuppressWarnings("resource") // withReuse keeps container alive across test runs
  static final PostgreSQLContainer<?> pg =
      new PostgreSQLContainer<>("postgres:16-alpine").withReuse(true);

  @BeforeAll
  static void startContainer() {
    pg.start();
  }

  @Override
  protected PgConnection connect() {
    return PgConnection.connect(
        pg.getHost(),
        pg.getMappedPort(5432),
        pg.getDatabaseName(),
        pg.getUsername(),
        pg.getPassword());
  }

  @Override
  protected String tempTableDDL(String name, String columns) {
    return "CREATE TEMP TABLE " + name + " (" + columns + ")";
  }

  // ===== Connection lifecycle =====

  @Test
  void connectAndClose() {
    try (var conn = connect()) {
      assertNotNull(conn);
      assertTrue(conn.processId() > 0);
      assertNotNull(conn.parameters().get("server_version"));
      assertEquals("UTF8", conn.parameters().get("client_encoding"));
    }
  }

  @Test
  void connectWithWrongPassword() {
    assertThrows(
        PgException.class,
        () ->
            PgConnection.connect(
                pg.getHost(),
                pg.getMappedPort(5432),
                pg.getDatabaseName(),
                pg.getUsername(),
                "wrongpassword"));
  }

  @Test
  void connectWithWrongDatabase() {
    assertThrows(
        PgException.class,
        () ->
            PgConnection.connect(
                pg.getHost(),
                pg.getMappedPort(5432),
                "nonexistent_db",
                pg.getUsername(),
                pg.getPassword()));
  }

  // ===== Parameterless queries =====

  @Test
  void simpleSelectMultipleRows() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT generate_series(1, 10) AS n");
      assertEquals(10, rs.size());
      int sum = 0;
      for (var row : rs) {
        sum += row.getInteger("n");
      }
      assertEquals(55, sum); // 1+2+...+10
    }
  }

  @Test
  void emptyQuery() {
    try (var conn = connect()) {
      var rs = conn.query("");
      assertEquals(0, rs.size());
    }
  }

  @Test
  void multiStatementQueryIsRejected() {
    try (var conn = connect()) {
      // Multi-statement strings are not supported with the extended query protocol.
      // Postgres rejects them at the Parse step.
      assertThrows(
          PgException.class,
          () ->
              conn.query(
                  "CREATE TEMP TABLE multi_test (id int); INSERT INTO multi_test VALUES (1)"));
    }
  }

  // ===== Extended query protocol (parameterized) =====

  @Test
  void parameterizedSelect() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::int AS num, $2::text AS msg", 42, "hello");
      assertEquals(1, rs.size());
      assertEquals(42, rs.first().getInteger("num"));
      assertEquals("hello", rs.first().getString("msg"));
    }
  }

  @Test
  void parameterizedInsertAndSelect() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_test (id int, name text)");
      conn.query("INSERT INTO param_test VALUES ($1, $2)", 1, "Alice");
      conn.query("INSERT INTO param_test VALUES ($1, $2)", 2, "Bob");

      var rs = conn.query("SELECT name FROM param_test WHERE id = $1", 1);
      assertEquals("Alice", rs.first().getString("name"));

      var rs2 = conn.query("SELECT name FROM param_test WHERE id = $1", 2);
      assertEquals("Bob", rs2.first().getString("name"));
    }
  }

  @Test
  void parameterizedWithNull() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::text AS val", (Object) null);
      assertEquals(1, rs.size());
      assertTrue(rs.first().isNull("val"));
    }
  }

  @Test
  void parameterizedQueryError() {
    try (var conn = connect()) {
      var ex = assertThrows(PgException.class, () -> conn.query("SELECT $1::int", "not_a_number"));
      assertNotNull(ex.sqlState());
    }
  }

  // ===== Pipelining =====

  @Test
  void pipelineSetupAndQuery() {
    try (var conn = connect()) {
      conn.enqueue("SET search_path TO public");
      conn.enqueue("SET statement_timeout TO '30s'");
      RowSet result = conn.query("SELECT 42 AS answer");

      assertEquals(42, result.first().getInteger("answer"));
    }
  }

  @Test
  void pipelineTransactionWithReturning() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_ret (id serial, name text)");

      conn.enqueue("BEGIN");
      int alice = conn.enqueue("INSERT INTO pipe_ret (name) VALUES ('Alice') RETURNING id");
      int bob = conn.enqueue("INSERT INTO pipe_ret (name) VALUES ('Bob') RETURNING id");
      conn.enqueue("COMMIT");
      List<RowSet> results = conn.flush();

      long aliceId = results.get(alice).first().getLong("id");
      long bobId = results.get(bob).first().getLong("id");
      assertTrue(aliceId > 0);
      assertTrue(bobId > 0);
      assertNotEquals(aliceId, bobId);
    }
  }

  @Test
  void pipelineParameterized() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_param (id int, val text)");

      conn.enqueue("BEGIN");
      conn.enqueue("INSERT INTO pipe_param VALUES ($1, $2)", 1, "one");
      conn.enqueue("INSERT INTO pipe_param VALUES ($1, $2)", 2, "two");
      conn.enqueue("INSERT INTO pipe_param VALUES ($1, $2)", 3, "three");
      conn.enqueue("COMMIT");
      conn.flush();

      var rs = conn.query("SELECT count(*) FROM pipe_param");
      assertEquals(3L, rs.first().getLong(0));
    }
  }

  @Test
  void queryImplicitlyFlushesPending() {
    try (var conn = connect()) {
      conn.enqueue("SET search_path TO public");
      conn.enqueue("SET statement_timeout TO '30s'");
      // query() should flush the 2 enqueued statements + the query itself
      var rs = conn.query("SELECT 42 AS answer");
      assertEquals(42, rs.first().getInteger("answer"));
    }
  }

  // ===== Error recovery =====

  @Test
  void connectionUsableAfterParameterizedError() {
    try (var conn = connect()) {
      try {
        conn.query("SELECT $1::int", "not_a_number");
        fail("Should have thrown");
      } catch (PgException e) {
        // expected
      }
      var rs = conn.query("SELECT 'recovered'");
      assertEquals("recovered", rs.first().getString(0));
    }
  }

  // ===== Large results =====

  @Test
  void largeResultSet() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT generate_series(1, 10000) AS n");
      assertEquals(10000, rs.size());

      long sum = 0;
      for (var row : rs) {
        sum += row.getLong("n");
      }
      assertEquals(50005000L, sum); // n*(n+1)/2
    }
  }

  // ===== Continuous pipelining (ported from PipeliningQueryTestBase) =====

  @Test
  void continuousSimpleQueries() {
    try (var conn = connect()) {
      for (int i = 0; i < 100; i++) {
        conn.enqueue("SELECT " + i);
      }
      List<RowSet> results = conn.flush();

      assertEquals(100, results.size());
      for (int i = 0; i < 100; i++) {
        assertEquals(i, results.get(i).first().getInteger(0));
      }
    }
  }

  @Test
  void continuousParameterizedQueries() {
    try (var conn = connect()) {
      for (int i = 0; i < 100; i++) {
        conn.enqueue("SELECT $1::int", i);
      }
      List<RowSet> results = conn.flush();

      assertEquals(100, results.size());
      for (int i = 0; i < 100; i++) {
        assertEquals(i, results.get(i).first().getInteger(0));
      }
    }
  }

  @Test
  void mixedSimpleAndParameterizedPipeline() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 'simple'");
      conn.enqueue("SELECT $1::text", "parameterized");
      conn.enqueue("SELECT 42");
      conn.enqueue("SELECT $1::int", 99);
      List<RowSet> results = conn.flush();

      assertEquals(4, results.size());
      assertEquals("simple", results.get(0).first().getString(0));
      assertEquals("parameterized", results.get(1).first().getString(0));
      assertEquals(42, results.get(2).first().getInteger(0));
      assertEquals(99, results.get(3).first().getInteger(0));
    }
  }

  // ===== Connection tests (ported from ConnectionTestBase) =====

  @Test
  void connectWithInvalidUsername() {
    assertThrows(
        PgException.class,
        () ->
            PgConnection.connect(
                pg.getHost(),
                pg.getMappedPort(5432),
                pg.getDatabaseName(),
                "nonexistent_user_xyz",
                pg.getPassword()));
  }

  @Test
  void connectAndQueryDatabaseMetadata() {
    try (var conn = connect()) {
      var version = conn.parameters().get("server_version");
      assertNotNull(version);
      assertFalse(version.isEmpty());
    }
  }

  // ===== Multiple error recovery (ported from PgConnectionTestBase) =====

  @Test
  void multipleErrorRecovery() {
    try (var conn = connect()) {
      for (int i = 0; i < 10; i++) {
        try {
          conn.query("INVALID SQL");
          fail("Should have thrown");
        } catch (PgException e) {
          // expected
        }
        var rs = conn.query("SELECT " + i);
        assertEquals(i, rs.first().getInteger(0));
      }
    }
  }

  @Test
  void pipelineMultipleErrors() {
    try (var conn = connect()) {
      // With the extended protocol, an error cancels all subsequent statements in the same
      // Sync boundary. Statements before the error succeed; the failing one and all after it
      // carry errors.
      conn.enqueue("SELECT 1");
      conn.enqueue("BAD SQL");
      conn.enqueue("SELECT 2");
      conn.enqueue("ALSO BAD SQL");
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(5, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertNotNull(results.get(1).getError());
      // Statements 2-4 are skipped due to the error at index 1
      assertNotNull(results.get(2).getError());
      assertNotNull(results.get(3).getError());
      assertNotNull(results.get(4).getError());
    }
  }

  // ===== Row access tests =====

  @Test
  void rowColumnByName() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1 AS a, 'hello' AS b, true AS c");
      var row = rs.first();
      assertEquals(1, row.getInteger("a"));
      assertEquals("hello", row.getString("b"));
      assertTrue(row.getBoolean("c"));
    }
  }

  @Test
  void rowColumnByIndex() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1, 'hello', true");
      var row = rs.first();
      assertEquals(1, row.getInteger(0));
      assertEquals("hello", row.getString(1));
      assertTrue(row.getBoolean(2));
      assertEquals(3, row.size());
    }
  }

  @Test
  void rowInvalidColumnName() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1 AS a");
      var row = rs.first();
      assertThrows(IllegalArgumentException.class, () -> row.getString("nonexistent"));
    }
  }

  @Test
  void rowSetStream() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT generate_series(1, 5) AS n");
      long sum = rs.stream().mapToLong(r -> r.getLong("n")).sum();
      assertEquals(15L, sum);
    }
  }

  @Test
  void rowSetNoRows() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE empty_test (id int)");
      var rs = conn.query("SELECT * FROM empty_test");
      assertEquals(0, rs.size());
      assertThrows(IllegalStateException.class, rs::first);
    }
  }

  // ===== Update count tests =====

  @Test
  void insertRowCount() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE rc_test (id int)");
      var rs = conn.query("INSERT INTO rc_test VALUES (1)");
      assertEquals(1, rs.rowsAffected());
    }
  }

  @Test
  void updateRowCount() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE rc_upd (id int, val text)");
      conn.query("INSERT INTO rc_upd VALUES (1, 'a'), (2, 'b'), (3, 'c')");
      var rs = conn.query("UPDATE rc_upd SET val = 'x' WHERE id <= 2");
      assertEquals(2, rs.rowsAffected());
    }
  }

  @Test
  void deleteRowCount() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE rc_del (id int)");
      conn.query("INSERT INTO rc_del VALUES (1), (2), (3), (4), (5)");
      var rs = conn.query("DELETE FROM rc_del WHERE id > 2");
      assertEquals(3, rs.rowsAffected());
    }
  }

  @Test
  void createTableRowCount() {
    try (var conn = connect()) {
      var rs = conn.query("CREATE TEMP TABLE rc_create (id int)");
      assertEquals(0, rs.rowsAffected());
    }
  }

  // ===== Prepare error tests (ported from PreparedQueryTestBase) =====

  @Test
  void prepareErrorInvalidSql() {
    try (var conn = connect()) {
      var ex = assertThrows(PgException.class, () -> conn.query("SELECT FROM WHERE INVALID", 1));
      assertNotNull(ex.sqlState());
    }
  }

  @Test
  void prepareErrorWrongParamCount() {
    try (var conn = connect()) {
      // Provide 2 params but SQL has 1 placeholder -- PG will error
      // (bind sends extra params, PG may ignore or error)
      // At minimum, providing too few params for the SQL should fail
      var ex = assertThrows(Exception.class, () -> conn.query("SELECT $1::int, $2::text", 42));
      // Connection should still be usable after
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  // ===== Schema setting test (ported from PgConnectionTest) =====

  @Test
  void settingSearchPath() {
    try (var conn = connect()) {
      conn.query("CREATE SCHEMA IF NOT EXISTS test_schema");
      conn.query("CREATE TABLE IF NOT EXISTS test_schema.schema_test (id int)");
      conn.query("SET search_path TO test_schema");
      conn.query("INSERT INTO schema_test VALUES (42)");
      var rs = conn.query("SELECT id FROM schema_test");
      assertEquals(42, rs.first().getInteger(0));
      conn.query("DROP TABLE test_schema.schema_test");
      conn.query("DROP SCHEMA test_schema");
    }
  }

  // ===== Prepared statements =====

  @Test
  void preparedStatementBasic() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT $1::int AS n")) {
        var rs1 = stmt.query(1);
        assertEquals(1, rs1.first().getInteger("n"));
        var rs2 = stmt.query(42);
        assertEquals(42, rs2.first().getInteger("n"));
        var rs3 = stmt.query(999);
        assertEquals(999, rs3.first().getInteger("n"));
      }
    }
  }

  @Test
  void preparedStatementInsertAndSelect() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE ps_test (id int, name text)");
      try (var insert = conn.prepare("INSERT INTO ps_test VALUES ($1, $2)")) {
        insert.query(1, "Alice");
        insert.query(2, "Bob");
        insert.query(3, "Charlie");
      }
      try (var select = conn.prepare("SELECT name FROM ps_test WHERE id = $1")) {
        assertEquals("Alice", select.query(1).first().getString(0));
        assertEquals("Bob", select.query(2).first().getString(0));
        assertEquals("Charlie", select.query(3).first().getString(0));
      }
    }
  }

  @Test
  void preparedStatementWithNull() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT $1::text AS val")) {
        var rs = stmt.query((Object) null);
        assertTrue(rs.first().isNull("val"));
      }
    }
  }

  @Test
  void preparedStatementError() {
    try (var conn = connect()) {
      assertThrows(PgException.class, () -> conn.prepare("INVALID SQL GIBBERISH"));
      // Connection still usable
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  @Test
  void preparedStatementReuseManyTimes() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT $1::int * 2 AS doubled")) {
        for (int i = 0; i < 100; i++) {
          assertEquals(i * 2, stmt.query(i).first().getInteger("doubled"));
        }
      }
    }
  }

  @Test
  void preparedStatementNamedParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_test (id int, name text)");
      try (var insert = conn.prepare("INSERT INTO np_test VALUES (:id, :name)")) {
        insert.query(Map.of("id", 1, "name", "Alice"));
        insert.query(Map.of("id", 2, "name", "Bob"));
      }
      try (var select = conn.prepare("SELECT name FROM np_test WHERE id = :id")) {
        assertEquals("Alice", select.query(Map.of("id", 1)).first().getString(0));
        assertEquals("Bob", select.query(Map.of("id", 2)).first().getString(0));
      }
    }
  }

  @Test
  void preparedStatementNamedParamsMultiple() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT :a::int + :b::int AS sum")) {
        assertEquals(3, stmt.query(Map.of("a", 1, "b", 2)).first().getInteger("sum"));
        assertEquals(11, stmt.query(Map.of("a", 5, "b", 6)).first().getInteger("sum"));
      }
    }
  }

  @Test
  void preparedStatementNamedParamsMissingThrows() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT :val::int")) {
        assertThrows(IllegalArgumentException.class, () -> stmt.query(Map.of()));
      }
    }
  }

  @Test
  void preparedStatementPositionalRejectsMapCall() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT $1::int")) {
        assertThrows(IllegalStateException.class, () -> stmt.query(Map.of("val", 1)));
      }
    }
  }

  @Test
  void preparedStatementNamedParamsWithNull() {
    try (var conn = connect()) {
      try (var stmt = conn.prepare("SELECT :val::text AS val")) {
        var params = new java.util.HashMap<String, Object>();
        params.put("val", null);
        var rs = stmt.query(params);
        assertTrue(rs.first().isNull("val"));
      }
    }
  }

  @Test
  void preparedStatementNamedParamsPositionalStillWorks() {
    try (var conn = connect()) {
      // Prepare with named params, but call with positional args
      try (var stmt = conn.prepare("SELECT :a::int + :b::int AS sum")) {
        assertEquals(7, stmt.query(3, 4).first().getInteger("sum"));
      }
    }
  }

  @Test
  void preparedStatementNamedParamsWithArray() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_arr (id int)");
      conn.query("INSERT INTO np_arr VALUES (1), (2), (3), (4), (5)");
      try (var stmt =
          conn.prepare("SELECT id FROM np_arr WHERE id = ANY(:ids::int[]) ORDER BY id")) {
        var rs1 = stmt.query(Map.of("ids", List.of(1, 3, 5)));
        var ids1 = new ArrayList<Integer>();
        for (var row : rs1) {
          ids1.add(row.getInteger("id"));
        }
        assertEquals(List.of(1, 3, 5), ids1);

        // Re-execute with different size collection -- same prepared statement
        var rs2 = stmt.query(Map.of("ids", List.of(2, 4)));
        var ids2 = new ArrayList<Integer>();
        for (var row : rs2) {
          ids2.add(row.getInteger("id"));
        }
        assertEquals(List.of(2, 4), ids2);
      }
    }
  }

  @Test
  void preparedStatementNamedParamsWithPrimitiveArray() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_parr (id int)");
      conn.query("INSERT INTO np_parr VALUES (10), (20), (30)");
      try (var stmt =
          conn.prepare("SELECT id FROM np_parr WHERE id = ANY(:ids::int[]) ORDER BY id")) {
        var rs = stmt.query(Map.of("ids", new int[] {10, 30}));
        var ids = new ArrayList<Integer>();
        for (var row : rs) {
          ids.add(row.getInteger("id"));
        }
        assertEquals(List.of(10, 30), ids);
      }
    }
  }

  @Test
  void preparedStatementNamedParamsWithStringArray() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_sarr (name text)");
      conn.query("INSERT INTO np_sarr VALUES ('Alice'), ('Bob'), ('Carol')");
      try (var stmt =
          conn.prepare("SELECT name FROM np_sarr WHERE name = ANY(:names::text[]) ORDER BY name")) {
        var rs = stmt.query(Map.of("names", List.of("Alice", "Carol")));
        var names = new ArrayList<String>();
        for (var row : rs) {
          names.add(row.getString("name"));
        }
        assertEquals(List.of("Alice", "Carol"), names);
      }
    }
  }

  @Test
  void preparedStatementNamedParamsWithEmptyCollection() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_empty (id int)");
      conn.query("INSERT INTO np_empty VALUES (1), (2)");
      try (var stmt = conn.prepare("SELECT id FROM np_empty WHERE id = ANY(:ids::int[])")) {
        var rs = stmt.query(Map.of("ids", List.of()));
        assertFalse(rs.iterator().hasNext());
      }
    }
  }

  @Test
  void preparedStatementNamedParamsCachedPath() {
    try (var conn =
        PgConnection.connect(
            ConnectionConfig.fromUri(
                    "postgresql://"
                        + pg.getUsername()
                        + ":"
                        + pg.getPassword()
                        + "@"
                        + pg.getHost()
                        + ":"
                        + pg.getMappedPort(5432)
                        + "/"
                        + pg.getDatabaseName())
                .cachePreparedStatements(true))) {
      String sql = "SELECT :n::int AS n";

      // First call: cache miss -- prepares and caches
      try (var stmt = conn.prepare(sql)) {
        assertEquals(1, stmt.query(Map.of("n", 1)).first().getInteger("n"));
      }

      // Second call: cache hit -- named param mapping must survive
      try (var stmt = conn.prepare(sql)) {
        assertEquals(42, stmt.query(Map.of("n", 42)).first().getInteger("n"));
      }
    }
  }

  // ===== Cursor =====

  @Test
  void cursorBasic() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_test AS SELECT generate_series(1, 100) AS n");
      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_test ORDER BY n")) {
        var batch1 = cursor.read(30);
        assertEquals(30, batch1.size());
        assertEquals(1, batch1.first().getInteger(0));
        assertTrue(cursor.hasMore());

        var batch2 = cursor.read(30);
        assertEquals(30, batch2.size());
        assertEquals(31, batch2.first().getInteger(0));

        var batch3 = cursor.read(30);
        assertEquals(30, batch3.size());

        var batch4 = cursor.read(30);
        assertEquals(10, batch4.size()); // only 10 remaining
        assertFalse(cursor.hasMore());
      }
      conn.query("COMMIT");
    }
  }

  @Test
  void cursorWithParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_param AS SELECT generate_series(1, 50) AS n");
      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_param WHERE n > $1 ORDER BY n", 40)) {
        var batch = cursor.read(100);
        assertEquals(10, batch.size());
        assertEquals(41, batch.first().getInteger(0));
        assertFalse(cursor.hasMore());
      }
      conn.query("COMMIT");
    }
  }

  // ===== Connection URI parsing =====

  @Test
  void connectWithUri() {
    var config =
        ConnectionConfig.fromUri(
            "postgresql://"
                + pg.getUsername()
                + ":"
                + pg.getPassword()
                + "@"
                + pg.getHost()
                + ":"
                + pg.getMappedPort(5432)
                + "/"
                + pg.getDatabaseName());
    try (var conn = PgConnection.connect(config)) {
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  @Test
  void connectionConfigDefaults() {
    var config = ConnectionConfig.fromUri("postgresql://user:pass@localhost/mydb");
    assertEquals("localhost", config.host());
    assertEquals(5432, config.port());
    assertEquals("mydb", config.database());
    assertEquals("user", config.username());
    assertEquals("pass", config.password());
  }

  // ===== Cancel request =====

  @Test
  void cancelRequestDoesNotCrash() {
    try (var conn = connect()) {
      conn.cancelRequest();
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  // ===== RowMapper =====

  @Test
  void rowMapperMapTo() {
    record User(int id, String name) {}

    RowMapper<User> mapper = row -> new User(row.getInteger("id"), row.getString("name"));
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE rm_test (id int, name text)");
      conn.query("INSERT INTO rm_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
      var users = conn.query("SELECT id, name FROM rm_test ORDER BY id").mapTo(mapper);
      assertEquals(3, users.size());
      assertEquals("Alice", users.get(0).name());
      assertEquals("Bob", users.get(1).name());
      assertEquals("Charlie", users.get(2).name());
    }
  }

  @Test
  void rowMapperMapFirst() {
    try (var conn = connect()) {
      String name = conn.query("SELECT 'hello' AS val").mapFirst(row -> row.getString("val"));
      assertEquals("hello", name);
    }
  }

  @Test
  void rowMapperEmptyResult() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE rm_empty (id int)");
      var result = conn.query("SELECT * FROM rm_empty").mapTo(row -> row.getInteger("id"));
      assertTrue(result.isEmpty());
    }
  }

  // ===== TypeRegistry (typed get) =====

  @Test
  void columnMapperDefaultTypes() {
    try (var conn = connect()) {
      // Cast to explicit types so Postgres uses the correct binary format
      var rs = conn.query("SELECT 42 AS n, 'hello' AS s, '2024-01-15'::date AS d");
      var row = rs.first();
      assertEquals(42, row.get("n", Integer.class));
      assertEquals("hello", row.get("s", String.class));
      assertEquals(java.time.LocalDate.of(2024, 1, 15), row.get("d", java.time.LocalDate.class));
    }
  }

  @Test
  void typeRegistryCustomDecode() {
    record Money(java.math.BigDecimal amount) {}

    try (var conn = connect()) {
      conn.typeRegistry()
          .register(Money.class, java.math.BigDecimal.class, Money::amount, Money::new);
      var rs = conn.query("SELECT '9.99'::numeric AS price");
      var money = rs.first().get("price", Money.class);
      assertEquals(new java.math.BigDecimal("9.99"), money.amount());
    }
  }

  @Test
  void columnMapperNull() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT NULL::int AS n");
      assertNull(rs.first().get("n", Integer.class));
    }
  }

  // ===== Named parameters =====

  @Test
  void namedParamSelect() {
    try (var conn = connect()) {
      var rs =
          conn.sql("SELECT :name AS name, :age AS age")
              .bind("name", "Alice")
              .bind("age", 30)
              .query();
      assertEquals("Alice", rs.first().getString("name"));
      assertEquals(30, rs.first().getInteger("age"));
    }
  }

  @Test
  void namedParamInsertAndSelect() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_test (id int, name text)");
      conn.sql("INSERT INTO np_test VALUES (:id, :name)")
          .bind("id", 1)
          .bind("name", "Alice")
          .query();
      conn.sql("INSERT INTO np_test VALUES (:id, :name)").bind("id", 2).bind("name", "Bob").query();

      var rs = conn.sql("SELECT name FROM np_test WHERE id = :id").bind("id", 1).query();
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  @Test
  void namedParamInPipeline() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_pipe (id int, val text)");
      conn.sql("INSERT INTO np_pipe VALUES (:id, :val)").bind("id", 1).bind("val", "one").enqueue();
      conn.sql("INSERT INTO np_pipe VALUES (:id, :val)").bind("id", 2).bind("val", "two").enqueue();
      conn.flush();

      var rs = conn.query("SELECT count(*) FROM np_pipe");
      assertEquals(2L, rs.first().getLong(0));
    }
  }

  @Test
  void namedParamMissingThrows() {
    try (var conn = connect()) {
      assertThrows(IllegalArgumentException.class, () -> conn.query("SELECT :missing", Map.of()));
    }
  }

  // ===== TypeRegistry (custom param encoding) =====

  @Test
  void customParamEncoder() {
    record Currency(String code) {}

    try (var conn = connect()) {
      conn.typeRegistry().register(Currency.class, String.class, Currency::code, null);
      conn.query("CREATE TEMP TABLE tb_test (id int, currency text)");
      conn.query("INSERT INTO tb_test VALUES ($1, $2)", 1, new Currency("USD"));
      var rs = conn.query("SELECT currency FROM tb_test WHERE id = 1");
      assertEquals("USD", rs.first().getString(0));
    }
  }

  // ===== Additional cursor tests =====

  @Test
  void cursorCloseEarly() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_close (n int)");
      conn.query("BEGIN");
      for (int i = 1; i <= 100; i++) {
        conn.query("INSERT INTO cursor_close VALUES (" + i + ")");
      }
      conn.query("COMMIT");

      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_close ORDER BY n")) {
        var batch = cursor.read(10);
        assertEquals(10, batch.size());
        assertEquals(1, batch.first().getInteger(0));
        assertTrue(cursor.hasMore());
        // Close cursor early without reading all rows
      }
      conn.query("COMMIT");

      // Connection should still be usable
      var rs = conn.query("SELECT 1");
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  @Test
  void cursorEmptyResult() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_empty (n int)");
      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_empty")) {
        var batch = cursor.read(10);
        assertEquals(0, batch.size());
        assertFalse(cursor.hasMore());
      }
      conn.query("COMMIT");
    }
  }

  @Test
  void cursorParamsUseBinaryEncoding() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_binder (id int, data bytea)");
      byte[] value = new byte[] {(byte) 0xCA, (byte) 0xFE};
      conn.query("INSERT INTO cursor_binder VALUES ($1, $2)", 1, value);

      conn.query("BEGIN");
      // Cursor with a byte[] param should be binary-encoded correctly
      try (var cursor =
          conn.cursor("SELECT id, data FROM cursor_binder WHERE data = $1", (Object) value)) {
        var batch = cursor.read(10);
        assertEquals(1, batch.size());
        assertEquals(1, batch.first().getInteger(0));
      }
      conn.query("COMMIT");
    }
  }

  // ===== Prepared statement cache =====

  @Test
  void preparedStatementCacheHit() {
    try (var conn =
        PgConnection.connect(
            ConnectionConfig.fromUri(
                    "postgresql://"
                        + pg.getUsername()
                        + ":"
                        + pg.getPassword()
                        + "@"
                        + pg.getHost()
                        + ":"
                        + pg.getMappedPort(5432)
                        + "/"
                        + pg.getDatabaseName())
                .cachePreparedStatements(true))) {
      // First call: cache miss
      var rs1 = conn.query("SELECT $1::int AS n", 1);
      assertEquals(1, rs1.first().getInteger("n"));

      // Second call: cache hit
      var rs2 = conn.query("SELECT $1::int AS n", 2);
      assertEquals(2, rs2.first().getInteger("n"));

      // Third call: still from cache
      var rs3 = conn.query("SELECT $1::int AS n", 3);
      assertEquals(3, rs3.first().getInteger("n"));
    }
  }

  @Test
  void searchPathChangeInvalidatesPreparedStatementCache() {
    try (var conn =
        PgConnection.connect(
            new ConnectionConfig(
                    pg.getHost(),
                    pg.getMappedPort(5432),
                    pg.getDatabaseName(),
                    pg.getUsername(),
                    pg.getPassword())
                .cachePreparedStatements(true))) {
      // Cache a prepared statement
      conn.query("SELECT $1::int AS n", 42);

      // Change search_path -- detected by SQL text inspection (Postgres does not send
      // ParameterStatus for search_path, so we detect SET search_path in the SQL)
      conn.query("SET search_path TO pg_catalog");

      // Queries still work after cache invalidation (statement re-prepared)
      var rs = conn.query("SELECT $1::int AS n", 99);
      assertEquals(99, rs.first().getInteger("n"));
    }
  }

  @Test
  void deallocateAllInvalidatesPreparedStatementCache() {
    try (var conn =
        PgConnection.connect(
            new ConnectionConfig(
                    pg.getHost(),
                    pg.getMappedPort(5432),
                    pg.getDatabaseName(),
                    pg.getUsername(),
                    pg.getPassword())
                .cachePreparedStatements(true))) {
      // Cache a prepared statement
      conn.query("SELECT $1::int AS n", 42);

      // DEALLOCATE ALL destroys server-side prepared statements
      conn.query("DEALLOCATE ALL");

      // Queries still work (re-prepared after cache invalidation)
      var rs = conn.query("SELECT $1::int AS n", 99);
      assertEquals(99, rs.first().getInteger("n"));
    }
  }

  // ===== LISTEN/NOTIFY =====

  @Test
  void listenNotifySameConnection() {
    try (var conn = connect()) {
      conn.listen("test_channel");

      // NOTIFY from same connection
      conn.notify("test_channel", "hello");

      // Notifications arrive during the next query roundtrip
      conn.query("SELECT 1");
      var notes = conn.getNotifications();
      assertEquals(1, notes.size());
      assertEquals("test_channel", notes.getFirst().channel());
      assertEquals("hello", notes.getFirst().payload());
      assertTrue(notes.getFirst().processId() > 0);
    }
  }

  @Test
  void listenNotifyFromAnotherConnection() {
    try (var listener = connect();
        var sender = connect()) {
      listener.listen("cross_channel");

      // Send from another connection
      sender.notify("cross_channel", "cross-msg");

      // Trigger a roundtrip on the listener to receive it
      listener.query("SELECT 1");
      var notes = listener.getNotifications();
      assertEquals(1, notes.size());
      assertEquals("cross_channel", notes.getFirst().channel());
      assertEquals("cross-msg", notes.getFirst().payload());
    }
  }

  @Test
  void unlistenStopsNotifications() {
    try (var conn = connect()) {
      conn.listen("temp_channel");
      conn.unlisten("temp_channel");

      conn.query("NOTIFY temp_channel, 'should-not-arrive'");
      conn.query("SELECT 1");
      assertTrue(conn.getNotifications().isEmpty());
    }
  }

  @Test
  void getNotificationsClearsQueue() {
    try (var conn = connect()) {
      conn.listen("clear_test");
      conn.notify("clear_test", "msg1");
      conn.query("SELECT 1");

      var first = conn.getNotifications();
      assertEquals(1, first.size());

      // Second call should be empty
      var second = conn.getNotifications();
      assertTrue(second.isEmpty());
    }
  }

  @Test
  void multipleNotifications() {
    try (var conn = connect()) {
      conn.listen("multi");
      conn.notify("multi", "a");
      conn.notify("multi", "b");
      conn.notify("multi", "c");
      conn.query("SELECT 1");

      var notes = conn.getNotifications();
      assertEquals(3, notes.size());
      assertEquals("a", notes.get(0).payload());
      assertEquals("b", notes.get(1).payload());
      assertEquals("c", notes.get(2).payload());
    }
  }

  // ===== IN-list expansion =====

  @Test
  void inListExpansionWithNamedParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inlist_test (id int, name text)");
      conn.query("INSERT INTO inlist_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

      var rs =
          conn.sql("SELECT * FROM inlist_test WHERE id IN (:ids) ORDER BY id")
              .bind("ids", List.of(1, 3))
              .query();

      assertEquals(2, rs.size());
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  // ===== ANY() with array literals =====
  //
  // For non-prepared queries, use IN (:ids) with collection expansion.
  // For prepared statements, use = ANY(:ids::type[]) -- see preparedStatementNamedParamsWith*
  // tests.
  // The positional query() path supports manually-formatted array literals:

  @Test
  void anyWithPositionalArrayLiteral() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE any_pos (id int, name text)");
      conn.query("INSERT INTO any_pos VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

      var rs =
          conn.query("SELECT name FROM any_pos WHERE id = ANY($1::int[]) ORDER BY id", "{1,3}");
      var names = new ArrayList<String>();
      for (var row : rs) {
        names.add(row.getString("name"));
      }
      assertEquals(List.of("Alice", "Carol"), names);
    }
  }

  @Test
  void anyWithPositionalArrayLiteralStrings() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE any_pstr (name text)");
      conn.query("INSERT INTO any_pstr VALUES ('Alice'), ('Bob'), ('Carol')");

      var rs =
          conn.query(
              "SELECT name FROM any_pstr WHERE name = ANY($1::text[]) ORDER BY name",
              "{Alice,Carol}");
      var names = new ArrayList<String>();
      for (var row : rs) {
        names.add(row.getString("name"));
      }
      assertEquals(List.of("Alice", "Carol"), names);
    }
  }

  @Test
  void anyWithPositionalArrayLiteralEmpty() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE any_pempty (id int)");
      conn.query("INSERT INTO any_pempty VALUES (1), (2)");

      var rs = conn.query("SELECT id FROM any_pempty WHERE id = ANY($1::int[])", "{}");
      assertFalse(rs.iterator().hasNext());
    }
  }

  @Test
  void anyWithPositionalEnqueue() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE any_penq (id int, name text)");
      conn.query("INSERT INTO any_penq VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

      conn.enqueue("SELECT name FROM any_penq WHERE id = ANY($1::int[]) ORDER BY id", "{1,2}");
      conn.enqueue("SELECT name FROM any_penq WHERE id = ANY($1::int[]) ORDER BY id", "{3}");
      var results = conn.flush();

      assertEquals(2, results.get(0).size());
      assertEquals("Alice", results.get(0).first().getString("name"));
      assertEquals(1, results.get(1).size());
      assertEquals("Carol", results.get(1).first().getString("name"));
    }
  }

  @Test
  void anyWithPositionalMixedWithScalar() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE any_pmix (id int, status text)");
      conn.query(
          "INSERT INTO any_pmix VALUES (1, 'active'), (2, 'inactive'), (3, 'active'), (4, 'inactive')");

      var rs =
          conn.query(
              "SELECT id FROM any_pmix WHERE status = $1 AND id = ANY($2::int[]) ORDER BY id",
              "active",
              "{1,2,3}");
      var ids = new ArrayList<Integer>();
      for (var row : rs) {
        ids.add(row.getInteger("id"));
      }
      assertEquals(List.of(1, 3), ids);
    }
  }

  // ===== Streaming =====

  @Test
  void queryStreamSimple() {
    try (var conn = connect()) {
      var values = new ArrayList<Integer>();
      conn.queryStream(
          "SELECT generate_series(1, 100) AS n", row -> values.add(row.getInteger("n")));
      assertEquals(100, values.size());
      assertEquals(1, values.getFirst());
      assertEquals(100, values.getLast());
    }
  }

  @Test
  void queryStreamWithParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE stream_test (id int, name text)");
      conn.query("INSERT INTO stream_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

      var names = new ArrayList<String>();
      conn.queryStream(
          "SELECT name FROM stream_test WHERE id > $1 ORDER BY id",
          new Object[] {1},
          row -> names.add(row.getString("name")));
      assertEquals(List.of("Bob", "Carol"), names);
    }
  }

  @Test
  void queryStreamNoRows() {
    try (var conn = connect()) {
      var count = new int[] {0};
      conn.queryStream("SELECT 1 WHERE false", row -> count[0]++);
      assertEquals(0, count[0]);
    }
  }

  @Test
  void queryStreamError() {
    try (var conn = connect()) {
      assertThrows(
          PgException.class,
          () -> conn.queryStream("SELECT * FROM nonexistent_streaming_table", row -> {}));
      // Connection should still be usable after error
      var rs = conn.query("SELECT 1 AS n");
      assertEquals(1, rs.first().getInteger("n"));
    }
  }

  @Test
  void streamIterable() {
    try (var conn = connect()) {
      try (var rows = conn.stream("SELECT generate_series(1, 50) AS n")) {
        int count = 0;
        int sum = 0;
        for (Row row : rows) {
          count++;
          sum += row.getInteger("n");
        }
        assertEquals(50, count);
        assertEquals(1275, sum); // 1+2+...+50
      }
    }
  }

  @Test
  void streamWithParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE stream_iter (id int, val text)");
      conn.query("INSERT INTO stream_iter VALUES (1, 'a'), (2, 'b'), (3, 'c')");

      try (var rows = conn.stream("SELECT val FROM stream_iter WHERE id <= $1 ORDER BY id", 2)) {
        var vals = rows.stream().map(r -> r.getString("val")).toList();
        assertEquals(List.of("a", "b"), vals);
      }
    }
  }

  @Test
  void streamCloseEarly() {
    try (var conn = connect()) {
      // Close the stream before consuming all rows -- should drain properly
      try (var rows = conn.stream("SELECT generate_series(1, 1000) AS n")) {
        var iter = rows.iterator();
        assertTrue(iter.hasNext());
        assertEquals(1, iter.next().getInteger("n"));
        // Close without consuming the rest
      }
      // Connection should still work
      var rs = conn.query("SELECT 42 AS n");
      assertEquals(42, rs.first().getInteger("n"));
    }
  }

  @Test
  void streamConnectionUsableAfter() {
    try (var conn = connect()) {
      try (var rows = conn.stream("SELECT generate_series(1, 5) AS n")) {
        rows.forEach(row -> {}); // consume all
      }
      // Normal query should work after stream is consumed and closed
      var rs = conn.query("SELECT 'ok' AS status");
      assertEquals("ok", rs.first().getString("status"));
    }
  }

  // ===== Postgres-specific error scenario tests =====

  @Test
  void pgExceptionContainsConstraintInfo() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pg_err_test (id int PRIMARY KEY, name text NOT NULL)");
      conn.query("INSERT INTO pg_err_test VALUES (1, 'Alice')");
      var ex =
          assertThrows(
              PgException.class, () -> conn.query("INSERT INTO pg_err_test VALUES (1, 'Bob')"));
      assertNotNull(ex.constraint());
      assertEquals("23505", ex.sqlState());
    }
  }

  @Test
  void pgExceptionNotNullViolation() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nn_test (id int, name text NOT NULL)");
      var ex =
          assertThrows(PgException.class, () -> conn.query("INSERT INTO nn_test VALUES (1, NULL)"));
      assertEquals("23502", ex.sqlState());
      assertNotNull(ex.column());
    }
  }

  @Test
  void pgExceptionDetailAndTable() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE detail_test (id int PRIMARY KEY)");
      conn.query("INSERT INTO detail_test VALUES (1)");
      var ex =
          assertThrows(PgException.class, () -> conn.query("INSERT INTO detail_test VALUES (1)"));
      assertNotNull(ex.detail()); // "Key (id)=(1) already exists."
      assertNotNull(ex.table()); // "detail_test"
      assertNotNull(ex.schema());
      assertEquals("detail_test", ex.table());
    }
  }

  @Test
  void pgExceptionToStringIncludesDetail() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tostr_test (id int PRIMARY KEY)");
      conn.query("INSERT INTO tostr_test VALUES (1)");
      var ex =
          assertThrows(PgException.class, () -> conn.query("INSERT INTO tostr_test VALUES (1)"));
      String s = ex.toString();
      assertTrue(s.contains("Detail:"));
      assertTrue(s.contains("SQL:"));
    }
  }

  @Test
  void parameterizedQueryErrorContainsSql() {
    try (var conn = connect()) {
      String sql = "SELECT $1::int";
      var ex = assertThrows(DbException.class, () -> conn.query(sql, "not_a_number"));
      assertEquals(sql, ex.sql());
    }
  }

  @Test
  void undefinedFunctionError() {
    try (var conn = connect()) {
      var ex =
          assertThrows(PgException.class, () -> conn.query("SELECT nonexistent_function_xyz()"));
      assertNotNull(ex.sqlState());
      // Connection should recover
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  @Test
  void typeMismatchInParameterizedQuery() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE type_test (id int, created_at timestamp)");
      var ex =
          assertThrows(
              DbException.class,
              () -> conn.query("INSERT INTO type_test VALUES ($1, $2)", 1, "not-a-timestamp"));
      assertNotNull(ex.getMessage());
      assertEquals("INSERT INTO type_test VALUES ($1, $2)", ex.sql());
      // Connection should recover
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  @Test
  void closedConnectionThrowsDbConnectionException() {
    var conn = connect();
    conn.close();
    assertThrows(DbConnectionException.class, () -> conn.query("SELECT 1"));
  }

  @Test
  void preparedStatementWithInvalidSqlContainsSql() {
    try (var conn = connect()) {
      var ex =
          assertThrows(
              PgException.class, () -> conn.prepare("SELECT * FROM totally_missing_table_xyz"));
      assertNotNull(ex.getMessage());
    }
  }

  @Test
  void cursorErrorInTransaction() {
    try (var conn = connect()) {
      try (var tx = conn.begin()) {
        assertThrows(PgException.class, () -> tx.cursor("SELECT * FROM nonexistent_cursor_table"));
      }
      // Connection usable after
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  @Test
  void streamingQueryError() {
    try (var conn = connect()) {
      assertThrows(
          PgException.class,
          () -> conn.queryStream("SELECT * FROM nonexistent_streaming_xyz", row -> {}));
      // Connection usable after
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // ===== Cursor: large result set with progressive reading =====

  @Test
  void cursorLargeResultProgressiveRead() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_large AS SELECT generate_series(1, 10000) AS n");

      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_large ORDER BY n")) {
        long sum = 0;
        int totalRows = 0;
        while (cursor.hasMore()) {
          var batch = cursor.read(500);
          for (var row : batch) {
            sum += row.getInteger(0);
            totalRows++;
          }
        }
        assertEquals(10000, totalRows);
        assertEquals(50005000L, sum); // n*(n+1)/2
      }
      conn.query("COMMIT");
    }
  }

  @Test
  void cursorReadExactBatchBoundary() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_exact AS SELECT generate_series(1, 100) AS n");

      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_exact ORDER BY n")) {
        // Read exactly the number of rows available
        var batch = cursor.read(100);
        assertEquals(100, batch.size());

        // Next read should return empty and hasMore false
        var batch2 = cursor.read(100);
        assertEquals(0, batch2.size());
        assertFalse(cursor.hasMore());
      }
      conn.query("COMMIT");
    }
  }

  @Test
  void cursorMultipleCursorsSequentially() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_multi AS SELECT generate_series(1, 50) AS n");

      conn.query("BEGIN");

      try (var c1 = conn.cursor("SELECT n FROM cursor_multi WHERE n <= $1 ORDER BY n", 25)) {
        int count = 0;
        while (c1.hasMore()) {
          count += c1.read(10).size();
        }
        assertEquals(25, count);
      }

      try (var c2 = conn.cursor("SELECT n FROM cursor_multi WHERE n > $1 ORDER BY n", 25)) {
        int count = 0;
        while (c2.hasMore()) {
          count += c2.read(10).size();
        }
        assertEquals(25, count);
      }

      conn.query("COMMIT");
    }
  }

  @Test
  void cursorConnectionUsableAfterClose() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_reuse AS SELECT generate_series(1, 100) AS n");

      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT n FROM cursor_reuse ORDER BY n")) {
        cursor.read(10); // read partial
        // cursor.close() called implicitly by try-with-resources
      }
      conn.query("COMMIT");

      // Connection should be fully usable after cursor close
      var rs = conn.query("SELECT 42 AS answer");
      assertEquals(42, rs.first().getInteger("answer"));
    }
  }

  // ===== Streaming: comprehensive tests =====

  @Test
  void streamLargeResultConstantMemory() {
    try (var conn = connect()) {
      long sum = 0;
      int count = 0;
      try (var rows = conn.stream("SELECT generate_series(1, 10000) AS n")) {
        for (var row : rows) {
          sum += row.getLong("n");
          count++;
        }
      }
      assertEquals(10000, count);
      assertEquals(50005000L, sum);
    }
  }

  @Test
  void streamWithParamsAndFilter() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE stream_filter (id int, category text)");
      for (int i = 1; i <= 100; i++) {
        conn.enqueue("INSERT INTO stream_filter VALUES ($1, $2)", i, i % 2 == 0 ? "even" : "odd");
      }
      conn.flush();

      List<Integer> evenIds = new ArrayList<>();
      try (var rows =
          conn.stream("SELECT id FROM stream_filter WHERE category = $1 ORDER BY id", "even")) {
        for (var row : rows) {
          evenIds.add(row.getInteger("id"));
        }
      }

      assertEquals(50, evenIds.size());
      assertEquals(2, evenIds.getFirst());
      assertEquals(100, evenIds.getLast());
    }
  }

  @Test
  void streamToJavaStream() {
    try (var conn = connect()) {
      try (var rows = conn.stream("SELECT generate_series(1, 100) AS n")) {
        long sum = rows.stream().mapToLong(r -> r.getLong("n")).sum();
        assertEquals(5050L, sum);
      }
    }
  }

  @Test
  void streamCloseBeforeFullConsumption() {
    try (var conn = connect()) {
      try (var rows = conn.stream("SELECT generate_series(1, 10000) AS n")) {
        var iter = rows.iterator();
        // Only read a few rows
        for (int i = 0; i < 5; i++) {
          assertTrue(iter.hasNext());
          iter.next();
        }
        // Close without consuming the rest -- should drain properly
      }

      // Connection must be usable after early stream close
      var rs = conn.query("SELECT 'after_stream' AS val");
      assertEquals("after_stream", rs.first().getString("val"));
    }
  }

  @Test
  void queryStreamCallbackCount() {
    try (var conn = connect()) {
      int[] count = {0};
      conn.queryStream(
          "SELECT generate_series(1, 500) AS n",
          row -> {
            assertNotNull(row.getInteger("n"));
            count[0]++;
          });
      assertEquals(500, count[0]);
    }
  }

  @Test
  void queryStreamWithParamsCallback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE qs_param (id int, val text)");
      conn.query("INSERT INTO qs_param VALUES (1, 'x'), (2, 'y'), (3, 'z')");

      List<String> vals = new ArrayList<>();
      conn.queryStream(
          "SELECT val FROM qs_param WHERE id >= $1 ORDER BY id",
          new Object[] {2},
          row -> vals.add(row.getString("val")));

      assertEquals(2, vals.size());
      assertEquals("y", vals.get(0));
      assertEquals("z", vals.get(1));
    }
  }

  // ===== SSL/TLS connection tests =====

  @Test
  void connectWithSslModeDisable() {
    // Explicit SSL disable should work (default behavior)
    var config =
        ConnectionConfig.fromUri(
            "postgresql://"
                + pg.getUsername()
                + ":"
                + pg.getPassword()
                + "@"
                + pg.getHost()
                + ":"
                + pg.getMappedPort(5432)
                + "/"
                + pg.getDatabaseName()
                + "?sslmode=disable");
    try (var conn = PgConnection.connect(config)) {
      var rs = conn.query("SELECT 1 AS n");
      assertEquals(1, rs.first().getInteger("n"));
    }
  }

  @Test
  void connectWithSslModePreferFallsBackToPlaintext() {
    // The default Testcontainers Postgres does not have SSL configured,
    // so sslmode=prefer should fall back to plaintext
    var config =
        ConnectionConfig.fromUri(
            "postgresql://"
                + pg.getUsername()
                + ":"
                + pg.getPassword()
                + "@"
                + pg.getHost()
                + ":"
                + pg.getMappedPort(5432)
                + "/"
                + pg.getDatabaseName()
                + "?sslmode=prefer");
    try (var conn = PgConnection.connect(config)) {
      var rs = conn.query("SELECT 1 AS n");
      assertEquals(1, rs.first().getInteger("n"));
    }
  }

  @Test
  void connectWithSslModeRequireFailsWhenServerHasNoSsl() {
    // Testcontainers Postgres without SSL should reject sslmode=require
    var config =
        ConnectionConfig.fromUri(
            "postgresql://"
                + pg.getUsername()
                + ":"
                + pg.getPassword()
                + "@"
                + pg.getHost()
                + ":"
                + pg.getMappedPort(5432)
                + "/"
                + pg.getDatabaseName()
                + "?sslmode=require");
    assertThrows(DbConnectionException.class, () -> PgConnection.connect(config));
  }

  // ===== Stale cached plan retry =====

  @Test
  void staleCachedPlanRetries() {
    try (var conn =
        PgConnection.connect(
            new ConnectionConfig(
                    pg.getHost(),
                    pg.getMappedPort(5432),
                    pg.getDatabaseName(),
                    pg.getUsername(),
                    pg.getPassword())
                .cachePreparedStatements(true))) {
      conn.query("CREATE TEMP TABLE stale_test (id int, name text)");
      conn.query("INSERT INTO stale_test VALUES (1, 'Alice')");

      // Cache the prepared statement
      var rs1 = conn.query("SELECT id, name FROM stale_test WHERE id = $1", 1);
      assertEquals("Alice", rs1.first().getString("name"));

      // Alter the table -- changes result type, invalidating cached plan
      conn.query("ALTER TABLE stale_test ADD COLUMN age int DEFAULT 0");

      // This should succeed via automatic re-preparation
      var rs2 = conn.query("SELECT id, name FROM stale_test WHERE id = $1", 1);
      assertEquals("Alice", rs2.first().getString("name"));
    }
  }

  // ===== Adaptive cursor fetch size =====

  @Test
  void adaptiveCursorFetchSize() {
    try (var conn =
        PgConnection.connect(
            new ConnectionConfig(
                    pg.getHost(),
                    pg.getMappedPort(5432),
                    pg.getDatabaseName(),
                    pg.getUsername(),
                    pg.getPassword())
                .maxResultBufferBytes(4096))) {
      conn.query(
          "CREATE TEMP TABLE adaptive_test AS"
              + " SELECT generate_series(1, 500) AS id, repeat('x', 100) AS data");
      conn.query("BEGIN");
      try (var cursor = conn.cursor("SELECT * FROM adaptive_test")) {
        // First read: adaptive sizing not yet active (no previous batch stats)
        RowSet batch1 = cursor.read(10);
        assertEquals(10, batch1.size());
        assertTrue(cursor.hasMore());

        // Second read: adaptive sizing kicks in based on observed row size from batch1
        // With 4KB buffer and ~100+ byte rows per column, should limit below 1000
        RowSet batch2 = cursor.read(1000);
        assertTrue(batch2.size() > 0);
        assertTrue(
            batch2.size() < 500, "Adaptive fetch should limit batch size, got " + batch2.size());

        // Keep reading until done
        int total = batch1.size() + batch2.size();
        while (cursor.hasMore()) {
          total += cursor.read(1000).size();
        }
        assertEquals(500, total);
      }
      conn.query("COMMIT");
    }
  }

  // ===== Streaming row recycling =====

  @Test
  void streamingRowRecycling() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE recycle_test AS SELECT generate_series(1, 100) AS id");
      var collected = new ArrayList<Integer>();
      conn.queryStream(
          "SELECT id FROM recycle_test ORDER BY id",
          row -> {
            collected.add(row.getInteger(0));
          });
      assertEquals(100, collected.size());
      assertEquals(1, collected.get(0));
      assertEquals(100, collected.get(99));
    }
  }

  // ===== Binary parameter encoding round-trips =====

  @Test
  void paramLocalDateRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_date (d date)");
      var date = java.time.LocalDate.of(2024, 6, 15);
      conn.query("INSERT INTO param_date VALUES ($1)", date);
      assertEquals(date, conn.query("SELECT d FROM param_date").first().getLocalDate("d"));
    }
  }

  @Test
  void paramLocalDateBeforeEpoch() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_date2 (d date)");
      var date = java.time.LocalDate.of(1970, 1, 1);
      conn.query("INSERT INTO param_date2 VALUES ($1)", date);
      assertEquals(date, conn.query("SELECT d FROM param_date2").first().getLocalDate("d"));
    }
  }

  @Test
  void paramLocalTimeRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_time (t time)");
      var time = java.time.LocalTime.of(13, 45, 30, 123_456_000);
      conn.query("INSERT INTO param_time VALUES ($1)", time);
      assertEquals(time, conn.query("SELECT t FROM param_time").first().getLocalTime("t"));
    }
  }

  @Test
  void paramLocalDateTimeRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_ts (ts timestamp)");
      var ts = java.time.LocalDateTime.of(2024, 6, 15, 13, 45, 30, 123_456_000);
      conn.query("INSERT INTO param_ts VALUES ($1)", ts);
      assertEquals(ts, conn.query("SELECT ts FROM param_ts").first().getLocalDateTime("ts"));
    }
  }

  @Test
  void paramLocalDateTimeBeforeEpoch() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_ts2 (ts timestamp)");
      var ts = java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 0);
      conn.query("INSERT INTO param_ts2 VALUES ($1)", ts);
      assertEquals(ts, conn.query("SELECT ts FROM param_ts2").first().getLocalDateTime("ts"));
    }
  }

  @Test
  void paramOffsetDateTimeRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_tstz (ts timestamptz)");
      var odt =
          java.time.OffsetDateTime.of(2024, 6, 15, 15, 45, 30, 0, java.time.ZoneOffset.ofHours(2));
      conn.query("INSERT INTO param_tstz VALUES ($1)", odt);
      var result = conn.query("SELECT ts FROM param_tstz").first().getOffsetDateTime("ts");
      // PG normalizes to UTC
      var expected =
          java.time.OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, java.time.ZoneOffset.UTC);
      assertEquals(expected, result);
    }
  }

  @Test
  void paramOffsetTimeRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_timetz (t timetz)");
      var ot =
          java.time.OffsetTime.of(
              java.time.LocalTime.of(17, 55, 4), java.time.ZoneOffset.ofHours(2));
      conn.query("INSERT INTO param_timetz VALUES ($1)", ot);
      var result = conn.query("SELECT t FROM param_timetz").first().getOffsetTime("t");
      assertEquals(ot, result);
    }
  }

  @Test
  void paramOffsetTimeNegativeOffset() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_timetz2 (t timetz)");
      var ot =
          java.time.OffsetTime.of(
              java.time.LocalTime.of(8, 15, 30), java.time.ZoneOffset.ofHours(-5));
      conn.query("INSERT INTO param_timetz2 VALUES ($1)", ot);
      var result = conn.query("SELECT t FROM param_timetz2").first().getOffsetTime("t");
      assertEquals(ot, result);
    }
  }

  @Test
  void paramBigDecimalRoundTrip() {
    try (var conn = connect()) {
      var value = new java.math.BigDecimal("12345.6789");
      var rs = conn.query("SELECT $1::numeric AS val", value);
      assertEquals(0, value.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void paramBigDecimalNegative() {
    try (var conn = connect()) {
      var value = new java.math.BigDecimal("-42.5");
      var rs = conn.query("SELECT $1::numeric AS val", value);
      assertEquals(0, value.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void paramBigDecimalZero() {
    try (var conn = connect()) {
      var value = java.math.BigDecimal.ZERO;
      var rs = conn.query("SELECT $1::numeric AS val", value);
      assertEquals(0, value.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void paramBigDecimalSmallFraction() {
    try (var conn = connect()) {
      var value = new java.math.BigDecimal("0.00000001");
      var rs = conn.query("SELECT $1::numeric AS val", value);
      assertEquals(0, value.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void paramBigDecimalLarge() {
    try (var conn = connect()) {
      var value = new java.math.BigDecimal("99999999999999999999.99999999999999999999");
      var rs = conn.query("SELECT $1::numeric AS val", value);
      assertEquals(0, value.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void paramBigDecimalArithmetic() {
    try (var conn = connect()) {
      // Verify PG can do arithmetic with binary-encoded numeric params
      var a = new java.math.BigDecimal("100.50");
      var b = new java.math.BigDecimal("200.25");
      var rs = conn.query("SELECT $1::numeric + $2::numeric AS sum", a, b);
      assertEquals(
          0, new java.math.BigDecimal("300.75").compareTo(rs.first().getBigDecimal("sum")));
    }
  }

  // --- Binary array parameter round-trips ---

  @Test
  void paramIntArrayRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_iarr (vals int[])");
      conn.query("INSERT INTO param_iarr VALUES ($1)", new int[] {1, 2, 3});
      var arr = conn.query("SELECT vals FROM param_iarr").first().getIntegerArray("vals");
      assertEquals(List.of(1, 2, 3), arr);
    }
  }

  @Test
  void paramIntArrayAny() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_iany (id int)");
      conn.query("INSERT INTO param_iany VALUES (1), (2), (3), (4), (5)");
      var rs =
          conn.query(
              "SELECT id FROM param_iany WHERE id = ANY($1) ORDER BY id", new int[] {1, 3, 5});
      var ids = new ArrayList<Integer>();
      for (var row : rs) ids.add(row.getInteger("id"));
      assertEquals(List.of(1, 3, 5), ids);
    }
  }

  @Test
  void paramLongArrayRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_larr (vals bigint[])");
      conn.query("INSERT INTO param_larr VALUES ($1)", new long[] {100L, 200L});
      var arr = conn.query("SELECT vals FROM param_larr").first().getLongArray("vals");
      assertEquals(List.of(100L, 200L), arr);
    }
  }

  @Test
  void paramBoolArrayRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_barr (vals boolean[])");
      conn.query("INSERT INTO param_barr VALUES ($1)", new boolean[] {true, false, true});
      var arr = conn.query("SELECT vals FROM param_barr").first().getBooleanArray("vals");
      assertEquals(List.of(true, false, true), arr);
    }
  }

  @Test
  void paramDoubleArrayRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_darr (vals float8[])");
      conn.query("INSERT INTO param_darr VALUES ($1)", new double[] {1.5, 2.5});
      var arr = conn.query("SELECT vals FROM param_darr").first().getDoubleArray("vals");
      assertEquals(List.of(1.5, 2.5), arr);
    }
  }

  @Test
  void paramIntegerListRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_ilist (vals int[])");
      conn.query("INSERT INTO param_ilist VALUES ($1)", List.of(10, 20, 30));
      var arr = conn.query("SELECT vals FROM param_ilist").first().getIntegerArray("vals");
      assertEquals(List.of(10, 20, 30), arr);
    }
  }

  @Test
  void paramStringListRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_slist (vals text[])");
      conn.query("INSERT INTO param_slist VALUES ($1)", List.of("hello", "world"));
      var arr = conn.query("SELECT vals FROM param_slist").first().getStringArray("vals");
      assertEquals(List.of("hello", "world"), arr);
    }
  }

  @Test
  void paramStringListWithSpecialChars() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_sspec (vals text[])");
      conn.query(
          "INSERT INTO param_sspec VALUES ($1)",
          List.of("hello world", "with,comma", "with\"quote"));
      var arr = conn.query("SELECT vals FROM param_sspec").first().getStringArray("vals");
      assertEquals(List.of("hello world", "with,comma", "with\"quote"), arr);
    }
  }

  @Test
  void paramUuidListRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_ulist (vals uuid[])");
      var u1 = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
      var u2 = java.util.UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
      conn.query("INSERT INTO param_ulist VALUES ($1)", List.of(u1, u2));
      var arr = conn.query("SELECT vals FROM param_ulist").first().getUuidArray("vals");
      assertEquals(List.of(u1, u2), arr);
    }
  }

  @Test
  void paramEmptyIntArrayRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_earr (vals int[])");
      conn.query("INSERT INTO param_earr VALUES ($1)", new int[] {});
      var arr = conn.query("SELECT vals FROM param_earr").first().getIntegerArray("vals");
      assertEquals(List.of(), arr);
    }
  }

  @Test
  void paramIntegerListAny() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_lany (id int)");
      conn.query("INSERT INTO param_lany VALUES (1), (2), (3), (4), (5)");
      var rs =
          conn.query("SELECT id FROM param_lany WHERE id = ANY($1) ORDER BY id", List.of(2, 4));
      var ids = new ArrayList<Integer>();
      for (var row : rs) ids.add(row.getInteger("id"));
      assertEquals(List.of(2, 4), ids);
    }
  }

  @Test
  void paramListWithNulls() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_lnull (vals int[])");
      var list = new ArrayList<Integer>();
      list.add(1);
      list.add(null);
      list.add(3);
      conn.query("INSERT INTO param_lnull VALUES ($1)", list);
      var arr = conn.query("SELECT vals FROM param_lnull").first().getIntegerArray("vals");
      assertEquals(3, arr.size());
      assertEquals(1, arr.get(0));
      assertNull(arr.get(1));
      assertEquals(3, arr.get(2));
    }
  }

  // --- Mixed binary/text params in same query ---

  @Test
  void paramMixedBinaryAndTextTypes() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE param_mix (d date, n numeric, s text, i int)");
      var date = java.time.LocalDate.of(2024, 1, 15);
      var num = new java.math.BigDecimal("99.99");
      conn.query("INSERT INTO param_mix VALUES ($1, $2, $3, $4)", date, num, "hello", 42);
      var row = conn.query("SELECT * FROM param_mix").first();
      assertEquals(date, row.getLocalDate("d"));
      assertEquals(0, num.compareTo(row.getBigDecimal("n")));
      assertEquals("hello", row.getString("s"));
      assertEquals(42, row.getInteger("i"));
    }
  }

  // --- Prepared statement with binary-encoded params ---

  @Test
  void preparedStatementBinaryParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE ps_bin (d date, n numeric, ts timestamptz)");
      try (var stmt = conn.prepare("INSERT INTO ps_bin VALUES ($1, $2, $3)")) {
        var date = java.time.LocalDate.of(2024, 6, 15);
        var num = new java.math.BigDecimal("123.45");
        var odt = java.time.OffsetDateTime.of(2024, 6, 15, 12, 0, 0, 0, java.time.ZoneOffset.UTC);
        stmt.query(date, num, odt);
      }
      var row = conn.query("SELECT * FROM ps_bin").first();
      assertEquals(java.time.LocalDate.of(2024, 6, 15), row.getLocalDate("d"));
      assertEquals(0, new java.math.BigDecimal("123.45").compareTo(row.getBigDecimal("n")));
      assertEquals(
          java.time.OffsetDateTime.of(2024, 6, 15, 12, 0, 0, 0, java.time.ZoneOffset.UTC),
          row.getOffsetDateTime("ts"));
    }
  }

  @Test
  void preparedStatementWithArrayParam() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE ps_arr (id int)");
      conn.query("INSERT INTO ps_arr VALUES (1), (2), (3), (4), (5)");
      try (var stmt = conn.prepare("SELECT id FROM ps_arr WHERE id = ANY($1) ORDER BY id")) {
        var rs = stmt.query(List.of(1, 3, 5));
        var ids = new ArrayList<Integer>();
        for (var row : rs) ids.add(row.getInteger("id"));
        assertEquals(List.of(1, 3, 5), ids);

        // Re-execute with different values
        var rs2 = stmt.query(List.of(2, 4));
        var ids2 = new ArrayList<Integer>();
        for (var row : rs2) ids2.add(row.getInteger("id"));
        assertEquals(List.of(2, 4), ids2);
      }
    }
  }

  // --- Pipelined with binary-encoded params ---

  @Test
  void pipelinedBinaryParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE pipe_bin (d date, n numeric)");
      var d1 = java.time.LocalDate.of(2024, 1, 1);
      var d2 = java.time.LocalDate.of(2024, 12, 31);
      conn.enqueue("INSERT INTO pipe_bin VALUES ($1, $2)", d1, new java.math.BigDecimal("100.00"));
      conn.enqueue("INSERT INTO pipe_bin VALUES ($1, $2)", d2, new java.math.BigDecimal("200.00"));
      conn.flush();
      var dates = new ArrayList<java.time.LocalDate>();
      for (var row : conn.query("SELECT * FROM pipe_bin ORDER BY d")) {
        dates.add(row.getLocalDate("d"));
      }
      assertEquals(List.of(d1, d2), dates);
    }
  }

  // ===== Prepared statement cache eviction =====

  @Test
  void preparedStatementCacheEviction() {
    try (var conn =
        PgConnection.connect(
            ConnectionConfig.fromUri(
                    "postgresql://"
                        + pg.getUsername()
                        + ":"
                        + pg.getPassword()
                        + "@"
                        + pg.getHost()
                        + ":"
                        + pg.getMappedPort(5432)
                        + "/"
                        + pg.getDatabaseName())
                .cachePreparedStatements(true))) {
      // Execute 300 distinct parameterized queries to stress the cache
      for (int i = 0; i < 300; i++) {
        var rs = conn.query("SELECT $1::int + " + i + " AS n", 1);
        assertEquals(1 + i, rs.first().getInteger("n"));
      }
      // Connection should still work normally after cache eviction
      var rs = conn.query("SELECT 42 AS answer");
      assertEquals(42, rs.first().getInteger("answer"));
    }
  }

  // =====================================================================
  // Override inherited pipeline error tests -- errors cancel subsequent statements in the same
  // Sync boundary when using the extended query protocol.
  // =====================================================================

  @Override
  @Test
  protected void pipelineErrorDoesNotPoisonSubsequent() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT * FROM nonexistent_table_xyz");
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(3, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertNotNull(results.get(1).getError());
      // In extended protocol, error at index 1 cancels index 2
      assertNotNull(results.get(2).getError());
    }
  }

  @Override
  @Test
  protected void pipelineMultipleErrorsAndRecovery() {
    try (var conn = connect()) {
      conn.enqueue("SELECT 1");
      conn.enqueue("SELECT * FROM no_such_table_aaa");
      conn.enqueue("SELECT 2");
      conn.enqueue("SELECT * FROM no_such_table_bbb");
      conn.enqueue("SELECT 3");
      List<RowSet> results = conn.flush();

      assertEquals(5, results.size());
      assertEquals(1, results.get(0).first().getInteger(0));
      assertNotNull(results.get(1).getError());
      // Statements 2-4 are skipped due to the error at index 1
      assertNotNull(results.get(2).getError());
      assertNotNull(results.get(3).getError());
      assertNotNull(results.get(4).getError());

      // Connection should still be usable after pipeline with errors
      var rs = conn.query("SELECT 42");
      assertEquals(42, rs.first().getInteger(0));
    }
  }

  // ===== Server-forced connection close =====

  @Test
  void queryAfterServerTerminatesBackend() {
    try (var conn = connect()) {
      int pid = conn.processId();

      // Kill the backend from another connection
      try (var admin = connect()) {
        admin.query("SELECT pg_terminate_backend(" + pid + ")");
      }

      // Small delay for termination to take effect
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Next query should fail with a connection exception
      assertThrows(Exception.class, () -> conn.query("SELECT 1"));
    }
  }

  // ===== NOTIFY does not corrupt connection =====

  @Test
  void notificationDoesNotCorruptConnection() {
    try (var conn = connect()) {
      conn.query("LISTEN test_channel");

      // Send notification from another connection
      try (var notifier = connect()) {
        notifier.query("NOTIFY test_channel, 'hello'");
      }

      // Small delay for notification delivery
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Normal query should still work -- notification is silently consumed
      var rs = conn.query("SELECT 42 AS answer");
      assertEquals(42, rs.first().getInteger("answer"));

      conn.query("UNLISTEN test_channel");
    }
  }

  // =====================================================================
  // TypeRegistry: custom types via binary encoding
  // =====================================================================

  @Test
  void typeRegistryCustomTypeRoundTrip() {
    record UserId(java.util.UUID uuid) {}
    try (var conn = connect()) {
      conn.typeRegistry().register(UserId.class, java.util.UUID.class, UserId::uuid, UserId::new);
      conn.query("CREATE TEMP TABLE pe_test (id uuid)");
      var uid = new UserId(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
      conn.query("INSERT INTO pe_test VALUES ($1)", uid);
      var result = conn.query("SELECT id FROM pe_test").first().getUUID("id");
      assertEquals(uid.uuid(), result);
    }
  }

  @Test
  void typeRegistryWithPreparedStatement() {
    record UserId(java.util.UUID uuid) {}
    try (var conn = connect()) {
      conn.typeRegistry().register(UserId.class, java.util.UUID.class, UserId::uuid, UserId::new);
      conn.query("CREATE TEMP TABLE pe_ps (id uuid, name text)");
      conn.query("INSERT INTO pe_ps VALUES ('550e8400-e29b-41d4-a716-446655440000', 'Alice')");
      try (var stmt = conn.prepare("SELECT name FROM pe_ps WHERE id = $1")) {
        var uid = new UserId(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        var rs = stmt.query(uid);
        assertEquals("Alice", rs.first().getString("name"));
      }
    }
  }

  @Test
  void typeRegistryMoneyToBigDecimal() {
    record Money(java.math.BigDecimal amount) {}
    try (var conn = connect()) {
      conn.typeRegistry()
          .register(Money.class, java.math.BigDecimal.class, Money::amount, Money::new);
      var price = new Money(new java.math.BigDecimal("99.99"));
      var rs = conn.query("SELECT $1::numeric AS val", price);
      assertEquals(0, new java.math.BigDecimal("99.99").compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void typeRegistryInPipeline() {
    record UserId(java.util.UUID uuid) {}
    try (var conn = connect()) {
      conn.typeRegistry().register(UserId.class, java.util.UUID.class, UserId::uuid, null);
      conn.query("CREATE TEMP TABLE pe_pipe (id uuid)");
      var u1 = new UserId(java.util.UUID.randomUUID());
      var u2 = new UserId(java.util.UUID.randomUUID());
      conn.enqueue("INSERT INTO pe_pipe VALUES ($1)", u1);
      conn.enqueue("INSERT INTO pe_pipe VALUES ($1)", u2);
      conn.flush();
      assertEquals(2, conn.query("SELECT count(*) FROM pe_pipe").first().getInteger(0));
    }
  }
}
