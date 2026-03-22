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
import io.github.bpdbi.pg.data.BitString;
import io.github.bpdbi.pg.data.Box;
import io.github.bpdbi.pg.data.Cidr;
import io.github.bpdbi.pg.data.Circle;
import io.github.bpdbi.pg.data.Inet;
import io.github.bpdbi.pg.data.Interval;
import io.github.bpdbi.pg.data.Line;
import io.github.bpdbi.pg.data.LineSegment;
import io.github.bpdbi.pg.data.Macaddr;
import io.github.bpdbi.pg.data.Macaddr8;
import io.github.bpdbi.pg.data.Money;
import io.github.bpdbi.pg.data.Path;
import io.github.bpdbi.pg.data.Point;
import io.github.bpdbi.pg.data.Polygon;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Integration tests for Connection using Testcontainers Postgres. Ported from vertx-sql-client's
 * SimpleQueryTestBase, PipeliningQueryTestBase, TransactionTestBase, and PgConnectionTestBase.
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

  // ===== Simple query protocol =====

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

  // ===== Data types (text format) =====

  @Test
  void dataTypeNumeric() {
    try (var conn = connect()) {
      var rs =
          conn.query(
              "SELECT 42::int2 AS s, 42::int4 AS i, 42::int8 AS l, 3.14::float4 AS f, 3.14::float8 AS d, 123.456::numeric AS n");
      var row = rs.first();
      assertEquals((short) 42, row.getShort(0));
      assertEquals(42, row.getInteger("i"));
      assertEquals(42L, row.getLong("l"));
      assertEquals(3.14f, row.getFloat(3), 0.01f);
      assertEquals(3.14, row.getDouble("d"), 0.001);
      assertEquals(new java.math.BigDecimal("123.456"), row.getBigDecimal("n"));
    }
  }

  @Test
  void dataTypeText() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'hello'::text AS t, 'world'::varchar AS v, 'x'::char(1) AS c");
      var row = rs.first();
      assertEquals("hello", row.getString("t"));
      assertEquals("world", row.getString("v"));
      assertEquals("x", row.getString("c"));
    }
  }

  @Test
  void dataTypeUUID() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u");
      assertEquals(
          java.util.UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
          rs.first().getUUID("u"));
    }
  }

  @Test
  void dataTypeDateAndTime() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2024-01-15'::date AS d, '13:45:30'::time AS t");
      var row = rs.first();
      assertEquals(java.time.LocalDate.of(2024, 1, 15), row.getLocalDate(0));
      assertEquals(java.time.LocalTime.of(13, 45, 30), row.getLocalTime(1));
    }
  }

  @Test
  void dataTypeBytea() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '\\xDEADBEEF'::bytea AS b");
      byte[] bytes = rs.first().getBytes("b");
      assertEquals(4, bytes.length);
      assertEquals((byte) 0xDE, bytes[0]);
      assertEquals((byte) 0xAD, bytes[1]);
      assertEquals((byte) 0xBE, bytes[2]);
      assertEquals((byte) 0xEF, bytes[3]);
    }
  }

  @Test
  void dataTypeNull() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT NULL::int AS n");
      assertTrue(rs.first().isNull("n"));
      assertNull(rs.first().getInteger("n"));
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

  // ===== Additional data type tests (ported from vertx TextDataTypeDecodeTestBase + PG codec
  // tests) =====

  @Test
  void dataTypeInt2() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 32767::int2");
      assertEquals(Short.MAX_VALUE, rs.first().getShort(0));
    }
  }

  @Test
  void dataTypeInt4() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 2147483647::int4");
      assertEquals(Integer.MAX_VALUE, rs.first().getInteger(0));
    }
  }

  @Test
  void dataTypeInt8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 9223372036854775807::int8");
      assertEquals(Long.MAX_VALUE, rs.first().getLong(0));
    }
  }

  @Test
  void dataTypeFloat4() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 3.4028235E38::float4");
      assertEquals(Float.MAX_VALUE, rs.first().getFloat(0), 1e30f);
    }
  }

  @Test
  void dataTypeFloat8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1.7976931348623157E308::float8");
      assertEquals(Double.MAX_VALUE, rs.first().getDouble(0), 1e300);
    }
  }

  @Test
  void dataTypeNumericLarge() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 999999999999999999.999999999999::numeric");
      assertEquals(
          new java.math.BigDecimal("999999999999999999.999999999999"), rs.first().getBigDecimal(0));
    }
  }

  @Test
  void dataTypeSerial() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE serial_test (id serial, name text)");
      conn.query("INSERT INTO serial_test (name) VALUES ('a')");
      conn.query("INSERT INTO serial_test (name) VALUES ('b')");
      var rs = conn.query("SELECT id FROM serial_test ORDER BY id");
      assertEquals(2, rs.size());
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  @Test
  void dataTypeBlankPaddedChar() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'ab'::char(5) AS c");
      assertEquals("ab   ", rs.first().getString("c")); // blank-padded
    }
  }

  @Test
  void dataTypeVarchar() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'hello world'::varchar(50) AS v");
      assertEquals("hello world", rs.first().getString("v"));
    }
  }

  @Test
  void dataTypeName() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'pg_catalog'::name AS n");
      assertEquals("pg_catalog", rs.first().getString("n"));
    }
  }

  @Test
  void dataTypeDate() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15'::date AS d");
      assertEquals(java.time.LocalDate.of(2023, 6, 15), rs.first().getLocalDate(0));
    }
  }

  @Test
  void dataTypeTime() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '17:55:04.905120'::time AS t");
      var t = rs.first().getLocalTime(0);
      assertEquals(17, t.getHour());
      assertEquals(55, t.getMinute());
      assertEquals(4, t.getSecond());
    }
  }

  @Test
  void dataTypeTimestamp() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120'::timestamp AS ts");
      var ts = rs.first().getLocalDateTime(0);
      assertEquals(2023, ts.getYear());
      assertEquals(6, ts.getMonthValue());
      assertEquals(15, ts.getDayOfMonth());
      assertEquals(17, ts.getHour());
    }
  }

  @Test
  void dataTypeTimestamptz() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120+02'::timestamptz AS ts");
      var odt = rs.first().getOffsetDateTime("ts");
      assertNotNull(odt);
      // PG returns timestamptz in UTC
      assertEquals(2023, odt.getYear());
      assertEquals(6, odt.getMonthValue());
      assertEquals(15, odt.getHour()); // 17:55 +02 = 15:55 UTC
    }
  }

  @Test
  void dataTypeInstant() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120+02'::timestamptz AS ts");
      var instant = rs.first().getInstant("ts");
      assertNotNull(instant);
      var odt = java.time.OffsetDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
      assertEquals(2023, odt.getYear());
      assertEquals(15, odt.getHour()); // 17:55 +02 = 15:55 UTC
    }
  }

  @Test
  void dataTypeInstantRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE instant_rt (ts timestamptz)");
      var now = java.time.Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
      conn.query("INSERT INTO instant_rt VALUES ($1)", now);
      var rs = conn.query("SELECT ts FROM instant_rt");
      assertEquals(now, rs.first().getInstant("ts"));
    }
  }

  @Test
  void dataTypeTimetz() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '17:55:04+02'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(17, ot.getHour());
      assertEquals(55, ot.getMinute());
      assertEquals(4, ot.getSecond());
      assertEquals(java.time.ZoneOffset.ofHours(2), ot.getOffset());
    }
  }

  @Test
  void dataTypeTimetzUTC() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '12:30:00+00'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(12, ot.getHour());
      assertEquals(30, ot.getMinute());
      assertEquals(java.time.ZoneOffset.UTC, ot.getOffset());
    }
  }

  @Test
  void dataTypeTimetzNegativeOffset() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '08:15:30-05'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(8, ot.getHour());
      assertEquals(15, ot.getMinute());
      assertEquals(java.time.ZoneOffset.ofHours(-5), ot.getOffset());
    }
  }

  @Test
  void dataTypeJsonAsString() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '{\"key\":\"value\"}'::json AS j");
      assertEquals("{\"key\":\"value\"}", rs.first().getString("j"));
    }
  }

  @Test
  void dataTypeJsonbAsString() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '{\"key\": \"value\"}'::jsonb AS j");
      String json = rs.first().getString("j");
      assertTrue(json.contains("\"key\""));
      assertTrue(json.contains("\"value\""));
    }
  }

  // ===== NULL value encoding tests (ported from NullValueEncodeTestBase) =====

  @Test
  void parameterizedNullInt() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE null_test (id int, val int)");
      conn.query("INSERT INTO null_test VALUES ($1, $2)", 1, null);
      var rs = conn.query("SELECT val FROM null_test WHERE id = 1");
      assertTrue(rs.first().isNull(0));
    }
  }

  @Test
  void parameterizedNullText() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE null_text (id int, val text)");
      conn.query("INSERT INTO null_text VALUES ($1, $2)", 1, null);
      var rs = conn.query("SELECT val FROM null_text WHERE id = 1");
      assertTrue(rs.first().isNull(0));
      assertNull(rs.first().getString(0));
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

  // ===== Transaction tests (ported from TransactionTestBase) =====

  @Test
  void transactionCommitWithPreparedQuery() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_prep (id int, val text)");
      conn.query("BEGIN");
      conn.query("INSERT INTO tx_prep VALUES ($1, $2)", 1, "hello");
      conn.query("INSERT INTO tx_prep VALUES ($1, $2)", 2, "world");
      conn.query("COMMIT");

      var rs = conn.query("SELECT val FROM tx_prep ORDER BY id");
      assertEquals(2, rs.size());
      assertEquals("hello", rs.first().getString(0));
    }
  }

  @Test
  void transactionRollbackData() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_rollback (id int)");
      conn.query("BEGIN");
      conn.query("INSERT INTO tx_rollback VALUES (1)");
      conn.query("INSERT INTO tx_rollback VALUES (2)");
      conn.query("ROLLBACK");

      var rs = conn.query("SELECT count(*) FROM tx_rollback");
      assertEquals(0L, rs.first().getLong(0));
    }
  }

  @Test
  void transactionAbortOnError() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_abort (id int PRIMARY KEY)");
      conn.query("BEGIN");
      conn.query("INSERT INTO tx_abort VALUES (1)");
      try {
        conn.query("INSERT INTO tx_abort VALUES (1)"); // duplicate PK
        fail("Should have thrown");
      } catch (PgException e) {
        assertEquals("23505", e.sqlState()); // unique_violation
      }
      conn.query("ROLLBACK");

      var rs = conn.query("SELECT count(*) FROM tx_abort");
      assertEquals(0L, rs.first().getLong(0));
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
      // Provide 2 params but SQL has 1 placeholder — PG will error
      // (our text-format bind sends extra params, PG may ignore or error)
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

        // Re-execute with different size collection — same prepared statement
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

      // First call: cache miss — prepares and caches
      try (var stmt = conn.prepare(sql)) {
        assertEquals(1, stmt.query(Map.of("n", 1)).first().getInteger("n"));
      }

      // Second call: cache hit — named param mapping must survive
      try (var stmt = conn.prepare(sql)) {
        assertEquals(42, stmt.query(Map.of("n", 42)).first().getInteger("n"));
      }
    }
  }

  // ===== Transaction interface =====

  @Test
  void transactionInterfaceCommit() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_if (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO tx_if VALUES (1)");
        tx.query("INSERT INTO tx_if VALUES (2)");
        tx.commit();
      }
      assertEquals(2L, conn.query("SELECT count(*) FROM tx_if").first().getLong(0));
    }
  }

  @Test
  void transactionInterfaceAutoRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_ar (id int)");
      conn.query("INSERT INTO tx_ar VALUES (1)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO tx_ar VALUES (2)");
        // no commit — should auto-rollback
      }
      assertEquals(1L, conn.query("SELECT count(*) FROM tx_ar").first().getLong(0));
    }
  }

  @Test
  void transactionInterfaceExplicitRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_er (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO tx_er VALUES (1)");
        tx.rollback();
      }
      assertEquals(0L, conn.query("SELECT count(*) FROM tx_er").first().getLong(0));
    }
  }

  @Test
  void transactionInterfaceWithPipelining() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_pipe (id int)");
      try (var tx = conn.begin()) {
        tx.enqueue("INSERT INTO tx_pipe VALUES (1)");
        tx.enqueue("INSERT INTO tx_pipe VALUES (2)");
        tx.enqueue("INSERT INTO tx_pipe VALUES (3)");
        tx.flush();
        tx.commit();
      }
      assertEquals(3L, conn.query("SELECT count(*) FROM tx_pipe").first().getLong(0));
    }
  }

  @Test
  void transactionDoubleCommitThrows() {
    try (var conn = connect()) {
      var tx = conn.begin();
      tx.commit();
      assertThrows(IllegalStateException.class, tx::commit);
    }
  }

  // ===== Nested transactions (savepoints) =====

  @Test
  void nestedTransactionCommit() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nested_test (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nested_test VALUES (1)");

        try (var nested = tx.begin()) {
          nested.query("INSERT INTO nested_test VALUES (2)");
          nested.commit(); // RELEASE SAVEPOINT
        }

        tx.commit(); // COMMIT
      }
      assertEquals(2L, conn.query("SELECT count(*) FROM nested_test").first().getLong(0));
    }
  }

  @Test
  void nestedTransactionRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nested_rb (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nested_rb VALUES (1)");

        try (var nested = tx.begin()) {
          nested.query("INSERT INTO nested_rb VALUES (2)");
          nested.rollback(); // ROLLBACK TO SAVEPOINT — undoes only the nested insert
        }

        tx.commit();
      }
      // Only the outer insert survived
      assertEquals(1L, conn.query("SELECT count(*) FROM nested_rb").first().getLong(0));
    }
  }

  @Test
  void nestedTransactionAutoRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nested_ar (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nested_ar VALUES (1)");

        try (var nested = tx.begin()) {
          nested.query("INSERT INTO nested_ar VALUES (2)");
          // no commit — auto-rollback on close
        }

        tx.commit();
      }
      assertEquals(1L, conn.query("SELECT count(*) FROM nested_ar").first().getLong(0));
    }
  }

  @Test
  void nestedTransactionOuterRollbackUndoesAll() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nested_outer_rb (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nested_outer_rb VALUES (1)");

        try (var nested = tx.begin()) {
          nested.query("INSERT INTO nested_outer_rb VALUES (2)");
          nested.commit();
        }

        tx.rollback(); // outer rollback undoes everything including committed nested
      }
      assertEquals(0L, conn.query("SELECT count(*) FROM nested_outer_rb").first().getLong(0));
    }
  }

  @Test
  void doubleNestedTransaction() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE double_nested (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO double_nested VALUES (1)");

        try (var nested1 = tx.begin()) {
          nested1.query("INSERT INTO double_nested VALUES (2)");

          try (var nested2 = nested1.begin()) {
            nested2.query("INSERT INTO double_nested VALUES (3)");
            nested2.commit();
          }

          nested1.commit();
        }

        tx.commit();
      }
      assertEquals(3L, conn.query("SELECT count(*) FROM double_nested").first().getLong(0));
    }
  }

  @Test
  void doubleNestedRollbackMiddle() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE double_nested_rb (id int)");
      try (var tx = conn.begin()) {
        tx.query("INSERT INTO double_nested_rb VALUES (1)");

        try (var nested1 = tx.begin()) {
          nested1.query("INSERT INTO double_nested_rb VALUES (2)");

          try (var nested2 = nested1.begin()) {
            nested2.query("INSERT INTO double_nested_rb VALUES (3)");
            nested2.commit(); // releases innermost savepoint
          }

          nested1.rollback(); // rolls back to before nested1, undoing 2 AND 3
        }

        tx.commit();
      }
      // Only the outer insert (1) survived
      assertEquals(1L, conn.query("SELECT count(*) FROM double_nested_rb").first().getLong(0));
    }
  }

  @Test
  void nestedTransactionWithPipelining() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nested_pipe (id int)");
      try (var tx = conn.begin()) {
        try (var nested = tx.begin()) {
          nested.enqueue("INSERT INTO nested_pipe VALUES (1)");
          nested.enqueue("INSERT INTO nested_pipe VALUES (2)");
          nested.flush();
          nested.commit();
        }
        tx.commit();
      }
      assertEquals(2L, conn.query("SELECT count(*) FROM nested_pipe").first().getLong(0));
    }
  }

  // ===== withTransaction (closure-based) =====

  @Test
  void withTransactionCommitsOnSuccess() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_test (id int)");
      conn.withTransaction(
          tx -> {
            tx.query("INSERT INTO wt_test VALUES (1)");
            tx.query("INSERT INTO wt_test VALUES (2)");
            return null;
          });
      assertEquals(2L, conn.query("SELECT count(*) FROM wt_test").first().getLong(0));
    }
  }

  @Test
  void withTransactionRollsBackOnException() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_rb (id int)");
      try {
        conn.withTransaction(
            tx -> {
              tx.query("INSERT INTO wt_rb VALUES (1)");
              if (true) {
                throw new RuntimeException("simulated failure");
              }
              return null;
            });
      } catch (RuntimeException e) {
        assertEquals("simulated failure", e.getMessage());
      }
      assertEquals(0L, conn.query("SELECT count(*) FROM wt_rb").first().getLong(0));
    }
  }

  @Test
  void withTransactionReturnsValue() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_ret (id serial, name text)");
      int id =
          conn.withTransaction(
              tx -> {
                var rs = tx.query("INSERT INTO wt_ret (name) VALUES ($1) RETURNING id", "Alice");
                return rs.first().getInteger("id");
              });
      assertTrue(id > 0);
    }
  }

  @Test
  void withTransactionRollsBackOnDbError() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_dberr (id int PRIMARY KEY)");
      conn.query("INSERT INTO wt_dberr VALUES (1)");
      try {
        conn.withTransaction(
            tx -> {
              tx.query("INSERT INTO wt_dberr VALUES (2)");
              tx.query("INSERT INTO wt_dberr VALUES (1)"); // duplicate PK
              return null;
            });
      } catch (PgException e) {
        // expected
      }
      // Only the original row should remain
      assertEquals(1L, conn.query("SELECT count(*) FROM wt_dberr").first().getLong(0));
    }
  }

  @Test
  void withTransactionNested() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_nested (id int)");
      conn.withTransaction(
          tx -> {
            tx.query("INSERT INTO wt_nested VALUES (1)");
            tx.withTransaction(
                nested -> {
                  nested.query("INSERT INTO wt_nested VALUES (2)");
                  return null;
                });
            return null;
          });
      assertEquals(2L, conn.query("SELECT count(*) FROM wt_nested").first().getLong(0));
    }
  }

  @Test
  void withTransactionNestedRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_nested_rb (id int)");
      conn.withTransaction(
          tx -> {
            tx.query("INSERT INTO wt_nested_rb VALUES (1)");
            try {
              tx.withTransaction(
                  nested -> {
                    nested.query("INSERT INTO wt_nested_rb VALUES (2)");
                    if (true) {
                      throw new RuntimeException("inner failure");
                    }
                    return null;
                  });
            } catch (RuntimeException e) {
              // inner rollback, outer continues
            }
            return null;
          });
      // Outer committed, inner rolled back
      assertEquals(1L, conn.query("SELECT count(*) FROM wt_nested_rb").first().getLong(0));
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

  // ===== PG Geometric data types =====

  @Test
  void dataTypePoint() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::point AS p", "(1.5,2.5)");
      var p = rs.first().get("p", Point.class);
      assertEquals(1.5, p.x(), 0.001);
      assertEquals(2.5, p.y(), 0.001);
    }
  }

  @Test
  void dataTypeLine() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::line AS l", "{1,2,3}");
      var l = rs.first().get("l", Line.class);
      assertEquals(1.0, l.a(), 0.001);
      assertEquals(2.0, l.b(), 0.001);
      assertEquals(3.0, l.c(), 0.001);
    }
  }

  @Test
  void dataTypeLineSegment() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::lseg AS ls", "[(1,2),(3,4)]");
      var ls = rs.first().get("ls", LineSegment.class);
      assertEquals(1.0, ls.p1().x(), 0.001);
      assertEquals(2.0, ls.p1().y(), 0.001);
      assertEquals(3.0, ls.p2().x(), 0.001);
      assertEquals(4.0, ls.p2().y(), 0.001);
    }
  }

  @Test
  void dataTypeBox() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::box AS b", "(3,4),(1,2)");
      var b = rs.first().get("b", Box.class);
      assertEquals(3.0, b.upperRightCorner().x(), 0.001);
      assertEquals(4.0, b.upperRightCorner().y(), 0.001);
      assertEquals(1.0, b.lowerLeftCorner().x(), 0.001);
      assertEquals(2.0, b.lowerLeftCorner().y(), 0.001);
    }
  }

  @Test
  void dataTypeCircle() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::circle AS c", "<(1,2),3>");
      var c = rs.first().get("c", Circle.class);
      assertEquals(1.0, c.centerPoint().x(), 0.001);
      assertEquals(2.0, c.centerPoint().y(), 0.001);
      assertEquals(3.0, c.radius(), 0.001);
    }
  }

  @Test
  void dataTypePolygon() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::polygon AS p", "((0,0),(1,0),(1,1),(0,1))");
      var p = rs.first().get("p", Polygon.class);
      assertEquals(4, p.points().size());
      assertEquals(0.0, p.points().get(0).x(), 0.001);
      assertEquals(1.0, p.points().get(2).x(), 0.001);
    }
  }

  @Test
  void dataTypePath() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::path AS p", "((0,0),(1,1),(2,0))");
      var p = rs.first().get("p", Path.class);
      assertFalse(p.isOpen());
      assertEquals(3, p.points().size());
    }
  }

  // ===== PG Network types =====

  @Test
  void dataTypeInet() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::inet AS i", "192.168.1.1");
      var inet = rs.first().get("i", Inet.class);
      assertEquals("192.168.1.1", inet.address().getHostAddress());
      assertNull(inet.netmask());
    }
  }

  @Test
  void dataTypeInetWithMask() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::inet AS i", "192.168.1.0/24");
      var inet = rs.first().get("i", Inet.class);
      assertEquals("192.168.1.0", inet.address().getHostAddress());
      assertEquals(24, inet.netmask());
    }
  }

  @Test
  void dataTypeCidr() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::cidr AS c", "10.0.0.0/8");
      var cidr = rs.first().get("c", Cidr.class);
      assertEquals(8, cidr.netmask());
    }
  }

  // ===== PG Money =====

  @Test
  void dataTypeMoney() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::money AS m", "$12.34");
      var m = rs.first().get("m", Money.class);
      assertEquals(new java.math.BigDecimal("12.34"), m.bigDecimalValue());
    }
  }

  // ===== PG MAC address types =====

  @Test
  void dataTypeMacaddr() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::macaddr AS m", "08:00:2b:01:02:03");
      var m = rs.first().get("m", Macaddr.class);
      assertEquals("08:00:2b:01:02:03", m.toString());
    }
  }

  @Test
  void dataTypeMacaddr8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::macaddr8 AS m", "08:00:2b:01:02:03:04:05");
      var m = rs.first().get("m", Macaddr8.class);
      assertEquals("08:00:2b:01:02:03:04:05", m.toString());
    }
  }

  // ===== PG Bit string types =====

  @Test
  void dataTypeBit() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::bit(8) AS b", "10110001");
      var b = rs.first().get("b", BitString.class);
      assertEquals(8, b.bitCount());
      assertEquals("10110001", b.toString());
    }
  }

  @Test
  void dataTypeVarbit() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::varbit AS b", "101");
      var b = rs.first().get("b", BitString.class);
      assertEquals(3, b.bitCount());
      assertEquals("101", b.toString());
    }
  }

  // ===== PG Interval =====

  @Test
  void dataTypeInterval() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::interval AS i", "1 year 2 mons 3 days 04:05:06.000007");
      var interval = rs.first().get("i", Interval.class);
      assertEquals(1, interval.years());
      assertEquals(2, interval.months());
      assertEquals(3, interval.days());
      assertEquals(4, interval.hours());
      assertEquals(5, interval.minutes());
      assertEquals(6, interval.seconds());
      assertEquals(7, interval.microseconds());
    }
  }

  @Test
  void dataTypeIntervalSimple() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::interval AS i", "2 hours");
      var interval = rs.first().get("i", Interval.class);
      assertEquals(2, interval.hours());
      assertEquals(0, interval.days());
    }
  }

  @Test
  void dataTypeIntervalToDuration() {
    var interval = Interval.of(0, 0, 1, 2, 30, 0);
    var duration = interval.toDuration();
    assertEquals(1 * 24 * 3600 + 2 * 3600 + 30 * 60, duration.getSeconds());
  }

  // ===== PG Array types =====

  @Test
  void dataTypeIntArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY[1,2,3] AS arr");
      assertEquals(List.of(1, 2, 3), rs.first().getIntegerArray("arr"));
    }
  }

  @Test
  void dataTypeTextArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY['hello','world'] AS arr");
      assertEquals(List.of("hello", "world"), rs.first().getStringArray("arr"));
    }
  }

  @Test
  void dataTypeNullInArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY[1,NULL,3] AS arr");
      var arr = rs.first().getIntegerArray("arr");
      assertEquals(3, arr.size());
      assertEquals(1, arr.get(0));
      assertNull(arr.get(1));
      assertEquals(3, arr.get(2));
    }
  }

  // ===== JSON access =====

  @Test
  void jsonRoundtrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE json_test (id int, data jsonb)");
      conn.query("INSERT INTO json_test VALUES ($1, $2)", 1, "{\"name\":\"Alice\",\"age\":30}");
      var rs = conn.query("SELECT data FROM json_test WHERE id = 1");
      String json = rs.first().getString("data");
      assertTrue(json.contains("\"name\""));
      assertTrue(json.contains("\"Alice\""));
      assertTrue(json.contains("\"age\""));
    }
  }

  @Test
  void jsonQueryOperator() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE json_op (id int, data jsonb)");
      conn.query("INSERT INTO json_op VALUES (1, '{\"name\":\"Alice\"}'::jsonb)");
      var rs = conn.query("SELECT data->>'name' AS name FROM json_op WHERE id = 1");
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  // ===== PG array literal quoting (named params with array values) =====

  @Test
  void pgArrayLiteralWithSpecialChars() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_special (vals text[])");
      // Use named params with a List → PG array literal conversion
      conn.query(
          "INSERT INTO arr_special VALUES ($1)",
          "{\"hello world\",\"with,comma\",\"with\\\"quote\"}");
      var rs = conn.query("SELECT vals[1] AS v FROM arr_special");
      assertEquals("hello world", rs.first().getString("v"));
    }
  }

  @Test
  void pgArrayLiteralWithNulls() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_null (vals int[])");
      conn.query("INSERT INTO arr_null VALUES ('{1,NULL,3}')");
      var rs = conn.query("SELECT vals FROM arr_null");
      var arr = rs.first().getIntegerArray("vals");
      // The array should contain non-null elements
      assertNotNull(arr);
    }
  }

  @Test
  void pgArrayLiteralEmptyString() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_empty (vals text[])");
      conn.query("INSERT INTO arr_empty VALUES ('{\"\"}'::text[])");
      var rs = conn.query("SELECT vals[1] AS v FROM arr_empty");
      assertEquals("", rs.first().getString("v"));
    }
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

  // ===== ColumnMapper (typed get) =====

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
  void columnMapperCustomType() {
    record Money(java.math.BigDecimal amount) {}

    try (var conn = connect()) {
      conn.mapperRegistry().register(Money.class, (v, c) -> new Money(new java.math.BigDecimal(v)));
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
          conn.query(
              "SELECT :name AS name, :age AS age", java.util.Map.of("name", "Alice", "age", 30));
      assertEquals("Alice", rs.first().getString("name"));
      assertEquals("30", rs.first().getString("age"));
    }
  }

  @Test
  void namedParamInsertAndSelect() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_test (id int, name text)");
      conn.query(
          "INSERT INTO np_test VALUES (:id, :name)", java.util.Map.of("id", 1, "name", "Alice"));
      conn.query(
          "INSERT INTO np_test VALUES (:id, :name)", java.util.Map.of("id", 2, "name", "Bob"));

      var rs = conn.query("SELECT name FROM np_test WHERE id = :id", java.util.Map.of("id", 1));
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  @Test
  void namedParamInPipeline() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE np_pipe (id int, val text)");
      conn.enqueue(
          "INSERT INTO np_pipe VALUES (:id, :val)", java.util.Map.of("id", 1, "val", "one"));
      conn.enqueue(
          "INSERT INTO np_pipe VALUES (:id, :val)", java.util.Map.of("id", 2, "val", "two"));
      conn.flush();

      var rs = conn.query("SELECT count(*) FROM np_pipe");
      assertEquals(2L, rs.first().getLong(0));
    }
  }

  @Test
  void namedParamMissingThrows() {
    try (var conn = connect()) {
      assertThrows(
          IllegalArgumentException.class, () -> conn.query("SELECT :missing", java.util.Map.of()));
    }
  }

  // ===== Binder (custom param binding) =====

  @Test
  void customBinder() {
    record Currency(String code) {}

    try (var conn = connect()) {
      conn.binderRegistry().register(Currency.class, c -> c.code());
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
  void cursorParamsUseBinderRegistry() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE cursor_binder (id int, data bytea)");
      byte[] value = new byte[] {(byte) 0xCA, (byte) 0xFE};
      conn.query("INSERT INTO cursor_binder VALUES ($1, $2)", 1, value);

      conn.query("BEGIN");
      // Cursor with a byte[] param should use BinderRegistry (hex-encoding),
      // not raw toString() which produces "[B@..." garbage
      try (var cursor =
          conn.cursor("SELECT id, data FROM cursor_binder WHERE data = $1", (Object) value)) {
        var batch = cursor.read(10);
        assertEquals(1, batch.size());
        assertEquals(1, batch.first().getInteger(0));
      }
      conn.query("COMMIT");
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

      int epochBefore = conn.deallocateEpoch();

      // Change search_path — detected by SQL text inspection (Postgres does not send
      // ParameterStatus for search_path, so we detect SET search_path in the SQL)
      conn.query("SET search_path TO pg_catalog");

      int epochAfter = conn.deallocateEpoch();
      assertTrue(
          epochAfter > epochBefore,
          "search_path change should increment deallocate epoch (was "
              + epochBefore
              + ", now "
              + epochAfter
              + ")");

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
      int epochBefore = conn.deallocateEpoch();

      // DEALLOCATE ALL destroys server-side prepared statements
      conn.query("DEALLOCATE ALL");

      int epochAfter = conn.deallocateEpoch();
      assertTrue(epochAfter > epochBefore, "DEALLOCATE ALL should increment deallocate epoch");

      // Queries still work (re-prepared)
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

  // ===== IN-list expansion =====

  @Test
  void inListExpansionWithNamedParams() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inlist_test (id int, name text)");
      conn.query("INSERT INTO inlist_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

      var rs =
          conn.query(
              "SELECT * FROM inlist_test WHERE id IN (:ids) ORDER BY id",
              java.util.Map.of("ids", List.of(1, 3)));

      assertEquals(2, rs.size());
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  // ===== ANY() with array literals =====
  //
  // For non-prepared queries, use IN (:ids) with collection expansion.
  // For prepared statements, use = ANY(:ids::type[]) — see preparedStatementNamedParamsWith* tests.
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
      var names = new ArrayList<String>();
      conn.queryStream("SELECT generate_series(1, 100) AS n", row -> names.add(row.getString("n")));
      assertEquals(100, names.size());
      assertEquals("1", names.getFirst());
      assertEquals("100", names.getLast());
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
      // Close the stream before consuming all rows — should drain properly
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
  void nestedTransactionErrorAndRecovery() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nest_err (id int PRIMARY KEY)");

      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nest_err VALUES (1)");

        try (var nested = tx.begin()) {
          // This should fail — duplicate key
          assertThrows(DbException.class, () -> nested.query("INSERT INTO nest_err VALUES (1)"));
          // Nested transaction auto-rolls back to savepoint, rescuing outer tx
        }

        // After savepoint rollback, the outer transaction should still work
        tx.query("INSERT INTO nest_err VALUES (2)");
        tx.commit();
      }

      // Both rows should be committed
      var rs = conn.query("SELECT count(*) AS cnt FROM nest_err");
      assertEquals(2L, rs.first().getLong("cnt"));
    }
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
        // Close without consuming the rest — should drain properly
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
            assertNotNull(row.getString("n"));
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

  // ===== Feature 5: extra_float_digits =====

  @Test
  void extraFloatDigitsPreservesDoublePrecision() {
    try (var conn = connect()) {
      // Without extra_float_digits=3, Postgres may round this value
      double precise = 1.0 / 3.0;
      var rs = conn.query("SELECT " + precise + "::float8 AS val");
      double result = rs.first().getDouble("val");
      assertEquals(precise, result, 0.0, "float8 should round-trip without precision loss");
    }
  }

  // ===== Feature 6: stale cached plan retry =====

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

  // ===== Feature 7: adaptive cursor fetch size =====

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

  // ===== Feature 11: streaming row recycling =====

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

  // =====================================================================
  // Plan 2: Special float/double values — NaN and Infinity
  // =====================================================================

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void floatNaNRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float4 AS val", Float.NaN);
      assertTrue(Float.isNaN(rs.first().getFloat("val")));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void doubleNaNRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float8 AS val", Double.NaN);
      assertTrue(Double.isNaN(rs.first().getDouble("val")));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void floatPositiveInfinityRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float4 AS val", Float.POSITIVE_INFINITY);
      assertEquals(Float.POSITIVE_INFINITY, rs.first().getFloat("val"));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void doubleNegativeInfinityRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float8 AS val", Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, rs.first().getDouble("val"));
    }
  }

  @Test
  void numericNaNFromServer() {
    try (var conn = connect()) {
      // Binary protocol: Postgres NUMERIC NaN cannot be represented as BigDecimal
      var rs = conn.query("SELECT 'NaN'::numeric AS val");
      assertThrows(ArithmeticException.class, () -> rs.first().getBigDecimal("val"));
    }
  }

  // =====================================================================
  // Plan 2: Infinity dates and timestamps
  // =====================================================================

  @Test
  void dateInfinity() {
    try (var conn = connect()) {
      // Insert infinity date, read back via extended query to get binary format
      conn.query("CREATE TEMP TABLE inf_date (id int, d date)");
      conn.query("INSERT INTO inf_date VALUES (1, 'infinity')");
      var rs = conn.query("SELECT d FROM inf_date WHERE id = $1", 1);
      assertEquals(java.time.LocalDate.MAX, rs.first().getLocalDate("d"));
    }
  }

  @Test
  void dateNegativeInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE ninf_date (id int, d date)");
      conn.query("INSERT INTO ninf_date VALUES (1, '-infinity')");
      var rs = conn.query("SELECT d FROM ninf_date WHERE id = $1", 1);
      assertEquals(java.time.LocalDate.MIN, rs.first().getLocalDate("d"));
    }
  }

  @Test
  void timestampInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inf_ts (id int, ts timestamp)");
      conn.query("INSERT INTO inf_ts VALUES (1, 'infinity')");
      var rs = conn.query("SELECT ts FROM inf_ts WHERE id = $1", 1);
      assertEquals(java.time.LocalDateTime.MAX, rs.first().getLocalDateTime("ts"));
    }
  }

  @Test
  void timestamptzInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inf_tstz (id int, ts timestamptz)");
      conn.query("INSERT INTO inf_tstz VALUES (1, 'infinity')");
      var rs = conn.query("SELECT ts FROM inf_tstz WHERE id = $1", 1);
      assertEquals(java.time.OffsetDateTime.MAX, rs.first().getOffsetDateTime("ts"));
    }
  }

  // =====================================================================
  // Plan 2: Unicode / multi-byte / emoji database round-trip
  // =====================================================================

  @Test
  void emojiRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::text AS val", "Hello \uD83C\uDF0D\uD83C\uDF89 World");
      assertEquals("Hello \uD83C\uDF0D\uD83C\uDF89 World", rs.first().getString("val"));
    }
  }

  @Test
  void supplementaryUnicodeRoundTrip() {
    try (var conn = connect()) {
      // U+1D11E = MUSICAL SYMBOL G CLEF (supplementary character, surrogate pair in Java)
      var rs = conn.query("SELECT $1::text AS val", "\uD834\uDD1E music");
      assertEquals("\uD834\uDD1E music", rs.first().getString("val"));
    }
  }

  @Test
  void emptyStringRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::text AS val", "");
      assertEquals("", rs.first().getString("val"));
      assertFalse(rs.first().isNull("val"));
    }
  }

  // =====================================================================
  // Plan 2: Numeric precision edge cases
  // =====================================================================

  @Test
  void numericLargePrecisionRoundTrip() {
    try (var conn = connect()) {
      var big = new java.math.BigDecimal("99999999999999999999.99999999999999999999");
      var rs = conn.query("SELECT $1::numeric AS val", big);
      assertEquals(0, big.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void numericZeroWithScale() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 0.00::numeric(10,2) AS val");
      var result = rs.first().getBigDecimal("val");
      assertEquals(0, java.math.BigDecimal.ZERO.compareTo(result));
    }
  }

  // =====================================================================
  // Plan 2: Prepared statement cache invalidation
  // =====================================================================

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
  // Override inherited pipeline error tests — errors cancel subsequent statements in the same
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

  // =====================================================================
  // Pipeline error resilience (stress)
  // =====================================================================

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

  // =====================================================================
  // Large pipeline: triggers mid-pipeline Sync insertion
  // =====================================================================

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

  // =====================================================================
  // Plan 2: Trailing spaces and empty strings
  // =====================================================================

  @Test
  void trailingSpacesPreserved() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE char_test (val char(10))");
      conn.query("INSERT INTO char_test VALUES ($1)", "hello");
      var rs = conn.query("SELECT val FROM char_test");
      String val = rs.first().getString(0);
      // PG char(10) right-pads with spaces
      assertEquals(10, val.length());
      assertTrue(val.startsWith("hello"));
    }
  }

  @Test
  void emptyStringVsNull() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE empty_str (val text)");
      conn.query("INSERT INTO empty_str VALUES ($1)", "");
      var rs = conn.query("SELECT val FROM empty_str");
      assertEquals("", rs.first().getString(0));
      assertFalse(rs.first().isNull(0));
    }
  }

  // =====================================================================
  // Plan 2: executeMany with partial failures
  // =====================================================================

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
                  new Object[] {2}, // duplicate — should error
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

  // =====================================================================
  // Plan 2: Large values and buffer boundaries
  // =====================================================================

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

  // =====================================================================
  // Plan 2: Server-forced connection close
  // =====================================================================

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

  // =====================================================================
  // Plan 2: NOTIFY does not corrupt connection
  // =====================================================================

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

      // Normal query should still work — notification is silently consumed
      var rs = conn.query("SELECT 42 AS answer");
      assertEquals(42, rs.first().getInteger("answer"));

      conn.query("UNLISTEN test_channel");
    }
  }
}
