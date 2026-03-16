package io.djb.pg;

import io.djb.*;
import io.djb.pg.data.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Connection using Testcontainers PostgreSQL.
 * Ported from vertx-sql-client's SimpleQueryTestBase, PipeliningQueryTestBase,
 * TransactionTestBase, and PgConnectionTestBase.
 */
class PgConnectionTest {

    static PostgreSQLContainer<?> pg;

    @BeforeAll
    static void startContainer() {
        pg = new PostgreSQLContainer<>("postgres:16-alpine");
        pg.start();
    }

    @AfterAll
    static void stopContainer() {
        if (pg != null) pg.stop();
    }

    PgConnection connect() {
        return PgConnection.connect(
            pg.getHost(),
            pg.getMappedPort(5432),
            pg.getDatabaseName(),
            pg.getUsername(),
            pg.getPassword()
        );
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
        assertThrows(PgException.class, () ->
            PgConnection.connect(pg.getHost(), pg.getMappedPort(5432),
                pg.getDatabaseName(), pg.getUsername(), "wrongpassword"));
    }

    @Test
    void connectWithWrongDatabase() {
        assertThrows(PgException.class, () ->
            PgConnection.connect(pg.getHost(), pg.getMappedPort(5432),
                "nonexistent_db", pg.getUsername(), pg.getPassword()));
    }

    // ===== Simple query protocol =====

    @Test
    void simpleSelect() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 1 AS num, 'hello' AS greeting");
            assertEquals(1, rs.size());
            var row = rs.first();
            assertEquals(1, row.getInteger("num"));
            assertEquals("hello", row.getString("greeting"));
        }
    }

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
    void simpleInsertUpdateDelete() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE test_iud (id int, val text)");

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
            var ex = assertThrows(PgException.class,
                () -> conn.query("SELECT * FROM nonexistent_table_xyz"));
            assertEquals("42P01", ex.sqlState()); // undefined_table
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
    void multiStatementSimpleQuery() {
        try (var conn = connect()) {
            // Simple query protocol supports multiple statements
            var rs = conn.query("CREATE TEMP TABLE multi_test (id int); INSERT INTO multi_test VALUES (1); SELECT * FROM multi_test");
            // The result should be from the last statement
            // (simple protocol sends multiple CommandComplete/RowDescription, but we read until ReadyForQuery)
            // Our implementation returns the last result
            assertNotNull(rs);
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
            var ex = assertThrows(PgException.class,
                () -> conn.query("SELECT $1::int", "not_a_number"));
            assertNotNull(ex.sqlState());
        }
    }

    // ===== Data types (text format) =====

    @Test
    void dataTypeBoolean() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT true AS t, false AS f");
            assertTrue(rs.first().getBoolean("t"));
            assertFalse(rs.first().getBoolean("f"));
        }
    }

    @Test
    void dataTypeNumeric() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 42::int2 AS s, 42::int4 AS i, 42::int8 AS l, 3.14::float4 AS f, 3.14::float8 AS d, 123.456::numeric AS n");
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
            assertEquals(java.util.UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
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
    void pipelineSetupAndQuery() {
        try (var conn = connect()) {
            conn.enqueue("SET search_path TO public");
            conn.enqueue("SET statement_timeout TO '30s'");
            RowSet result = conn.query("SELECT 42 AS answer");

            assertEquals(42, result.first().getInteger("answer"));
        }
    }

    @Test
    void pipelineTransaction() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE pipe_tx (id int, name text)");

            conn.enqueue("BEGIN");
            int r1 = conn.enqueue("INSERT INTO pipe_tx VALUES (1, 'Alice')");
            int r2 = conn.enqueue("INSERT INTO pipe_tx VALUES (2, 'Bob')");
            conn.enqueue("COMMIT");
            List<RowSet> results = conn.flush();

            assertEquals(4, results.size());
            assertEquals(1, results.get(r1).rowsAffected());
            assertEquals(1, results.get(r2).rowsAffected());

            var rs = conn.query("SELECT count(*) FROM pipe_tx");
            assertEquals(2L, rs.first().getLong(0));
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
    void pipelineErrorDoesNotPoisonSubsequent() {
        try (var conn = connect()) {
            conn.enqueue("SELECT 1");
            conn.enqueue("SELECT * FROM nonexistent_table_xyz"); // will fail
            conn.enqueue("SELECT 3");
            List<RowSet> results = conn.flush();

            assertEquals(3, results.size());
            // First succeeds
            assertEquals(1, results.get(0).first().getInteger(0));
            // Second has error
            assertTrue(results.get(1).hasError());
            assertInstanceOf(PgException.class, results.get(1).getError());
            // Third succeeds
            assertEquals(3, results.get(2).first().getInteger(0));
        }
    }

    @Test
    void pipelineEmptyFlush() {
        try (var conn = connect()) {
            List<RowSet> results = conn.flush();
            assertTrue(results.isEmpty());
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

    // ===== Transactions =====

    @Test
    void transactionCommit() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE tx_test (id int)");
            conn.query("BEGIN");
            conn.query("INSERT INTO tx_test VALUES (1)");
            conn.query("INSERT INTO tx_test VALUES (2)");
            conn.query("COMMIT");

            var rs = conn.query("SELECT count(*) FROM tx_test");
            assertEquals(2L, rs.first().getLong(0));
        }
    }

    @Test
    void transactionRollback() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE tx_rb (id int)");
            conn.query("INSERT INTO tx_rb VALUES (1)");
            conn.query("BEGIN");
            conn.query("INSERT INTO tx_rb VALUES (2)");
            conn.query("ROLLBACK");

            var rs = conn.query("SELECT count(*) FROM tx_rb");
            assertEquals(1L, rs.first().getLong(0)); // only the first insert survived
        }
    }

    // ===== Error recovery =====

    @Test
    void connectionUsableAfterError() {
        try (var conn = connect()) {
            try {
                conn.query("INVALID SQL GIBBERISH");
                fail("Should have thrown");
            } catch (PgException e) {
                // expected
            }
            // Connection should still be usable
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

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

    // ===== Additional data type tests (ported from vertx TextDataTypeDecodeTestBase + PG codec tests) =====

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
            assertEquals(new java.math.BigDecimal("999999999999999999.999999999999"),
                rs.first().getBigDecimal(0));
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

    @Test
    void selectMultipleNulls() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT NULL::int AS a, NULL::text AS b, NULL::bool AS c");
            var row = rs.first();
            assertTrue(row.isNull("a"));
            assertTrue(row.isNull("b"));
            assertTrue(row.isNull("c"));
            assertNull(row.getInteger("a"));
            assertNull(row.getString("b"));
            assertNull(row.getBoolean("c"));
        }
    }

    // ===== Connection tests (ported from ConnectionTestBase) =====

    @Test
    void connectWithInvalidUsername() {
        assertThrows(PgException.class, () ->
            PgConnection.connect(pg.getHost(), pg.getMappedPort(5432),
                pg.getDatabaseName(), "nonexistent_user_xyz", pg.getPassword()));
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
            conn.enqueue("SELECT 1");
            conn.enqueue("BAD SQL");
            conn.enqueue("SELECT 2");
            conn.enqueue("ALSO BAD SQL");
            conn.enqueue("SELECT 3");
            List<RowSet> results = conn.flush();

            assertEquals(5, results.size());
            assertEquals(1, results.get(0).first().getInteger(0));
            assertTrue(results.get(1).hasError());
            assertEquals(2, results.get(2).first().getInteger(0));
            assertTrue(results.get(3).hasError());
            assertEquals(3, results.get(4).first().getInteger(0));
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
            var ex = assertThrows(PgException.class,
                () -> conn.query("SELECT FROM WHERE INVALID", 1));
            assertNotNull(ex.sqlState());
        }
    }

    @Test
    void prepareErrorWrongParamCount() {
        try (var conn = connect()) {
            // Provide 2 params but SQL has 1 placeholder — PG will error
            // (our text-format bind sends extra params, PG may ignore or error)
            // At minimum, providing too few params for the SQL should fail
            var ex = assertThrows(Exception.class,
                () -> conn.query("SELECT $1::int, $2::text", 42));
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
        var config = ConnectionConfig.fromUri("postgresql://" + pg.getUsername() + ":" + pg.getPassword()
            + "@" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/" + pg.getDatabaseName());
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
            var rs = conn.query("SELECT '(1.5,2.5)'::point AS p");
            var p = Point.parse(rs.first().getString("p"));
            assertEquals(1.5, p.x(), 0.001);
            assertEquals(2.5, p.y(), 0.001);
        }
    }

    @Test
    void dataTypeLine() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '{1,2,3}'::line AS l");
            var l = Line.parse(rs.first().getString("l"));
            assertEquals(1.0, l.a(), 0.001);
            assertEquals(2.0, l.b(), 0.001);
            assertEquals(3.0, l.c(), 0.001);
        }
    }

    @Test
    void dataTypeLineSegment() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '[(1,2),(3,4)]'::lseg AS ls");
            var ls = LineSegment.parse(rs.first().getString("ls"));
            assertEquals(1.0, ls.p1().x(), 0.001);
            assertEquals(2.0, ls.p1().y(), 0.001);
            assertEquals(3.0, ls.p2().x(), 0.001);
            assertEquals(4.0, ls.p2().y(), 0.001);
        }
    }

    @Test
    void dataTypeBox() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '(3,4),(1,2)'::box AS b");
            var b = Box.parse(rs.first().getString("b"));
            assertEquals(3.0, b.upperRightCorner().x(), 0.001);
            assertEquals(4.0, b.upperRightCorner().y(), 0.001);
            assertEquals(1.0, b.lowerLeftCorner().x(), 0.001);
            assertEquals(2.0, b.lowerLeftCorner().y(), 0.001);
        }
    }

    @Test
    void dataTypeCircle() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '<(1,2),3>'::circle AS c");
            var c = Circle.parse(rs.first().getString("c"));
            assertEquals(1.0, c.centerPoint().x(), 0.001);
            assertEquals(2.0, c.centerPoint().y(), 0.001);
            assertEquals(3.0, c.radius(), 0.001);
        }
    }

    @Test
    void dataTypePolygon() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '((0,0),(1,0),(1,1),(0,1))'::polygon AS p");
            var p = Polygon.parse(rs.first().getString("p"));
            assertEquals(4, p.points().size());
            assertEquals(0.0, p.points().get(0).x(), 0.001);
            assertEquals(1.0, p.points().get(2).x(), 0.001);
        }
    }

    @Test
    void dataTypePath() {
        try (var conn = connect()) {
            // Closed path
            var rs = conn.query("SELECT '((0,0),(1,1),(2,0))'::path AS p");
            var p = Path.parse(rs.first().getString("p"));
            assertFalse(p.isOpen());
            assertEquals(3, p.points().size());
        }
    }

    // ===== PG Network types =====

    @Test
    void dataTypeInet() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '192.168.1.1'::inet AS i");
            var inet = Inet.parse(rs.first().getString("i"));
            assertEquals("192.168.1.1", inet.address().getHostAddress());
            assertNull(inet.netmask());
        }
    }

    @Test
    void dataTypeInetWithMask() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '192.168.1.0/24'::inet AS i");
            var inet = Inet.parse(rs.first().getString("i"));
            assertEquals("192.168.1.0", inet.address().getHostAddress());
            assertEquals(24, inet.netmask());
        }
    }

    @Test
    void dataTypeCidr() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '10.0.0.0/8'::cidr AS c");
            var inet = Inet.parse(rs.first().getString("c"));
            assertEquals(8, inet.netmask());
        }
    }

    // ===== PG Interval =====

    @Test
    void dataTypeInterval() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT '1 year 2 mons 3 days 04:05:06.000007'::interval AS i");
            var interval = Interval.parse(rs.first().getString("i"));
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
            var rs = conn.query("SELECT '2 hours'::interval AS i");
            var interval = Interval.parse(rs.first().getString("i"));
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
            String arrStr = rs.first().getString("arr");
            assertEquals("{1,2,3}", arrStr);
        }
    }

    @Test
    void dataTypeTextArray() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT ARRAY['hello','world'] AS arr");
            String arrStr = rs.first().getString("arr");
            assertEquals("{hello,world}", arrStr);
        }
    }

    @Test
    void dataTypeNullInArray() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT ARRAY[1,NULL,3] AS arr");
            String arrStr = rs.first().getString("arr");
            assertEquals("{1,NULL,3}", arrStr);
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
            var rs = conn.query("SELECT 42 AS n, 'hello' AS s, '2024-01-15' AS d");
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
            var rs = conn.query(
                "SELECT :name AS name, :age AS age",
                java.util.Map.of("name", "Alice", "age", 30));
            assertEquals("Alice", rs.first().getString("name"));
            assertEquals("30", rs.first().getString("age"));
        }
    }

    @Test
    void namedParamInsertAndSelect() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE np_test (id int, name text)");
            conn.query("INSERT INTO np_test VALUES (:id, :name)",
                java.util.Map.of("id", 1, "name", "Alice"));
            conn.query("INSERT INTO np_test VALUES (:id, :name)",
                java.util.Map.of("id", 2, "name", "Bob"));

            var rs = conn.query("SELECT name FROM np_test WHERE id = :id",
                java.util.Map.of("id", 1));
            assertEquals("Alice", rs.first().getString("name"));
        }
    }

    @Test
    void namedParamInPipeline() {
        try (var conn = connect()) {
            conn.query("CREATE TEMP TABLE np_pipe (id int, val text)");
            conn.enqueue("INSERT INTO np_pipe VALUES (:id, :val)",
                java.util.Map.of("id", 1, "val", "one"));
            conn.enqueue("INSERT INTO np_pipe VALUES (:id, :val)",
                java.util.Map.of("id", 2, "val", "two"));
            conn.flush();

            var rs = conn.query("SELECT count(*) FROM np_pipe");
            assertEquals(2L, rs.first().getLong(0));
        }
    }

    @Test
    void namedParamMissingThrows() {
        try (var conn = connect()) {
            assertThrows(IllegalArgumentException.class, () ->
                conn.query("SELECT :missing", java.util.Map.of()));
        }
    }

    // ===== TypeBinder (custom param binding) =====

    @Test
    void customTypeBinder() {
        record Currency(String code) {}
        try (var conn = connect()) {
            conn.typeRegistry().register(Currency.class, c -> c.code());
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
            for (int i = 1; i <= 100; i++) conn.query("INSERT INTO cursor_close VALUES (" + i + ")");
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

    // ===== Additional pipeline error handling tests =====

    @Test
    void pipelineErrorAtStart() {
        try (var conn = connect()) {
            conn.enqueue("BAD SQL");
            conn.enqueue("SELECT 1");
            conn.enqueue("SELECT 2");
            List<RowSet> results = conn.flush();
            assertEquals(3, results.size());
            assertTrue(results.get(0).hasError());
            assertEquals(1, results.get(1).first().getInteger(0));
            assertEquals(2, results.get(2).first().getInteger(0));
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
            assertTrue(results.get(2).hasError());
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
            assertTrue(results.get(0).hasError());
            assertTrue(results.get(1).hasError());
            assertTrue(results.get(2).hasError());
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
            assertFalse(results.get(0).hasError()); // BEGIN ok
            assertFalse(results.get(1).hasError()); // insert ok
            assertTrue(results.get(2).hasError());  // duplicate error

            conn.query("ROLLBACK");

            // Only original row should remain
            var rs = conn.query("SELECT count(*) FROM pipe_tx_err");
            assertEquals(1L, rs.first().getLong(0));
        }
    }

    @Test
    void preparedStatementCacheHit() {
        try (var conn = PgConnection.connect(
                ConnectionConfig.fromUri("postgresql://" + pg.getUsername() + ":" + pg.getPassword()
                    + "@" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/" + pg.getDatabaseName())
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
}
