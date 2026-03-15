package io.djb;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Connection using Testcontainers PostgreSQL.
 * Ported from vertx-sql-client's SimpleQueryTestBase, PipeliningQueryTestBase,
 * TransactionTestBase, and PgConnectionTestBase.
 */
class ConnectionTest {

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

    Connection connect() {
        return Connection.connect(
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
            Connection.connect(pg.getHost(), pg.getMappedPort(5432),
                pg.getDatabaseName(), pg.getUsername(), "wrongpassword"));
    }

    @Test
    void connectWithWrongDatabase() {
        assertThrows(PgException.class, () ->
            Connection.connect(pg.getHost(), pg.getMappedPort(5432),
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
}
