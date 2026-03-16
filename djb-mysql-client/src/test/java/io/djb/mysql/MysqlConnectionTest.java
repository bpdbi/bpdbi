package io.djb.mysql;

import io.djb.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MysqlConnectionTest {

    static MySQLContainer<?> mysql;

    @BeforeAll
    static void startContainer() {
        mysql = new MySQLContainer<>("mysql:8.0");
        mysql.start();
    }

    @AfterAll
    static void stopContainer() {
        if (mysql != null) mysql.stop();
    }

    MysqlConnection connect() {
        return MysqlConnection.connect(
            mysql.getHost(),
            mysql.getMappedPort(3306),
            mysql.getDatabaseName(),
            mysql.getUsername(),
            mysql.getPassword()
        );
    }

    // ===== Connection lifecycle =====

    @Test
    void connectAndClose() {
        try (var conn = connect()) {
            assertNotNull(conn);
            assertTrue(conn.connectionId() > 0);
            assertNotNull(conn.parameters().get("server_version"));
        }
    }

    @Test
    void connectWithWrongPassword() {
        assertThrows(MysqlException.class, () ->
            MysqlConnection.connect(mysql.getHost(), mysql.getMappedPort(3306),
                mysql.getDatabaseName(), mysql.getUsername(), "wrongpassword"));
    }

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
    void simpleSelectMultipleRows() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE gen_test (n INT)");
            for (int i = 1; i <= 10; i++) {
                conn.query("INSERT INTO gen_test VALUES (" + i + ")");
            }
            var rs = conn.query("SELECT n FROM gen_test ORDER BY n");
            assertEquals(10, rs.size());
            int sum = 0;
            for (var row : rs) {
                sum += row.getInteger("n");
            }
            assertEquals(55, sum);
        }
    }

    @Test
    void simpleInsertUpdateDelete() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE test_iud (id INT, val VARCHAR(255))");

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
            var ex = assertThrows(MysqlException.class,
                () -> conn.query("SELECT * FROM nonexistent_table_xyz"));
            assertNotNull(ex.sqlState());
        }
    }

    // ===== Parameterized queries (via interpolation) =====

    @Test
    void parameterizedSelect() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT ? + 0 AS num, ? AS msg", 42, "hello");
            assertEquals(1, rs.size());
            assertEquals(42, rs.first().getInteger("num"));
            assertEquals("hello", rs.first().getString("msg"));
        }
    }

    @Test
    void parameterizedInsertAndSelect() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE param_test (id INT, name VARCHAR(255))");
            conn.query("INSERT INTO param_test VALUES (?, ?)", 1, "Alice");
            conn.query("INSERT INTO param_test VALUES (?, ?)", 2, "Bob");

            var rs = conn.query("SELECT name FROM param_test WHERE id = ?", 1);
            assertEquals("Alice", rs.first().getString("name"));
        }
    }

    @Test
    void parameterizedWithNull() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT ? AS val", (Object) null);
            assertEquals(1, rs.size());
            assertTrue(rs.first().isNull("val"));
        }
    }

    @Test
    void parameterizedWithSpecialChars() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT ? AS val", "it's a \"test\" with \\backslash");
            assertEquals("it's a \"test\" with \\backslash", rs.first().getString("val"));
        }
    }

    // ===== Data types =====

    @Test
    void dataTypeNumeric() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 42 AS i, 3.14 AS d, 123.456 AS n");
            var row = rs.first();
            assertEquals(42, row.getInteger("i"));
            assertEquals(3.14, row.getDouble("d"), 0.001);
            assertEquals(new java.math.BigDecimal("123.456"), row.getBigDecimal("n"));
        }
    }

    @Test
    void dataTypeBoolean() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT TRUE AS t, FALSE AS f");
            assertEquals(true, rs.first().getBoolean("t"));
            assertEquals(false, rs.first().getBoolean("f"));
        }
    }

    @Test
    void dataTypeNull() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT NULL AS n");
            assertTrue(rs.first().isNull("n"));
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
            conn.enqueue("SET @myvar = 42");
            RowSet result = conn.query("SELECT @myvar AS answer");
            assertEquals(42, result.first().getInteger("answer"));
        }
    }

    @Test
    void pipelineTransaction() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE pipe_tx (id INT, name VARCHAR(255))");

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
            assertTrue(results.get(1).hasError());
            assertEquals(3, results.get(2).first().getInteger(0));
        }
    }

    @Test
    void pipelineEmptyFlush() {
        try (var conn = connect()) {
            assertTrue(conn.flush().isEmpty());
        }
    }

    @Test
    void continuousSimpleQueries() {
        try (var conn = connect()) {
            for (int i = 0; i < 50; i++) {
                conn.enqueue("SELECT " + i);
            }
            List<RowSet> results = conn.flush();

            assertEquals(50, results.size());
            for (int i = 0; i < 50; i++) {
                assertEquals(i, results.get(i).first().getInteger(0));
            }
        }
    }

    // ===== Transactions =====

    @Test
    void transactionCommit() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_test (id INT)");
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
            conn.query("CREATE TEMPORARY TABLE tx_rb (id INT)");
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
            assertThrows(DbException.class, () -> conn.query("INVALID SQL GIBBERISH"));
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

    @Test
    void connectionUsableAfterParameterizedError() {
        try (var conn = connect()) {
            assertThrows(DbException.class,
                () -> conn.query("SELECT * FROM nonexistent_table_xyz WHERE id = ?", 1));
            var rs = conn.query("SELECT 'recovered'");
            assertEquals("recovered", rs.first().getString(0));
        }
    }

    // ===== Additional data type tests (ported from TextDataTypeDecodeTestBase + MySQL codec tests) =====

    @Test
    void dataTypeInt() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST(32767 AS SIGNED) AS s, CAST(2147483647 AS SIGNED) AS i, CAST(9223372036854775807 AS SIGNED) AS l");
            var row = rs.first();
            assertEquals(32767, row.getInteger("s"));
            assertEquals(2147483647, row.getInteger("i"));
            assertEquals(9223372036854775807L, row.getLong("l"));
        }
    }

    @Test
    void dataTypeFloat() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST(3.14 AS FLOAT) AS f, CAST(3.14159265358979 AS DOUBLE) AS d");
            var row = rs.first();
            assertEquals(3.14f, row.getFloat(0), 0.01f);
            assertEquals(3.14159265358979, row.getDouble("d"), 1e-10);
        }
    }

    @Test
    void dataTypeDecimal() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST(123.456 AS DECIMAL(10,3)) AS d");
            assertEquals(new java.math.BigDecimal("123.456"), rs.first().getBigDecimal("d"));
        }
    }

    @Test
    void dataTypeString() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 'hello' AS t, CAST('world' AS CHAR(10)) AS c");
            assertEquals("hello", rs.first().getString("t"));
            // MySQL CHAR is right-padded
            assertTrue(rs.first().getString("c").startsWith("world"));
        }
    }

    @Test
    void dataTypeDate() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST('2023-06-15' AS DATE) AS d");
            assertEquals(java.time.LocalDate.of(2023, 6, 15), rs.first().getLocalDate(0));
        }
    }

    @Test
    void dataTypeTime() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST('13:45:30' AS TIME) AS t");
            assertEquals(java.time.LocalTime.of(13, 45, 30), rs.first().getLocalTime(0));
        }
    }

    @Test
    void dataTypeJsonAsString() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST('{\"key\":\"value\"}' AS JSON) AS j");
            String json = rs.first().getString("j");
            assertTrue(json.contains("\"key\""));
            assertTrue(json.contains("\"value\""));
        }
    }

    // ===== NULL tests (ported from NullValueEncodeTestBase) =====

    @Test
    void parameterizedNullValue() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE null_test (id INT, val VARCHAR(255))");
            conn.query("INSERT INTO null_test VALUES (?, ?)", 1, null);
            var rs = conn.query("SELECT val FROM null_test WHERE id = 1");
            assertTrue(rs.first().isNull(0));
            assertNull(rs.first().getString(0));
        }
    }

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

    // ===== Transaction tests (ported from TransactionTestBase) =====

    @Test
    void transactionCommitWithPreparedQuery() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_prep (id INT, val VARCHAR(255))");
            conn.query("BEGIN");
            conn.query("INSERT INTO tx_prep VALUES (?, ?)", 1, "hello");
            conn.query("INSERT INTO tx_prep VALUES (?, ?)", 2, "world");
            conn.query("COMMIT");

            var rs = conn.query("SELECT val FROM tx_prep ORDER BY id");
            assertEquals(2, rs.size());
            assertEquals("hello", rs.first().getString(0));
        }
    }

    @Test
    void transactionRollbackData() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_rb2 (id INT)");
            conn.query("BEGIN");
            conn.query("INSERT INTO tx_rb2 VALUES (1)");
            conn.query("INSERT INTO tx_rb2 VALUES (2)");
            conn.query("ROLLBACK");

            var rs = conn.query("SELECT count(*) AS cnt FROM tx_rb2");
            assertEquals(0L, rs.first().getLong("cnt"));
        }
    }

    // ===== Pipelining stress (ported from PipeliningQueryTestBase) =====

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

    @Test
    void pipelineStressParameterized() {
        try (var conn = connect()) {
            for (int i = 0; i < 200; i++) {
                conn.enqueue("SELECT ? + 0", i);
            }
            List<RowSet> results = conn.flush();

            assertEquals(200, results.size());
            for (int i = 0; i < 200; i++) {
                assertEquals(i, results.get(i).first().getInteger(0));
            }
        }
    }

    @Test
    void pipelineBatchInsert() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE pipe_batch (id INT, val VARCHAR(255))");
            conn.enqueue("BEGIN");
            for (int i = 0; i < 100; i++) {
                conn.enqueue("INSERT INTO pipe_batch VALUES (?, ?)", i, "val-" + i);
            }
            conn.enqueue("COMMIT");
            conn.flush();

            var rs = conn.query("SELECT count(*) AS cnt FROM pipe_batch");
            assertEquals(100L, rs.first().getLong("cnt"));
        }
    }

    @Test
    void pipelineMultipleErrors() {
        try (var conn = connect()) {
            conn.enqueue("SELECT 1");
            conn.enqueue("BAD SQL");
            conn.enqueue("SELECT 2");
            conn.enqueue("ALSO BAD");
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

    @Test
    void mixedSimpleAndParameterizedPipeline() {
        try (var conn = connect()) {
            conn.enqueue("SELECT 'simple'");
            conn.enqueue("SELECT ?", "parameterized");
            conn.enqueue("SELECT 42");
            conn.enqueue("SELECT ? + 0", 99);
            List<RowSet> results = conn.flush();

            assertEquals(4, results.size());
            assertEquals("simple", results.get(0).first().getString(0));
            assertEquals("parameterized", results.get(1).first().getString(0));
            assertEquals(42, results.get(2).first().getInteger(0));
            assertEquals(99, results.get(3).first().getInteger(0));
        }
    }

    // ===== Multiple error recovery =====

    @Test
    void multipleErrorRecovery() {
        try (var conn = connect()) {
            for (int i = 0; i < 10; i++) {
                assertThrows(DbException.class, () -> conn.query("INVALID SQL"));
                var rs = conn.query("SELECT 1");
                assertEquals(1, rs.first().getInteger(0));
            }
        }
    }

    // ===== Row access tests =====

    @Test
    void rowColumnByName() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 1 AS a, 'hello' AS b, TRUE AS c");
            var row = rs.first();
            assertEquals(1, row.getInteger("a"));
            assertEquals("hello", row.getString("b"));
            assertTrue(row.getBoolean("c"));
        }
    }

    @Test
    void rowColumnByIndex() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 1, 'hello', TRUE");
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
            assertThrows(IllegalArgumentException.class, () -> rs.first().getString("nonexistent"));
        }
    }

    @Test
    void rowSetStream() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE stream_test (n INT)");
            for (int i = 1; i <= 5; i++) conn.query("INSERT INTO stream_test VALUES (" + i + ")");
            var rs = conn.query("SELECT n FROM stream_test ORDER BY n");
            long sum = rs.stream().mapToLong(r -> r.getLong("n")).sum();
            assertEquals(15L, sum);
        }
    }

    @Test
    void rowSetNoRows() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE empty_test (id INT)");
            var rs = conn.query("SELECT * FROM empty_test");
            assertEquals(0, rs.size());
            assertThrows(IllegalStateException.class, rs::first);
        }
    }

    // ===== Update count tests =====

    @Test
    void insertRowCount() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE rc_test (id INT)");
            var rs = conn.query("INSERT INTO rc_test VALUES (1)");
            assertEquals(1, rs.rowsAffected());
        }
    }

    @Test
    void updateRowCount() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE rc_upd (id INT, val VARCHAR(255))");
            conn.query("INSERT INTO rc_upd VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            var rs = conn.query("UPDATE rc_upd SET val = 'x' WHERE id <= 2");
            assertEquals(2, rs.rowsAffected());
        }
    }

    @Test
    void deleteRowCount() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE rc_del (id INT)");
            conn.query("INSERT INTO rc_del VALUES (1), (2), (3), (4), (5)");
            var rs = conn.query("DELETE FROM rc_del WHERE id > 2");
            assertEquals(3, rs.rowsAffected());
        }
    }

    // ===== Large result =====

    @Test
    void largeResultSet() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE large_test (n INT)");
            conn.enqueue("BEGIN");
            for (int i = 1; i <= 1000; i++) {
                conn.enqueue("INSERT INTO large_test VALUES (" + i + ")");
            }
            conn.enqueue("COMMIT");
            conn.flush();

            var rs = conn.query("SELECT n FROM large_test ORDER BY n");
            assertEquals(1000, rs.size());
            long sum = rs.stream().mapToLong(r -> r.getLong("n")).sum();
            assertEquals(500500L, sum);
        }
    }

    // ===== Prepare error tests (ported from PreparedQueryTestBase) =====

    @Test
    void prepareErrorInvalidSql() {
        try (var conn = connect()) {
            assertThrows(MysqlException.class,
                () -> conn.query("SELECT FROM WHERE INVALID", 1));
        }
    }

    @Test
    void connectionUsableAfterPrepareError() {
        try (var conn = connect()) {
            assertThrows(MysqlException.class,
                () -> conn.query("SELECT FROM WHERE INVALID", 1));
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

    // ===== Connection tests (ported from ConnectionTestBase) =====

    @Test
    void connectWithInvalidUsername() {
        assertThrows(MysqlException.class, () ->
            MysqlConnection.connect(mysql.getHost(), mysql.getMappedPort(3306),
                mysql.getDatabaseName(), "nonexistent_user_xyz", "password"));
    }

    // ===== Transaction abort test (ported from TransactionTestBase) =====

    @Test
    void transactionAbortOnError() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_abort (id INT PRIMARY KEY)");
            conn.query("BEGIN");
            conn.query("INSERT INTO tx_abort VALUES (1)");
            try {
                conn.query("INSERT INTO tx_abort VALUES (1)"); // duplicate PK
                fail("Should have thrown");
            } catch (MysqlException e) {
                assertNotNull(e.sqlState());
            }
            conn.query("ROLLBACK");

            var rs = conn.query("SELECT count(*) AS cnt FROM tx_abort");
            assertEquals(0L, rs.first().getLong("cnt"));
        }
    }

    // ===== Prepared statements =====

    @Test
    void preparedStatementBasic() {
        try (var conn = connect()) {
            try (var stmt = conn.prepare("SELECT ? + 0 AS n")) {
                assertEquals(1, stmt.query(1).first().getInteger("n"));
                assertEquals(42, stmt.query(42).first().getInteger("n"));
                assertEquals(999, stmt.query(999).first().getInteger("n"));
            }
        }
    }

    @Test
    void preparedStatementInsertAndSelect() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE ps_test (id INT, name VARCHAR(255))");
            try (var insert = conn.prepare("INSERT INTO ps_test VALUES (?, ?)")) {
                insert.query(1, "Alice");
                insert.query(2, "Bob");
            }
            try (var select = conn.prepare("SELECT name FROM ps_test WHERE id = ?")) {
                assertEquals("Alice", select.query(1).first().getString(0));
                assertEquals("Bob", select.query(2).first().getString(0));
            }
        }
    }

    @Test
    void preparedStatementReuseManyTimes() {
        try (var conn = connect()) {
            try (var stmt = conn.prepare("SELECT ? * 2 + 0 AS doubled")) {
                for (int i = 0; i < 50; i++) {
                    assertEquals(i * 2, stmt.query(i).first().getInteger("doubled"));
                }
            }
        }
    }

    // ===== Transaction interface =====

    @Test
    void transactionInterfaceCommit() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_if (id INT)");
            try (var tx = conn.begin()) {
                tx.query("INSERT INTO tx_if VALUES (1)");
                tx.query("INSERT INTO tx_if VALUES (2)");
                tx.commit();
            }
            assertEquals(2L, conn.query("SELECT count(*) AS cnt FROM tx_if").first().getLong("cnt"));
        }
    }

    @Test
    void transactionInterfaceAutoRollback() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_ar (id INT)");
            conn.query("INSERT INTO tx_ar VALUES (1)");
            try (var tx = conn.begin()) {
                tx.query("INSERT INTO tx_ar VALUES (2)");
                // no commit
            }
            assertEquals(1L, conn.query("SELECT count(*) AS cnt FROM tx_ar").first().getLong("cnt"));
        }
    }

    @Test
    void transactionInterfaceExplicitRollback() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_er (id INT)");
            try (var tx = conn.begin()) {
                tx.query("INSERT INTO tx_er VALUES (1)");
                tx.rollback();
            }
            assertEquals(0L, conn.query("SELECT count(*) AS cnt FROM tx_er").first().getLong("cnt"));
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
            conn.query("CREATE TEMPORARY TABLE cursor_test (n INT)");
            conn.enqueue("BEGIN");
            for (int i = 1; i <= 50; i++) conn.enqueue("INSERT INTO cursor_test VALUES (" + i + ")");
            conn.enqueue("COMMIT");
            conn.flush();

            try (var cursor = conn.cursor("SELECT n FROM cursor_test ORDER BY n")) {
                var batch1 = cursor.read(20);
                assertEquals(20, batch1.size());
                assertEquals(1, batch1.first().getInteger(0));
                assertTrue(cursor.hasMore());

                var batch2 = cursor.read(20);
                assertEquals(20, batch2.size());

                var batch3 = cursor.read(20);
                assertEquals(10, batch3.size());
                assertFalse(cursor.hasMore());
            }
        }
    }

    // ===== Connection URI =====

    @Test
    void connectWithUri() {
        var config = ConnectionConfig.fromUri("mysql://" + mysql.getUsername() + ":" + mysql.getPassword()
            + "@" + mysql.getHost() + ":" + mysql.getMappedPort(3306) + "/" + mysql.getDatabaseName());
        try (var conn = MysqlConnection.connect(config)) {
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

    @Test
    void connectionConfigDefaults() {
        var config = ConnectionConfig.fromUri("mysql://user:pass@localhost/mydb");
        assertEquals("localhost", config.host());
        assertEquals(3306, config.port());
        assertEquals("mydb", config.database());
        assertEquals("user", config.username());
        assertEquals("pass", config.password());
    }

    // ===== RowMapper =====

    @Test
    void rowMapperMapTo() {
        record User(int id, String name) {}
        RowMapper<User> mapper = row -> new User(row.getInteger("id"), row.getString("name"));
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE rm_test (id INT, name VARCHAR(255))");
            conn.query("INSERT INTO rm_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
            var users = conn.query("SELECT id, name FROM rm_test ORDER BY id").mapTo(mapper);
            assertEquals(3, users.size());
            assertEquals("Alice", users.get(0).name());
            assertEquals("Charlie", users.get(2).name());
        }
    }

    @Test
    void rowMapperMapFirst() {
        try (var conn = connect()) {
            String val = conn.query("SELECT 'hello' AS val").mapFirst(row -> row.getString("val"));
            assertEquals("hello", val);
        }
    }

    // ===== ColumnMapper (typed get) =====

    @Test
    void columnMapperDefaultTypes() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT 42 AS n, 'hello' AS s");
            var row = rs.first();
            assertEquals(42, row.get("n", Integer.class));
            assertEquals("hello", row.get("s", String.class));
        }
    }

    @Test
    void columnMapperCustomType() {
        record Tag(String value) {}
        try (var conn = connect()) {
            conn.mapperRegistry().register(Tag.class, (v, c) -> new Tag(v));
            var rs = conn.query("SELECT 'important' AS tag");
            var tag = rs.first().get("tag", Tag.class);
            assertEquals("important", tag.value());
        }
    }

    // ===== Named parameters =====

    @Test
    void namedParamSelect() {
        try (var conn = connect()) {
            var rs = conn.query(
                "SELECT :val AS val",
                java.util.Map.of("val", "hello"));
            assertEquals("hello", rs.first().getString("val"));
        }
    }

    @Test
    void namedParamInsertAndSelect() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE np_test (id INT, name VARCHAR(255))");
            conn.query("INSERT INTO np_test VALUES (:id, :name)",
                java.util.Map.of("id", 1, "name", "Alice"));

            var rs = conn.query("SELECT name FROM np_test WHERE id = :id",
                java.util.Map.of("id", 1));
            assertEquals("Alice", rs.first().getString("name"));
        }
    }

    @Test
    void namedParamInPipeline() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE np_pipe (id INT, val VARCHAR(255))");
            conn.enqueue("INSERT INTO np_pipe VALUES (:id, :val)",
                java.util.Map.of("id", 1, "val", "one"));
            conn.enqueue("INSERT INTO np_pipe VALUES (:id, :val)",
                java.util.Map.of("id", 2, "val", "two"));
            conn.flush();

            var rs = conn.query("SELECT count(*) AS cnt FROM np_pipe");
            assertEquals(2L, rs.first().getLong("cnt"));
        }
    }

    // ===== TypeBinder =====

    @Test
    void customTypeBinder() {
        record Currency(String code) {}
        try (var conn = connect()) {
            conn.typeRegistry().register(Currency.class, c -> c.code());
            conn.query("CREATE TEMPORARY TABLE tb_test (id INT, currency VARCHAR(10))");
            conn.query("INSERT INTO tb_test VALUES (?, ?)", 1, new Currency("EUR"));
            var rs = conn.query("SELECT currency FROM tb_test WHERE id = 1");
            assertEquals("EUR", rs.first().getString(0));
        }
    }

    // ===== Additional data type tests (parity with PG) =====

    @Test
    void dataTypeTimestamp() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT CAST('2023-06-15 14:30:00' AS DATETIME) AS dt");
            var dt = rs.first().getLocalDateTime(0);
            assertEquals(java.time.LocalDate.of(2023, 6, 15), dt.toLocalDate());
            assertEquals(java.time.LocalTime.of(14, 30, 0), dt.toLocalTime());
        }
    }

    @Test
    void dataTypeBlob() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE blob_test (id INT, data BLOB)");
            conn.query("INSERT INTO blob_test VALUES (1, X'DEADBEEF')");
            var rs = conn.query("SELECT data FROM blob_test WHERE id = 1");
            byte[] bytes = rs.first().getBytes(0);
            assertNotNull(bytes);
            assertEquals(4, bytes.length);
        }
    }

    @Test
    void parameterizedNullInt() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE null_int (id INT, val INT)");
            conn.query("INSERT INTO null_int VALUES (?, ?)", 1, null);
            var rs = conn.query("SELECT val FROM null_int WHERE id = 1");
            assertTrue(rs.first().isNull(0));
            assertNull(rs.first().getInteger(0));
        }
    }

    @Test
    void parameterizedNullText() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE null_txt (id INT, val VARCHAR(255))");
            conn.query("INSERT INTO null_txt VALUES (?, ?)", 1, null);
            var rs = conn.query("SELECT val FROM null_txt WHERE id = 1");
            assertTrue(rs.first().isNull("val"));
            assertNull(rs.first().getString("val"));
        }
    }

    @Test
    void preparedStatementWithNull() {
        try (var conn = connect()) {
            try (var stmt = conn.prepare("SELECT ? AS val")) {
                var rs = stmt.query((Object) null);
                assertTrue(rs.first().isNull(0));
            }
        }
    }

    @Test
    void preparedStatementError() {
        try (var conn = connect()) {
            assertThrows(MysqlException.class, () -> conn.prepare("INVALID SQL GIBBERISH"));
            // Connection should still be usable
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

    @Test
    void transactionInterfaceWithPipelining() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE tx_pipe (id INT)");
            try (var tx = conn.begin()) {
                tx.enqueue("INSERT INTO tx_pipe VALUES (1)");
                tx.enqueue("INSERT INTO tx_pipe VALUES (2)");
                tx.enqueue("INSERT INTO tx_pipe VALUES (3)");
                tx.flush();
                tx.commit();
            }
            assertEquals(3L, conn.query("SELECT count(*) AS cnt FROM tx_pipe").first().getLong("cnt"));
        }
    }

    @Test
    void queryImplicitlyFlushesPending() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE flush_test (n INT)");
            conn.enqueue("INSERT INTO flush_test VALUES (1)");
            conn.enqueue("INSERT INTO flush_test VALUES (2)");
            // query() should flush the pending inserts first
            var rs = conn.query("SELECT count(*) AS cnt FROM flush_test");
            assertEquals(2L, rs.first().getLong("cnt"));
        }
    }

    @Test
    void continuousParameterizedQueries() {
        try (var conn = connect()) {
            for (int i = 0; i < 200; i++) {
                var rs = conn.query("SELECT ? + 0 AS n", i);
                assertEquals(i, rs.first().getInteger("n"));
            }
        }
    }

    @Test
    void pipelineParameterized() {
        try (var conn = connect()) {
            conn.enqueue("SELECT ? + 0", 10);
            conn.enqueue("SELECT ? + 0", 20);
            conn.enqueue("SELECT ? + 0", 30);
            List<RowSet> results = conn.flush();
            assertEquals(3, results.size());
            assertEquals(10, results.get(0).first().getInteger(0));
            assertEquals(20, results.get(1).first().getInteger(0));
            assertEquals(30, results.get(2).first().getInteger(0));
        }
    }

    @Test
    void rowMapperEmptyResult() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE rm_empty (id INT)");
            var result = conn.query("SELECT id FROM rm_empty").mapTo(row -> row.getInteger("id"));
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void columnMapperNull() {
        try (var conn = connect()) {
            var rs = conn.query("SELECT NULL AS n");
            assertNull(rs.first().get("n", String.class));
        }
    }

    @Test
    void namedParamMissingThrows() {
        try (var conn = connect()) {
            assertThrows(IllegalArgumentException.class,
                () -> conn.query("SELECT :missing AS val", java.util.Map.of()));
        }
    }

    // ===== Cursor tests (additional coverage) =====

    @Test
    void cursorWithParams() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE cursor_param (n INT, label VARCHAR(50))");
            conn.enqueue("BEGIN");
            for (int i = 1; i <= 30; i++) conn.enqueue("INSERT INTO cursor_param VALUES (" + i + ", 'item-" + i + "')");
            conn.enqueue("COMMIT");
            conn.flush();

            try (var cursor = conn.cursor("SELECT n, label FROM cursor_param WHERE n > ? ORDER BY n", 10)) {
                var batch1 = cursor.read(10);
                assertEquals(10, batch1.size());
                assertEquals(11, batch1.first().getInteger("n"));
                assertTrue(cursor.hasMore());

                var batch2 = cursor.read(20);
                assertEquals(10, batch2.size());
                assertFalse(cursor.hasMore());
            }
        }
    }

    @Test
    void cursorCloseEarly() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE cursor_close (n INT)");
            conn.enqueue("BEGIN");
            for (int i = 1; i <= 100; i++) conn.enqueue("INSERT INTO cursor_close VALUES (" + i + ")");
            conn.enqueue("COMMIT");
            conn.flush();

            try (var cursor = conn.cursor("SELECT n FROM cursor_close ORDER BY n")) {
                var batch = cursor.read(10);
                assertEquals(10, batch.size());
                // Close cursor early without reading all rows
            }
            // Connection should still be usable
            var rs = conn.query("SELECT 1");
            assertEquals(1, rs.first().getInteger(0));
        }
    }

    @Test
    void cursorEmptyResult() {
        try (var conn = connect()) {
            conn.query("CREATE TEMPORARY TABLE cursor_empty (n INT)");
            try (var cursor = conn.cursor("SELECT n FROM cursor_empty")) {
                var batch = cursor.read(10);
                assertEquals(0, batch.size());
                assertFalse(cursor.hasMore());
            }
        }
    }

    // ===== Pipeline error handling (additional coverage) =====

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
            conn.query("CREATE TEMPORARY TABLE pipe_tx_err (id INT PRIMARY KEY)");
            conn.query("INSERT INTO pipe_tx_err VALUES (1)");

            conn.enqueue("BEGIN");
            conn.enqueue("INSERT INTO pipe_tx_err VALUES (2)");
            conn.enqueue("INSERT INTO pipe_tx_err VALUES (1)"); // duplicate PK error
            List<RowSet> results = conn.flush();

            assertEquals(3, results.size());
            assertFalse(results.get(0).hasError()); // BEGIN ok
            assertFalse(results.get(1).hasError()); // first insert ok
            assertTrue(results.get(2).hasError());  // duplicate error

            conn.query("ROLLBACK");

            // Only original row should remain
            var rs = conn.query("SELECT count(*) AS cnt FROM pipe_tx_err");
            assertEquals(1L, rs.first().getLong("cnt"));
        }
    }

    @Test
    void preparedStatementCacheHit() {
        try (var conn = MysqlConnection.connect(
                ConnectionConfig.fromUri("mysql://" + mysql.getUsername() + ":" + mysql.getPassword()
                    + "@" + mysql.getHost() + ":" + mysql.getMappedPort(3306) + "/" + mysql.getDatabaseName())
                .cachePreparedStatements(true))) {
            // First call: cache miss
            var rs1 = conn.query("SELECT ? + 0 AS n", 1);
            assertEquals(1, rs1.first().getInteger("n"));

            // Second call: cache hit (same SQL)
            var rs2 = conn.query("SELECT ? + 0 AS n", 2);
            assertEquals(2, rs2.first().getInteger("n"));

            // Third call: still from cache
            var rs3 = conn.query("SELECT ? + 0 AS n", 3);
            assertEquals(3, rs3.first().getInteger("n"));
        }
    }

    @Test
    void connectWithWrongDatabase() {
        assertThrows(Exception.class, () ->
            MysqlConnection.connect(mysql.getHost(), mysql.getMappedPort(3306),
                "nonexistent_database_xyz", mysql.getUsername(), mysql.getPassword()));
    }
}
