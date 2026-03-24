package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.bpdbi.core.DbException;
import org.junit.jupiter.api.Test;

/**
 * Transaction interface, nested transactions (savepoints), and useTransaction/inTransaction tests
 * for the Postgres driver.
 */
class PgConnectionTransactionTest extends PgTestBase {

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
        // no commit -- should auto-rollback
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
          nested.rollback(); // ROLLBACK TO SAVEPOINT -- undoes only the nested insert
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
          // no commit -- auto-rollback on close
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

  // ===== useTransaction / inTransaction (closure-based) =====

  @Test
  void useTransactionCommitsOnSuccess() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_test (id int)");
      conn.useTransaction(
          tx -> {
            tx.query("INSERT INTO wt_test VALUES (1)");
            tx.query("INSERT INTO wt_test VALUES (2)");
          });
      assertEquals(2L, conn.query("SELECT count(*) FROM wt_test").first().getLong(0));
    }
  }

  @Test
  void useTransactionRollsBackOnException() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_rb (id int)");
      try {
        conn.useTransaction(
            tx -> {
              tx.query("INSERT INTO wt_rb VALUES (1)");
              if (true) {
                throw new RuntimeException("simulated failure");
              }
            });
      } catch (RuntimeException e) {
        assertEquals("simulated failure", e.getMessage());
      }
      assertEquals(0L, conn.query("SELECT count(*) FROM wt_rb").first().getLong(0));
    }
  }

  @Test
  void inTransactionReturnsValue() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_ret (id serial, name text)");
      int id =
          conn.inTransaction(
              tx -> {
                var rs = tx.query("INSERT INTO wt_ret (name) VALUES ($1) RETURNING id", "Alice");
                return rs.first().getInteger("id");
              });
      assertTrue(id > 0);
    }
  }

  @Test
  void useTransactionRollsBackOnDbError() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_dberr (id int PRIMARY KEY)");
      conn.query("INSERT INTO wt_dberr VALUES (1)");
      try {
        conn.useTransaction(
            tx -> {
              tx.query("INSERT INTO wt_dberr VALUES (2)");
              tx.query("INSERT INTO wt_dberr VALUES (1)"); // duplicate PK
            });
      } catch (PgException e) {
        // expected
      }
      // Only the original row should remain
      assertEquals(1L, conn.query("SELECT count(*) FROM wt_dberr").first().getLong(0));
    }
  }

  @Test
  void useTransactionNested() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_nested (id int)");
      conn.useTransaction(
          tx -> {
            tx.query("INSERT INTO wt_nested VALUES (1)");
            tx.useTransaction(
                nested -> {
                  nested.query("INSERT INTO wt_nested VALUES (2)");
                });
          });
      assertEquals(2L, conn.query("SELECT count(*) FROM wt_nested").first().getLong(0));
    }
  }

  @Test
  void useTransactionNestedRollback() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE wt_nested_rb (id int)");
      conn.useTransaction(
          tx -> {
            tx.query("INSERT INTO wt_nested_rb VALUES (1)");
            try {
              tx.useTransaction(
                  nested -> {
                    nested.query("INSERT INTO wt_nested_rb VALUES (2)");
                    if (true) {
                      throw new RuntimeException("inner failure");
                    }
                  });
            } catch (RuntimeException e) {
              // inner rollback, outer continues
            }
          });
      // Outer committed, inner rolled back
      assertEquals(1L, conn.query("SELECT count(*) FROM wt_nested_rb").first().getLong(0));
    }
  }

  // ===== Nested transaction error recovery =====

  @Test
  void nestedTransactionErrorAndRecovery() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE nest_err (id int PRIMARY KEY)");

      try (var tx = conn.begin()) {
        tx.query("INSERT INTO nest_err VALUES (1)");

        try (var nested = tx.begin()) {
          // This should fail -- duplicate key
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

  // ===== Transaction delegation methods =====

  @Test
  void transactionEnqueueAndFlush() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_pipe (id int)");
      try (var tx = conn.begin()) {
        tx.enqueue("INSERT INTO tx_pipe VALUES ($1)", 1);
        tx.enqueue("INSERT INTO tx_pipe VALUES ($1)", 2);
        var results = tx.flush();
        // flush includes the BEGIN that was enqueued by begin(), plus 2 inserts
        assertTrue(results.size() >= 2);
        tx.commit();
      }
      assertEquals(2L, conn.query("SELECT count(*) FROM tx_pipe").first().getLong(0));
    }
  }

  @Test
  void transactionPrepare() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_prep2 (id int, val text)");
      try (var tx = conn.begin()) {
        try (var stmt = tx.prepare("INSERT INTO tx_prep2 VALUES ($1, $2)")) {
          stmt.query(1, "a");
          stmt.query(2, "b");
        }
        tx.commit();
      }
      assertEquals(2, conn.query("SELECT count(*) FROM tx_prep2").first().getInteger(0));
    }
  }

  @Test
  void transactionQueryStream() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_qs AS SELECT generate_series(1, 5) AS n");
      try (var tx = conn.begin()) {
        int[] count = {0};
        tx.queryStream("SELECT n FROM tx_qs", row -> count[0]++);
        assertEquals(5, count[0]);
        tx.commit();
      }
    }
  }

  @Test
  void transactionStream() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_stream AS SELECT generate_series(1, 5) AS n");
      try (var tx = conn.begin()) {
        try (var rows = tx.stream("SELECT n FROM tx_stream ORDER BY n")) {
          long sum = rows.stream().mapToLong(r -> r.getLong("n")).sum();
          assertEquals(15, sum);
        }
        tx.commit();
      }
    }
  }

  @Test
  void transactionExecuteManyNamed() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_emn (id int, val text)");
      try (var tx = conn.begin()) {
        var paramSets =
            java.util.List.of(
                java.util.Map.<String, Object>of("id", 1, "val", "a"),
                java.util.Map.<String, Object>of("id", 2, "val", "b"),
                java.util.Map.<String, Object>of("id", 3, "val", "c"));
        tx.executeManyNamed("INSERT INTO tx_emn VALUES (:id, :val)", paramSets);
        tx.commit();
      }
      assertEquals(3L, conn.query("SELECT count(*) FROM tx_emn").first().getLong(0));
    }
  }

  @Test
  void transactionCursor() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE tx_cur AS SELECT generate_series(1, 25) AS n");
      // Cursor requires an active transaction; use query() to flush the BEGIN first
      try (var tx = conn.begin()) {
        tx.query("SELECT 1"); // flushes the BEGIN
        try (var cursor = tx.cursor("SELECT n FROM tx_cur ORDER BY n")) {
          var batch1 = cursor.read(10);
          assertEquals(10, batch1.size());
          assertTrue(cursor.hasMore());
          var batch2 = cursor.read(10);
          assertEquals(10, batch2.size());
          assertTrue(cursor.hasMore());
          var batch3 = cursor.read(10);
          assertEquals(5, batch3.size()); // only 5 remaining
          assertFalse(cursor.hasMore());
        }
        tx.commit();
      }
    }
  }
}
