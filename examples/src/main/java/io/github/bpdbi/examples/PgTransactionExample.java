package io.github.bpdbi.examples;

import io.github.bpdbi.pg.PgConnection;
import io.github.bpdbi.pg.PgException;

/**
 * Transaction patterns: commit, rollback, error handling, pipelined transactions.
 *
 * <p>Run: ./gradlew :examples:run -PmainClass=io.github.bpdbi.examples.PgTransactionExample
 */
public class PgTransactionExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {

      conn.query("CREATE TEMP TABLE accounts (id int PRIMARY KEY, name text, balance numeric)");
      conn.query("INSERT INTO accounts VALUES (1, 'Alice', 1000), (2, 'Bob', 500)");

      // --- Successful transfer using useTransaction ---
      System.out.println("=== Successful Transfer (useTransaction) ===");
      conn.useTransaction(
          tx -> {
            tx.query("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 200, 1);
            tx.query("UPDATE accounts SET balance = balance + $1 WHERE id = $2", 200, 2);
          });

      printBalances(conn);

      // --- Failed transfer (auto-rollback) ---
      System.out.println("\n=== Failed Transfer (constraint violation, auto-rollback) ===");
      conn.query("ALTER TABLE accounts ADD CONSTRAINT positive_balance CHECK (balance >= 0)");

      try {
        conn.useTransaction(
            tx -> {
              // Try to transfer more than Alice has — throws PgException, auto-rollback
              tx.query("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 10000, 1);
            });
      } catch (PgException e) {
        System.out.println("Transfer failed: " + e.getMessage());
      }

      printBalances(conn); // balances unchanged

      // --- inTransaction returning a value ---
      System.out.println("\n=== Transfer with Return Value (inTransaction) ===");
      long aliceBalance =
          conn.inTransaction(
              tx -> {
                tx.query("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
                tx.query("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
                return tx.query("SELECT balance FROM accounts WHERE id = $1", 1).first().getLong(0);
              });
      System.out.println("  Alice's new balance: $" + aliceBalance);

      printBalances(conn);

      // --- Pipelined transfer (entire transaction in 1 roundtrip) ---
      System.out.println("\n=== Pipelined Transfer (1 roundtrip) ===");
      conn.enqueue("BEGIN");
      conn.enqueue("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
      conn.enqueue("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
      conn.enqueue("COMMIT");
      conn.flush();

      printBalances(conn);
    }
  }

  private static void printBalances(PgConnection conn) {
    var rs = conn.query("SELECT name, balance FROM accounts ORDER BY id");
    for (var row : rs) {
      System.out.printf("  %s: $%s%n", row.getString("name"), row.getBigDecimal("balance"));
    }
  }
}
