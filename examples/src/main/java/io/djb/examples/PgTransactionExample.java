package io.djb.examples;

import io.djb.pg.PgConnection;
import io.djb.pg.PgException;

/**
 * Transaction patterns: commit, rollback, error handling, pipelined transactions.
 * <p>
 * Run: ./gradlew :examples:run -PmainClass=io.djb.examples.PgTransactionExample
 */
public class PgTransactionExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {

      conn.query("CREATE TEMP TABLE accounts (id int PRIMARY KEY, name text, balance numeric)");
      conn.query("INSERT INTO accounts VALUES (1, 'Alice', 1000), (2, 'Bob', 500)");

      // --- Successful transfer ---
      System.out.println("=== Successful Transfer ===");
      conn.query("BEGIN");
      conn.query("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 200, 1);
      conn.query("UPDATE accounts SET balance = balance + $1 WHERE id = $2", 200, 2);
      conn.query("COMMIT");

      printBalances(conn);

      // --- Failed transfer (rolled back) ---
      System.out.println("\n=== Failed Transfer (constraint violation) ===");
      conn.query("ALTER TABLE accounts ADD CONSTRAINT positive_balance CHECK (balance >= 0)");

      conn.query("BEGIN");
      try {
        // Try to transfer more than Alice has
        conn.query("UPDATE accounts SET balance = balance - $1 WHERE id = $2", 10000, 1);
        conn.query("COMMIT");
      } catch (PgException e) {
        System.out.println("Transfer failed: " + e.getMessage());
        conn.query("ROLLBACK");
      }

      printBalances(conn); // balances unchanged

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
