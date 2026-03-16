package io.djb.examples;

import io.djb.pg.PgConnection;
import io.djb.pg.PgException;

/**
 * Demonstrates error handling: query errors, pipeline errors, recovery.
 * <p>
 * Run: ./gradlew :examples:run -PmainClass=io.djb.examples.ErrorHandlingExample
 */
public class ErrorHandlingExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {

      // --- Catching query errors ---
      System.out.println("=== Query Error ===");
      try {
        conn.query("SELECT * FROM nonexistent_table");
      } catch (PgException e) {
        System.out.println("Caught: " + e.getMessage());
        System.out.println("  SQL State: " + e.sqlState());
        System.out.println("  Severity:  " + e.severity());
        if (e.hint() != null) {
          System.out.println("  Hint: " + e.hint());
        }
      }

      // Connection is still usable after an error
      var rs = conn.query("SELECT 'still alive'");
      System.out.println("After error: " + rs.first().getString(0));

      // --- Pipeline error isolation ---
      System.out.println("\n=== Pipeline Error Isolation ===");
      conn.enqueue("SELECT 'statement 1'");
      conn.enqueue("SELECT * FROM this_table_does_not_exist");
      conn.enqueue("SELECT 'statement 3'");
      var results = conn.flush();

      for (int i = 0; i < results.size(); i++) {
        var result = results.get(i);
        if (result.getError() != null) {
          System.out.printf("  [%d] ERROR: %s%n", i, result.getError().getMessage());
        } else {
          System.out.printf("  [%d] OK: %s%n", i, result.first().getString(0));
        }
      }

      // --- Constraint violation in transaction ---
      System.out.println("\n=== Constraint Violation ===");
      conn.query("CREATE TEMP TABLE unique_test (id int UNIQUE)");
      conn.query("INSERT INTO unique_test VALUES (1)");

      conn.query("BEGIN");
      try {
        conn.query("INSERT INTO unique_test VALUES (1)"); // duplicate
      } catch (PgException e) {
        System.out.println("Constraint violated: " + e.sqlState());
        if (e.constraint() != null) {
          System.out.println("  Constraint: " + e.constraint());
        }
        conn.query("ROLLBACK");
        System.out.println("  Transaction rolled back");
      }

      // Verify data is intact
      rs = conn.query("SELECT count(*) FROM unique_test");
      System.out.println("  Rows in table: " + rs.first().getLong(0));
    }
  }
}
