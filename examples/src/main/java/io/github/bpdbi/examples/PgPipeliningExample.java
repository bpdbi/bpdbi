package io.github.bpdbi.examples;

import io.github.bpdbi.pg.PgConnection;

/**
 * Demonstrates pipelining: multiple statements sent in a single network roundtrip.
 *
 * <p>Run: ./gradlew :examples:run -PmainClass=io.github.bpdbi.examples.PgPipeliningExample
 */
public class PgPipeliningExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {

      conn.query("CREATE TEMP TABLE orders (id serial, product text, qty int)");

      // --- Fire-and-forget setup + query (4 statements, 1 roundtrip) ---
      System.out.println("=== Setup + Query (1 roundtrip) ===");
      conn.enqueue("SET statement_timeout TO '30s'");
      conn.enqueue("SET work_mem TO '64MB'");
      conn.enqueue("INSERT INTO orders (product, qty) VALUES ('Widget', 10)");
      var rs = conn.query("SELECT count(*) FROM orders");
      System.out.println("Orders after setup: " + rs.first().getLong(0));

      // --- Transaction with RETURNING (5 statements, 1 roundtrip) ---
      System.out.println("\n=== Pipelined Transaction with RETURNING ===");
      conn.enqueue("BEGIN");
      int i1 =
          conn.enqueue(
              "INSERT INTO orders (product, qty) VALUES ('Gadget', 5) RETURNING id, product");
      int i2 =
          conn.enqueue(
              "INSERT INTO orders (product, qty) VALUES ('Doohickey', 3) RETURNING id, product");
      conn.enqueue("COMMIT");
      var results = conn.flush();

      var gadget = results.get(i1).first();
      var doohickey = results.get(i2).first();
      System.out.printf("Inserted Gadget:    id=%d%n", gadget.getLong("id"));
      System.out.printf("Inserted Doohickey: id=%d%n", doohickey.getLong("id"));

      // --- Batch insert (102 statements, 1 roundtrip) ---
      System.out.println("\n=== Batch Insert (102 statements, 1 roundtrip) ===");
      conn.enqueue("BEGIN");
      for (int i = 0; i < 100; i++) {
        conn.enqueue("INSERT INTO orders (product, qty) VALUES ($1, $2)", "Product-" + i, i + 1);
      }
      conn.enqueue("COMMIT");
      conn.flush();

      var total = conn.query("SELECT count(*) FROM orders");
      System.out.println("Total orders: " + total.first().getLong(0));

      // --- Error isolation in pipeline ---
      System.out.println("\n=== Error Isolation ===");
      conn.enqueue("SELECT 'before error'");
      conn.enqueue("SELECT * FROM nonexistent_table"); // will fail
      conn.enqueue("SELECT 'after error'");
      var errorResults = conn.flush();

      System.out.println("Statement 1: " + errorResults.get(0).first().getString(0));
      System.out.println("Statement 2 error: " + (errorResults.get(1).getError() != null));
      System.out.println("Statement 3: " + errorResults.get(2).first().getString(0));
    }
  }
}
