package io.djb.examples;

import io.djb.pg.PgConnection;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates djb with Java 21 virtual threads. Each virtual thread gets its own connection — no
 * pool needed for moderate concurrency.
 * <p>
 * Run: ./gradlew :examples:run -PmainClass=io.djb.examples.VirtualThreadsExample
 */
public class VirtualThreadsExample {

  public static void main(String[] args) {
    // Setup: create a shared table
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {
      conn.query("CREATE TABLE IF NOT EXISTS vt_counter (id int PRIMARY KEY, count int)");
      conn.query("TRUNCATE vt_counter");
      conn.query("INSERT INTO vt_counter VALUES (1, 0)");
    }

    var completed = new AtomicInteger();
    int numTasks = 100;

    // Launch 100 virtual threads, each with its own connection
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < numTasks; i++) {
        executor.submit(() -> {
          try (var conn = PgConnection.connect(
              "localhost",
              5432,
              "postgres",
              "postgres",
              "postgres"
          )) {
            // Each virtual thread does a pipelined transaction
            conn.enqueue("BEGIN");
            conn.enqueue("UPDATE vt_counter SET count = count + 1 WHERE id = 1");
            conn.enqueue("COMMIT");
            conn.flush();

            completed.incrementAndGet();
          }
        });
      }
    } // executor.close() waits for all tasks

    // Verify
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {
      var rs = conn.query("SELECT count FROM vt_counter WHERE id = 1");
      System.out.println("Completed tasks: " + completed.get());
      System.out.println("Counter value:   " + rs.first().getInteger("count"));
      System.out.println("Expected:        " + numTasks);

      conn.query("DROP TABLE vt_counter");
    }
  }
}
