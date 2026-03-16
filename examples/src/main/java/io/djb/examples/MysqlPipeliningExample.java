package io.djb.examples;

import io.djb.mysql.MysqlConnection;

/**
 * MySQL pipelining: queue statements and send them in a single roundtrip.
 * <p>
 * Run: ./gradlew :examples:run -PmainClass=io.djb.examples.MysqlPipeliningExample
 */
public class MysqlPipeliningExample {

  public static void main(String[] args) {
    try (var conn = MysqlConnection.connect("localhost", 3306, "test", "root", "password")) {

      conn.query(
          "CREATE TEMPORARY TABLE log_entries (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(255), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

      // --- Setup + query in 1 roundtrip ---
      System.out.println("=== Setup + Query ===");
      conn.enqueue("SET @batch_id = 1");
      conn.enqueue("INSERT INTO log_entries (msg) VALUES ('startup')");
      var rs = conn.query("SELECT count(*) AS cnt FROM log_entries");
      System.out.println("Log entries: " + rs.first().getLong("cnt"));

      // --- Batch insert via pipeline ---
      System.out.println("\n=== Batch Insert (52 statements, 1 roundtrip) ===");
      conn.enqueue("BEGIN");
      for (int i = 0; i < 50; i++) {
        conn.enqueue("INSERT INTO log_entries (msg) VALUES (?)", "event-" + i);
      }
      conn.enqueue("COMMIT");
      conn.flush();

      rs = conn.query("SELECT count(*) AS cnt FROM log_entries");
      System.out.println("Total log entries: " + rs.first().getLong("cnt"));

      // --- Pipeline with index-based result access ---
      System.out.println("\n=== Index-Based Results ===");
      int countIdx = conn.enqueue("SELECT count(*) AS cnt FROM log_entries");
      int firstIdx = conn.enqueue("SELECT msg FROM log_entries ORDER BY id LIMIT 1");
      int lastIdx = conn.enqueue("SELECT msg FROM log_entries ORDER BY id DESC LIMIT 1");
      var results = conn.flush();

      System.out.println("Count: " + results.get(countIdx).first().getLong("cnt"));
      System.out.println("First: " + results.get(firstIdx).first().getString("msg"));
      System.out.println("Last:  " + results.get(lastIdx).first().getString("msg"));
    }
  }
}
