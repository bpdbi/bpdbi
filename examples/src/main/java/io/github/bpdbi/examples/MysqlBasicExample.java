package io.github.bpdbi.examples;

import io.github.bpdbi.mysql.MysqlConnection;

/**
 * Basic MySQL usage: connect, create a table, insert, query.
 *
 * <p>Run: ./gradlew :examples:run -PmainClass=io.github.bpdbi.examples.MysqlBasicExample Requires:
 * MySQL running on localhost:3306 with database "test"
 */
public class MysqlBasicExample {

  public static void main(String[] args) {
    try (var conn = MysqlConnection.connect("localhost", 3306, "test", "root", "password")) {
      System.out.println("Connected to MySQL " + conn.parameters().get("server_version"));

      // Create a table
      conn.query(
          "CREATE TEMPORARY TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2))");

      // Insert rows with parameterized queries
      conn.query("INSERT INTO products (name, price) VALUES (?, ?)", "Widget", "9.99");
      conn.query("INSERT INTO products (name, price) VALUES (?, ?)", "Gadget", "24.99");
      conn.query("INSERT INTO products (name, price) VALUES (?, ?)", "Doohickey", "4.50");

      // Query all rows
      var rs = conn.query("SELECT id, name, price FROM products ORDER BY id");
      System.out.println("\nAll products (" + rs.size() + " rows):");
      for (var row : rs) {
        System.out.printf(
            "  id=%d name=%-10s price=$%s%n",
            row.getInteger("id"), row.getString("name"), row.getBigDecimal("price"));
      }

      // Filtered query
      var cheap = conn.query("SELECT name, price FROM products WHERE price < ?", "10.00");
      System.out.println("\nCheap products:");
      for (var row : cheap) {
        System.out.printf("  %s ($%s)%n", row.getString("name"), row.getBigDecimal("price"));
      }
    }
  }
}
