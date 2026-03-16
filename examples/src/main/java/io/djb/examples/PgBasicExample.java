package io.djb.examples;

import io.djb.pg.PgConnection;

/**
 * Basic Postgres usage: connect, create a table, insert, query.
 * <p>
 * Run: ./gradlew :examples:run -PmainClass=io.djb.examples.PgBasicExample Requires: Postgres
 * running on localhost:5432 with database "postgres"
 */
public class PgBasicExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {
      System.out.println("Connected to Postgres " + conn.parameters().get("server_version"));

      // Create a table
      conn.query("CREATE TEMP TABLE users (id serial PRIMARY KEY, name text, email text)");

      // Insert rows
      conn.query("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com");
      conn.query("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com");
      conn.query(
          "INSERT INTO users (name, email) VALUES ($1, $2)",
          "Charlie",
          "charlie@example.com"
      );

      // Query all rows
      var rs = conn.query("SELECT id, name, email FROM users ORDER BY id");
      System.out.println("\nAll users (" + rs.size() + " rows):");
      for (var row : rs) {
        System.out.printf(
            "  id=%d name=%s email=%s%n",
            row.getInteger("id"), row.getString("name"), row.getString("email")
        );
      }

      // Query with parameter
      var rs2 = conn.query("SELECT * FROM users WHERE name = $1", "Bob");
      System.out.println("\nFound Bob: " + rs2.first().getString("email"));

      // Update
      var updated = conn.query(
          "UPDATE users SET email = $1 WHERE name = $2",
          "bob@newdomain.com",
          "Bob"
      );
      System.out.println("Updated " + updated.rowsAffected() + " row(s)");

      // Delete
      var deleted = conn.query("DELETE FROM users WHERE name = $1", "Charlie");
      System.out.println("Deleted " + deleted.rowsAffected() + " row(s)");
    }
  }
}
