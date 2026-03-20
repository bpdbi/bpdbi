package io.github.bpdbi.examples;

import io.github.bpdbi.core.Connection;
import io.github.bpdbi.pg.PgConnection;

/**
 * Shows how to use bpdbi with a simple hand-rolled connection pool. For production use, consider
 * Apache Commons Pool 2 (see README.md).
 *
 * <p>Run: ./gradlew :examples:run -PmainClass=io.github.bpdbi.examples.ConnectionPoolExample
 */
public class ConnectionPoolExample {

  public static void main(String[] args) {
    // A minimal pool using a BlockingDeque — for illustration only.
    // In production, use Apache Commons Pool 2 as shown in the README.
    var pool = new java.util.concurrent.LinkedBlockingDeque<Connection>(5);

    // Pre-fill with 3 connections
    for (int i = 0; i < 3; i++) {
      pool.add(PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres"));
    }
    System.out.println("Pool created with " + pool.size() + " connections");

    // Borrow, use, return
    Connection conn = pool.poll();
    try {
      var rs = conn.query("SELECT version()");
      System.out.println("Version: " + rs.first().getString(0));
    } finally {
      pool.offer(conn);
    }
    System.out.println("Connection returned, pool size: " + pool.size());

    // Cleanup
    for (Connection c : pool) {
      c.close();
    }
    System.out.println("All connections closed");
  }
}
