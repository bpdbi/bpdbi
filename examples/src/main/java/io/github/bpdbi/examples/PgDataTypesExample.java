package io.github.bpdbi.examples;

import io.github.bpdbi.pg.PgConnection;

/**
 * Demonstrates reading various Postgres data types through the text protocol.
 *
 * <p>Run: ./gradlew :examples:run -PmainClass=io.github.bpdbi.examples.PgDataTypesExample
 */
public class PgDataTypesExample {

  public static void main(String[] args) {
    try (var conn = PgConnection.connect("localhost", 5432, "postgres", "postgres", "postgres")) {

      System.out.println("=== Numeric Types ===");
      var rs =
          conn.query(
              """
                              SELECT 42::int2 AS small,
                                     2147483647::int4 AS medium,
                                     9223372036854775807::int8 AS large,
                                     3.14::float4 AS float_val,
                                     2.718281828459045::float8 AS double_val,
                                     123456789.123456789::numeric AS numeric_val
                              """);
      var row = rs.first();
      System.out.println("  int2:    " + row.getShort(0));
      System.out.println("  int4:    " + row.getInteger("medium"));
      System.out.println("  int8:    " + row.getLong("large"));
      System.out.println("  float4:  " + row.getFloat(3));
      System.out.println("  float8:  " + row.getDouble("double_val"));
      System.out.println("  numeric: " + row.getBigDecimal("numeric_val"));

      System.out.println("\n=== String Types ===");
      rs = conn.query("SELECT 'hello'::text, 'world'::varchar(50), 'x'::char(5)");
      row = rs.first();
      System.out.println("  text:    '" + row.getString(0) + "'");
      System.out.println("  varchar: '" + row.getString(1) + "'");
      System.out.println("  char(5): '" + row.getString(2) + "'");

      System.out.println("\n=== Boolean ===");
      rs = conn.query("SELECT true AS yes, false AS no");
      row = rs.first();
      System.out.println("  true:  " + row.getBoolean("yes"));
      System.out.println("  false: " + row.getBoolean("no"));

      System.out.println("\n=== Date/Time Types ===");
      rs =
          conn.query(
              """
                          SELECT '2024-06-15'::date AS d,
                                 '13:45:30'::time AS t,
                                 '2024-06-15 13:45:30'::timestamp AS ts
                          """);
      row = rs.first();
      System.out.println("  date:      " + row.getLocalDate(0));
      System.out.println("  time:      " + row.getLocalTime(1));
      System.out.println("  timestamp: " + row.getLocalDateTime(2));

      System.out.println("\n=== UUID ===");
      rs = conn.query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u");
      System.out.println("  uuid: " + rs.first().getUUID("u"));

      System.out.println("\n=== Bytea ===");
      rs = conn.query("SELECT '\\xCAFEBABE'::bytea AS b");
      byte[] bytes = rs.first().getBytes("b");
      System.out.printf("  bytea: %02X %02X %02X %02X%n", bytes[0], bytes[1], bytes[2], bytes[3]);

      System.out.println("\n=== JSON ===");
      rs = conn.query("SELECT '{\"name\": \"djb\", \"version\": 1}'::jsonb AS j");
      System.out.println("  jsonb: " + rs.first().getString("j"));

      System.out.println("\n=== NULL Handling ===");
      rs = conn.query("SELECT NULL::int AS n");
      System.out.println("  isNull: " + rs.first().isNull("n"));
      System.out.println("  value:  " + rs.first().getInteger("n"));
    }
  }
}
