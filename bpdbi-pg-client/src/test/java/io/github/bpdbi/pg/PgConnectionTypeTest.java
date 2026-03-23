package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.pg.data.BitString;
import io.github.bpdbi.pg.data.Box;
import io.github.bpdbi.pg.data.Cidr;
import io.github.bpdbi.pg.data.Circle;
import io.github.bpdbi.pg.data.Inet;
import io.github.bpdbi.pg.data.Interval;
import io.github.bpdbi.pg.data.Line;
import io.github.bpdbi.pg.data.LineSegment;
import io.github.bpdbi.pg.data.Macaddr;
import io.github.bpdbi.pg.data.Macaddr8;
import io.github.bpdbi.pg.data.Money;
import io.github.bpdbi.pg.data.Path;
import io.github.bpdbi.pg.data.Point;
import io.github.bpdbi.pg.data.Polygon;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Data type encoding/decoding tests for the Postgres driver. */
class PgConnectionTypeTest extends PgTestBase {

  // ===== Data types =====

  @Test
  void dataTypeNumeric() {
    try (var conn = connect()) {
      var rs =
          conn.query(
              "SELECT 42::int2 AS s, 42::int4 AS i, 42::int8 AS l, 3.14::float4 AS f, 3.14::float8 AS d, 123.456::numeric AS n");
      var row = rs.first();
      assertEquals((short) 42, row.getShort(0));
      assertEquals(42, row.getInteger("i"));
      assertEquals(42L, row.getLong("l"));
      assertEquals(3.14f, row.getFloat(3), 0.01f);
      assertEquals(3.14, row.getDouble("d"), 0.001);
      assertEquals(new java.math.BigDecimal("123.456"), row.getBigDecimal("n"));
    }
  }

  @Test
  void dataTypeText() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'hello'::text AS t, 'world'::varchar AS v, 'x'::char(1) AS c");
      var row = rs.first();
      assertEquals("hello", row.getString("t"));
      assertEquals("world", row.getString("v"));
      assertEquals("x", row.getString("c"));
    }
  }

  @Test
  void dataTypeUUID() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u");
      assertEquals(
          java.util.UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
          rs.first().getUUID("u"));
    }
  }

  @Test
  void dataTypeDateAndTime() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2024-01-15'::date AS d, '13:45:30'::time AS t");
      var row = rs.first();
      assertEquals(java.time.LocalDate.of(2024, 1, 15), row.getLocalDate(0));
      assertEquals(java.time.LocalTime.of(13, 45, 30), row.getLocalTime(1));
    }
  }

  @Test
  void dataTypeBytea() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '\\xDEADBEEF'::bytea AS b");
      byte[] bytes = rs.first().getBytes("b");
      assertEquals(4, bytes.length);
      assertEquals((byte) 0xDE, bytes[0]);
      assertEquals((byte) 0xAD, bytes[1]);
      assertEquals((byte) 0xBE, bytes[2]);
      assertEquals((byte) 0xEF, bytes[3]);
    }
  }

  @Test
  void dataTypeNull() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT NULL::int AS n");
      assertTrue(rs.first().isNull("n"));
      assertNull(rs.first().getInteger("n"));
    }
  }

  // ===== Additional data type tests (ported from vertx TextDataTypeDecodeTestBase + PG codec
  // tests) =====

  @Test
  void dataTypeInt2() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 32767::int2");
      assertEquals(Short.MAX_VALUE, rs.first().getShort(0));
    }
  }

  @Test
  void dataTypeInt4() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 2147483647::int4");
      assertEquals(Integer.MAX_VALUE, rs.first().getInteger(0));
    }
  }

  @Test
  void dataTypeInt8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 9223372036854775807::int8");
      assertEquals(Long.MAX_VALUE, rs.first().getLong(0));
    }
  }

  @Test
  void dataTypeFloat4() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 3.4028235E38::float4");
      assertEquals(Float.MAX_VALUE, rs.first().getFloat(0), 1e30f);
    }
  }

  @Test
  void dataTypeFloat8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 1.7976931348623157E308::float8");
      assertEquals(Double.MAX_VALUE, rs.first().getDouble(0), 1e300);
    }
  }

  @Test
  void dataTypeNumericLarge() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 999999999999999999.999999999999::numeric");
      assertEquals(
          new java.math.BigDecimal("999999999999999999.999999999999"), rs.first().getBigDecimal(0));
    }
  }

  @Test
  void dataTypeSerial() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE serial_test (id serial, name text)");
      conn.query("INSERT INTO serial_test (name) VALUES ('a')");
      conn.query("INSERT INTO serial_test (name) VALUES ('b')");
      var rs = conn.query("SELECT id FROM serial_test ORDER BY id");
      assertEquals(2, rs.size());
      assertEquals(1, rs.first().getInteger(0));
    }
  }

  @Test
  void dataTypeBlankPaddedChar() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'ab'::char(5) AS c");
      assertEquals("ab   ", rs.first().getString("c")); // blank-padded
    }
  }

  @Test
  void dataTypeVarchar() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'hello world'::varchar(50) AS v");
      assertEquals("hello world", rs.first().getString("v"));
    }
  }

  @Test
  void dataTypeName() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 'pg_catalog'::name AS n");
      assertEquals("pg_catalog", rs.first().getString("n"));
    }
  }

  @Test
  void dataTypeDate() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15'::date AS d");
      assertEquals(java.time.LocalDate.of(2023, 6, 15), rs.first().getLocalDate(0));
    }
  }

  @Test
  void dataTypeTime() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '17:55:04.905120'::time AS t");
      var t = rs.first().getLocalTime(0);
      assertEquals(17, t.getHour());
      assertEquals(55, t.getMinute());
      assertEquals(4, t.getSecond());
    }
  }

  @Test
  void dataTypeTimestamp() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120'::timestamp AS ts");
      var ts = rs.first().getLocalDateTime(0);
      assertEquals(2023, ts.getYear());
      assertEquals(6, ts.getMonthValue());
      assertEquals(15, ts.getDayOfMonth());
      assertEquals(17, ts.getHour());
    }
  }

  @Test
  void dataTypeTimestamptz() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120+02'::timestamptz AS ts");
      var odt = rs.first().getOffsetDateTime("ts");
      assertNotNull(odt);
      // PG returns timestamptz in UTC
      assertEquals(2023, odt.getYear());
      assertEquals(6, odt.getMonthValue());
      assertEquals(15, odt.getHour()); // 17:55 +02 = 15:55 UTC
    }
  }

  @Test
  void dataTypeInstant() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '2023-06-15 17:55:04.905120+02'::timestamptz AS ts");
      var instant = rs.first().getInstant("ts");
      assertNotNull(instant);
      var odt = java.time.OffsetDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
      assertEquals(2023, odt.getYear());
      assertEquals(15, odt.getHour()); // 17:55 +02 = 15:55 UTC
    }
  }

  @Test
  void dataTypeInstantRoundTrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE instant_rt (ts timestamptz)");
      var now = java.time.Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
      conn.query("INSERT INTO instant_rt VALUES ($1)", now);
      var rs = conn.query("SELECT ts FROM instant_rt");
      assertEquals(now, rs.first().getInstant("ts"));
    }
  }

  @Test
  void dataTypeTimetz() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '17:55:04+02'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(17, ot.getHour());
      assertEquals(55, ot.getMinute());
      assertEquals(4, ot.getSecond());
      assertEquals(java.time.ZoneOffset.ofHours(2), ot.getOffset());
    }
  }

  @Test
  void dataTypeTimetzUTC() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '12:30:00+00'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(12, ot.getHour());
      assertEquals(30, ot.getMinute());
      assertEquals(java.time.ZoneOffset.UTC, ot.getOffset());
    }
  }

  @Test
  void dataTypeTimetzNegativeOffset() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '08:15:30-05'::timetz AS t");
      var ot = rs.first().getOffsetTime("t");
      assertNotNull(ot);
      assertEquals(8, ot.getHour());
      assertEquals(15, ot.getMinute());
      assertEquals(java.time.ZoneOffset.ofHours(-5), ot.getOffset());
    }
  }

  @Test
  void dataTypeJsonAsString() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '{\"key\":\"value\"}'::json AS j");
      assertEquals("{\"key\":\"value\"}", rs.first().getString("j"));
    }
  }

  @Test
  void dataTypeJsonbAsString() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT '{\"key\": \"value\"}'::jsonb AS j");
      String json = rs.first().getString("j");
      assertTrue(json.contains("\"key\""));
      assertTrue(json.contains("\"value\""));
    }
  }

  // ===== NULL value encoding tests (ported from NullValueEncodeTestBase) =====

  @Test
  void parameterizedNullInt() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE null_test (id int, val int)");
      conn.query("INSERT INTO null_test VALUES ($1, $2)", 1, null);
      var rs = conn.query("SELECT val FROM null_test WHERE id = 1");
      assertTrue(rs.first().isNull(0));
    }
  }

  @Test
  void parameterizedNullText() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE null_text (id int, val text)");
      conn.query("INSERT INTO null_text VALUES ($1, $2)", 1, null);
      var rs = conn.query("SELECT val FROM null_text WHERE id = 1");
      assertTrue(rs.first().isNull(0));
      assertNull(rs.first().getString(0));
    }
  }

  // ===== PG Geometric data types =====

  @Test
  void dataTypePoint() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::point AS p", "(1.5,2.5)");
      var p = rs.first().get("p", Point.class);
      assertEquals(1.5, p.x(), 0.001);
      assertEquals(2.5, p.y(), 0.001);
    }
  }

  @Test
  void dataTypeLine() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::line AS l", "{1,2,3}");
      var l = rs.first().get("l", Line.class);
      assertEquals(1.0, l.a(), 0.001);
      assertEquals(2.0, l.b(), 0.001);
      assertEquals(3.0, l.c(), 0.001);
    }
  }

  @Test
  void dataTypeLineSegment() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::lseg AS ls", "[(1,2),(3,4)]");
      var ls = rs.first().get("ls", LineSegment.class);
      assertEquals(1.0, ls.p1().x(), 0.001);
      assertEquals(2.0, ls.p1().y(), 0.001);
      assertEquals(3.0, ls.p2().x(), 0.001);
      assertEquals(4.0, ls.p2().y(), 0.001);
    }
  }

  @Test
  void dataTypeBox() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::box AS b", "(3,4),(1,2)");
      var b = rs.first().get("b", Box.class);
      assertEquals(3.0, b.upperRightCorner().x(), 0.001);
      assertEquals(4.0, b.upperRightCorner().y(), 0.001);
      assertEquals(1.0, b.lowerLeftCorner().x(), 0.001);
      assertEquals(2.0, b.lowerLeftCorner().y(), 0.001);
    }
  }

  @Test
  void dataTypeCircle() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::circle AS c", "<(1,2),3>");
      var c = rs.first().get("c", Circle.class);
      assertEquals(1.0, c.centerPoint().x(), 0.001);
      assertEquals(2.0, c.centerPoint().y(), 0.001);
      assertEquals(3.0, c.radius(), 0.001);
    }
  }

  @Test
  void dataTypePolygon() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::polygon AS p", "((0,0),(1,0),(1,1),(0,1))");
      var p = rs.first().get("p", Polygon.class);
      assertEquals(4, p.points().size());
      assertEquals(0.0, p.points().get(0).x(), 0.001);
      assertEquals(1.0, p.points().get(2).x(), 0.001);
    }
  }

  @Test
  void dataTypePath() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::path AS p", "((0,0),(1,1),(2,0))");
      var p = rs.first().get("p", Path.class);
      assertFalse(p.isOpen());
      assertEquals(3, p.points().size());
    }
  }

  // ===== PG Network types =====

  @Test
  void dataTypeInet() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::inet AS i", "192.168.1.1");
      var inet = rs.first().get("i", Inet.class);
      assertEquals("192.168.1.1", inet.address().getHostAddress());
      assertNull(inet.netmask());
    }
  }

  @Test
  void dataTypeInetWithMask() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::inet AS i", "192.168.1.0/24");
      var inet = rs.first().get("i", Inet.class);
      assertEquals("192.168.1.0", inet.address().getHostAddress());
      assertEquals(24, inet.netmask());
    }
  }

  @Test
  void dataTypeCidr() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::cidr AS c", "10.0.0.0/8");
      var cidr = rs.first().get("c", Cidr.class);
      assertEquals(8, cidr.netmask());
    }
  }

  // ===== PG Money =====

  @Test
  void dataTypeMoney() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::money AS m", "$12.34");
      var m = rs.first().get("m", Money.class);
      assertEquals(new java.math.BigDecimal("12.34"), m.bigDecimalValue());
    }
  }

  // ===== PG MAC address types =====

  @Test
  void dataTypeMacaddr() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::macaddr AS m", "08:00:2b:01:02:03");
      var m = rs.first().get("m", Macaddr.class);
      assertEquals("08:00:2b:01:02:03", m.toString());
    }
  }

  @Test
  void dataTypeMacaddr8() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::macaddr8 AS m", "08:00:2b:01:02:03:04:05");
      var m = rs.first().get("m", Macaddr8.class);
      assertEquals("08:00:2b:01:02:03:04:05", m.toString());
    }
  }

  // ===== PG Bit string types =====

  @Test
  void dataTypeBit() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::bit(8) AS b", "10110001");
      var b = rs.first().get("b", BitString.class);
      assertEquals(8, b.bitCount());
      assertEquals("10110001", b.toString());
    }
  }

  @Test
  void dataTypeVarbit() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::varbit AS b", "101");
      var b = rs.first().get("b", BitString.class);
      assertEquals(3, b.bitCount());
      assertEquals("101", b.toString());
    }
  }

  // ===== PG Interval =====

  @Test
  void dataTypeInterval() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::interval AS i", "1 year 2 mons 3 days 04:05:06.000007");
      var interval = rs.first().get("i", Interval.class);
      assertEquals(1, interval.years());
      assertEquals(2, interval.months());
      assertEquals(3, interval.days());
      assertEquals(4, interval.hours());
      assertEquals(5, interval.minutes());
      assertEquals(6, interval.seconds());
      assertEquals(7, interval.microseconds());
    }
  }

  @Test
  void dataTypeIntervalSimple() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::interval AS i", "2 hours");
      var interval = rs.first().get("i", Interval.class);
      assertEquals(2, interval.hours());
      assertEquals(0, interval.days());
    }
  }

  @Test
  void dataTypeIntervalToDuration() {
    var interval = Interval.of(0, 0, 1, 2, 30, 0);
    var duration = interval.toDuration();
    assertEquals(1 * 24 * 3600 + 2 * 3600 + 30 * 60, duration.getSeconds());
  }

  // ===== PG Array types =====

  @Test
  void dataTypeIntArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY[1,2,3] AS arr");
      assertEquals(List.of(1, 2, 3), rs.first().getIntegerArray("arr"));
    }
  }

  @Test
  void dataTypeTextArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY['hello','world'] AS arr");
      assertEquals(List.of("hello", "world"), rs.first().getStringArray("arr"));
    }
  }

  @Test
  void dataTypeNullInArray() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT ARRAY[1,NULL,3] AS arr");
      var arr = rs.first().getIntegerArray("arr");
      assertEquals(3, arr.size());
      assertEquals(1, arr.get(0));
      assertNull(arr.get(1));
      assertEquals(3, arr.get(2));
    }
  }

  // ===== JSON access =====

  @Test
  void jsonRoundtrip() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE json_test (id int, data jsonb)");
      conn.query("INSERT INTO json_test VALUES ($1, $2)", 1, "{\"name\":\"Alice\",\"age\":30}");
      var rs = conn.query("SELECT data FROM json_test WHERE id = 1");
      String json = rs.first().getString("data");
      assertTrue(json.contains("\"name\""));
      assertTrue(json.contains("\"Alice\""));
      assertTrue(json.contains("\"age\""));
    }
  }

  @Test
  void jsonQueryOperator() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE json_op (id int, data jsonb)");
      conn.query("INSERT INTO json_op VALUES (1, '{\"name\":\"Alice\"}'::jsonb)");
      var rs = conn.query("SELECT data->>'name' AS name FROM json_op WHERE id = 1");
      assertEquals("Alice", rs.first().getString("name"));
    }
  }

  // ===== PG array literal quoting (named params with array values) =====

  @Test
  void pgArrayLiteralWithSpecialChars() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_special (vals text[])");
      // Use named params with a List -> PG array literal conversion
      conn.query(
          "INSERT INTO arr_special VALUES ($1)",
          "{\"hello world\",\"with,comma\",\"with\\\"quote\"}");
      var rs = conn.query("SELECT vals[1] AS v FROM arr_special");
      assertEquals("hello world", rs.first().getString("v"));
    }
  }

  @Test
  void pgArrayLiteralWithNulls() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_null (vals int[])");
      conn.query("INSERT INTO arr_null VALUES ('{1,NULL,3}')");
      var rs = conn.query("SELECT vals FROM arr_null");
      var arr = rs.first().getIntegerArray("vals");
      // The array should contain non-null elements
      assertNotNull(arr);
    }
  }

  @Test
  void pgArrayLiteralEmptyString() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE arr_empty (vals text[])");
      conn.query("INSERT INTO arr_empty VALUES ('{\"\"}'::text[])");
      var rs = conn.query("SELECT vals[1] AS v FROM arr_empty");
      assertEquals("", rs.first().getString("v"));
    }
  }

  // ===== Special float/double values -- NaN and Infinity =====

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"}) // testing primitive boxing round-trip
  void floatNaNRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float4 AS val", Float.NaN);
      assertTrue(Float.isNaN(rs.first().getFloat("val")));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"}) // testing primitive boxing round-trip
  void doubleNaNRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float8 AS val", Double.NaN);
      assertTrue(Double.isNaN(rs.first().getDouble("val")));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"}) // testing primitive boxing round-trip
  void floatPositiveInfinityRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float4 AS val", Float.POSITIVE_INFINITY);
      assertEquals(Float.POSITIVE_INFINITY, rs.first().getFloat("val"));
    }
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"}) // testing primitive boxing round-trip
  void doubleNegativeInfinityRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::float8 AS val", Double.NEGATIVE_INFINITY);
      assertEquals(Double.NEGATIVE_INFINITY, rs.first().getDouble("val"));
    }
  }

  @Test
  void numericNaNFromServer() {
    try (var conn = connect()) {
      // Binary protocol: Postgres NUMERIC NaN cannot be represented as BigDecimal
      var rs = conn.query("SELECT 'NaN'::numeric AS val");
      assertThrows(ArithmeticException.class, () -> rs.first().getBigDecimal("val"));
    }
  }

  // ===== Infinity dates and timestamps =====

  @Test
  void dateInfinity() {
    try (var conn = connect()) {
      // Insert infinity date, read back via extended query to get binary format
      conn.query("CREATE TEMP TABLE inf_date (id int, d date)");
      conn.query("INSERT INTO inf_date VALUES (1, 'infinity')");
      var rs = conn.query("SELECT d FROM inf_date WHERE id = $1", 1);
      assertEquals(java.time.LocalDate.MAX, rs.first().getLocalDate("d"));
    }
  }

  @Test
  void dateNegativeInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE ninf_date (id int, d date)");
      conn.query("INSERT INTO ninf_date VALUES (1, '-infinity')");
      var rs = conn.query("SELECT d FROM ninf_date WHERE id = $1", 1);
      assertEquals(java.time.LocalDate.MIN, rs.first().getLocalDate("d"));
    }
  }

  @Test
  void timestampInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inf_ts (id int, ts timestamp)");
      conn.query("INSERT INTO inf_ts VALUES (1, 'infinity')");
      var rs = conn.query("SELECT ts FROM inf_ts WHERE id = $1", 1);
      assertEquals(java.time.LocalDateTime.MAX, rs.first().getLocalDateTime("ts"));
    }
  }

  @Test
  void timestamptzInfinity() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE inf_tstz (id int, ts timestamptz)");
      conn.query("INSERT INTO inf_tstz VALUES (1, 'infinity')");
      var rs = conn.query("SELECT ts FROM inf_tstz WHERE id = $1", 1);
      assertEquals(java.time.OffsetDateTime.MAX, rs.first().getOffsetDateTime("ts"));
    }
  }

  // ===== Unicode / multi-byte / emoji database round-trip =====

  @Test
  void emojiRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::text AS val", "Hello \uD83C\uDF0D\uD83C\uDF89 World");
      assertEquals("Hello \uD83C\uDF0D\uD83C\uDF89 World", rs.first().getString("val"));
    }
  }

  @Test
  void supplementaryUnicodeRoundTrip() {
    try (var conn = connect()) {
      // U+1D11E = MUSICAL SYMBOL G CLEF (supplementary character, surrogate pair in Java)
      var rs = conn.query("SELECT $1::text AS val", "\uD834\uDD1E music");
      assertEquals("\uD834\uDD1E music", rs.first().getString("val"));
    }
  }

  @Test
  void emptyStringRoundTrip() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT $1::text AS val", "");
      assertEquals("", rs.first().getString("val"));
      assertFalse(rs.first().isNull("val"));
    }
  }

  // ===== Numeric precision edge cases =====

  @Test
  void numericLargePrecisionRoundTrip() {
    try (var conn = connect()) {
      var big = new java.math.BigDecimal("99999999999999999999.99999999999999999999");
      var rs = conn.query("SELECT $1::numeric AS val", big);
      assertEquals(0, big.compareTo(rs.first().getBigDecimal("val")));
    }
  }

  @Test
  void numericZeroWithScale() {
    try (var conn = connect()) {
      var rs = conn.query("SELECT 0.00::numeric(10,2) AS val");
      var result = rs.first().getBigDecimal("val");
      assertEquals(0, java.math.BigDecimal.ZERO.compareTo(result));
    }
  }

  // ===== Binary float8 precision =====

  @Test
  void binaryFloat8PreservesDoublePrecision() {
    try (var conn = connect()) {
      // Binary float8 is IEEE 754 -- always full precision, no extra_float_digits needed
      double precise = 1.0 / 3.0;
      var rs = conn.query("SELECT " + precise + "::float8 AS val");
      double result = rs.first().getDouble("val");
      assertEquals(precise, result, 0.0, "float8 should round-trip without precision loss");
    }
  }

  // ===== Trailing spaces and empty strings =====

  @Test
  void trailingSpacesPreserved() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE char_test (val char(10))");
      conn.query("INSERT INTO char_test VALUES ($1)", "hello");
      var rs = conn.query("SELECT val FROM char_test");
      String val = rs.first().getString(0);
      // PG char(10) right-pads with spaces
      assertEquals(10, val.length());
      assertTrue(val.startsWith("hello"));
    }
  }

  @Test
  void emptyStringVsNull() {
    try (var conn = connect()) {
      conn.query("CREATE TEMP TABLE empty_str (val text)");
      conn.query("INSERT INTO empty_str VALUES ($1)", "");
      var rs = conn.query("SELECT val FROM empty_str");
      assertEquals("", rs.first().getString(0));
      assertFalse(rs.first().isNull(0));
    }
  }
}
