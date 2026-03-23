package io.github.bpdbi.core;

import static io.github.bpdbi.core.test.TestRows.col;
import static io.github.bpdbi.core.test.TestRows.row;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RowAndRowSetTest {

  // =====================================================================
  // Helper to create rows
  // =====================================================================

  private Row textRow(String[] colNames, String[] values) {
    return row(colNames, values, ColumnMapperRegistry.defaults());
  }

  // =====================================================================
  // Row — basic accessors
  // =====================================================================

  @Test
  void size() {
    var row = textRow(new String[] {"a", "b"}, new String[] {"1", "2"});
    assertEquals(2, row.size());
  }

  @Test
  void isNullByIndex() {
    var row = textRow(new String[] {"a"}, new String[] {null});
    assertTrue(row.isNull(0));
  }

  @Test
  void isNullByName() {
    var row = textRow(new String[] {"a"}, new String[] {null});
    assertTrue(row.isNull("a"));
  }

  @Test
  void isNotNull() {
    var row = textRow(new String[] {"a"}, new String[] {"hello"});
    assertFalse(row.isNull(0));
    assertFalse(row.isNull("a"));
  }

  // =====================================================================
  // Row — getString
  // =====================================================================

  @Test
  void getStringByIndex() {
    var row = textRow(new String[] {"name"}, new String[] {"Alice"});
    assertEquals("Alice", row.getString(0));
  }

  @Test
  void getStringByName() {
    var row = textRow(new String[] {"name"}, new String[] {"Bob"});
    assertEquals("Bob", row.getString("name"));
  }

  @Test
  void getStringNull() {
    var row = textRow(new String[] {"name"}, new String[] {null});
    assertNull(row.getString(0));
  }

  // =====================================================================
  // Row — column lookup errors
  // =====================================================================

  @Test
  void invalidColumnName() {
    var row = textRow(new String[] {"a"}, new String[] {"1"});
    assertThrows(IllegalArgumentException.class, () -> row.getString("nonexistent"));
  }

  // =====================================================================
  // RowSet — basic operations
  // =====================================================================

  @Test
  void rowSetSize() {
    var row = textRow(new String[] {"a"}, new String[] {"1"});
    var rs = new RowSet(List.of(row, row), List.of(col("a")), 0);
    assertEquals(2, rs.size());
  }

  @Test
  void rowSetRowsAffected() {
    var rs = new RowSet(List.of(), List.of(), 5);
    assertEquals(5, rs.rowsAffected());
  }

  @Test
  void rowSetFirst() {
    var row = textRow(new String[] {"a"}, new String[] {"1"});
    var rs = new RowSet(List.of(row), List.of(col("a")), 0);
    assertSame(row, rs.first());
  }

  @Test
  void rowSetFirstEmpty() {
    var rs = new RowSet(List.of(), List.of(), 0);
    assertThrows(IllegalStateException.class, rs::first);
  }

  @Test
  void rowSetColumnDescriptors() {
    var cols = List.of(col("a"), col("b"));
    var rs = new RowSet(List.of(), cols, 0);
    assertEquals(cols, rs.columnDescriptors());
  }

  @Test
  void rowSetStream() {
    var r1 = textRow(new String[] {"a"}, new String[] {"1"});
    var r2 = textRow(new String[] {"a"}, new String[] {"2"});
    var rs = new RowSet(List.of(r1, r2), List.of(col("a")), 0);
    assertEquals(2, rs.stream().count());
  }

  @Test
  void rowSetIterator() {
    var r1 = textRow(new String[] {"a"}, new String[] {"1"});
    var rs = new RowSet(List.of(r1), List.of(col("a")), 0);
    int count = 0;
    for (var ignored : rs) {
      count++;
    }
    assertEquals(1, count);
  }

  // =====================================================================
  // RowSet — mapTo / mapFirst
  // =====================================================================

  @Test
  void rowSetMapTo() {
    var r1 = textRow(new String[] {"n"}, new String[] {"a"});
    var r2 = textRow(new String[] {"n"}, new String[] {"b"});
    var rs = new RowSet(List.of(r1, r2), List.of(col("n")), 0);

    List<String> result = rs.mapTo(row -> row.getString("n"));
    assertEquals(List.of("a", "b"), result);
  }

  @Test
  void rowSetMapFirst() {
    var row = textRow(new String[] {"n"}, new String[] {"hello"});
    var rs = new RowSet(List.of(row), List.of(col("n")), 0);
    assertEquals("hello", rs.mapFirst(r -> r.getString("n")));
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void rowSetMapFirstEmpty() {
    var rs = new RowSet(List.of(), List.of(), 0);
    assertThrows(IllegalStateException.class, () -> rs.mapFirst(r -> r.getString(0)));
  }

  // =====================================================================
  // RowSet — error handling
  // =====================================================================

  @Test
  void rowSetWithError() {
    var error = new DbException("ERROR", "42P01", "relation does not exist");
    var rs = new RowSet(error);
    assertTrue(rs.getError() != null);
    assertSame(error, rs.getError());
  }

  @Test
  void rowSetWithErrorThrowsOnAccess() {
    var error = new DbException("ERROR", "42P01", "relation does not exist");
    var rs = new RowSet(error);

    assertThrows(DbException.class, rs::size);
    assertThrows(DbException.class, rs::first);
    assertThrows(DbException.class, rs::rowsAffected);
    assertThrows(DbException.class, rs::columnDescriptors);
    assertThrows(DbException.class, rs::stream);
    assertThrows(DbException.class, rs::iterator);
    assertThrows(DbException.class, () -> rs.mapTo(r -> r));
  }

  @Test
  void rowSetCheckErrorThrowsStoredError() {
    var error = new DbException("ERROR", "42P01", "relation does not exist");
    var rs = new RowSet(error);

    var ex1 = assertThrows(DbException.class, rs::size);
    var ex2 = assertThrows(DbException.class, rs::size);
    assertSame(error, ex1);
    assertSame(error, ex2);
    assertEquals("42P01", ex1.sqlState());
    assertEquals("ERROR", ex1.severity());
    assertEquals("relation does not exist", ex1.getMessage());
  }

  @Test
  void rowSetNoError() {
    var rs = new RowSet(List.of(), List.of(), 0);
    assertNull(rs.getError());
  }

  // =====================================================================
  // DbException
  // =====================================================================

  @Test
  void dbExceptionFields() {
    var e = new DbException("ERROR", "42P01", "relation does not exist");
    assertEquals("ERROR", e.severity());
    assertEquals("42P01", e.sqlState());
    assertEquals("relation does not exist", e.getMessage());
  }

  @Test
  void dbExceptionToString() {
    var e = new DbException("ERROR", "42P01", "relation does not exist");
    String s = e.toString();
    assertTrue(s.contains("ERROR"));
    assertTrue(s.contains("42P01"));
    assertTrue(s.contains("relation does not exist"));
  }

  @Test
  @SuppressWarnings({"NullAway", "DataFlowIssue"})
  void dbExceptionNullFields() {
    var e = new DbException(null, null, "oops");
    String s = e.toString();
    assertTrue(s.contains("oops"));
  }

  @Test
  void dbExceptionWithSqlInToString() {
    var e = new DbException("ERROR", "42P01", "relation does not exist");
    e.setSql("SELECT * FROM nope");
    String s = e.toString();
    assertTrue(s.contains("SELECT * FROM nope"));
    assertTrue(s.contains("SQL:"));
  }

  @Test
  void dbExceptionWithCause() {
    var cause = new RuntimeException("underlying error");
    var e = new DbException("ERROR", "42P01", "relation does not exist", cause);
    assertSame(cause, e.getCause());
    assertEquals("ERROR", e.severity());
    assertEquals("42P01", e.sqlState());
  }

  @Test
  void dbConnectionExceptionFields() {
    var e = new DbConnectionException("connection lost");
    assertEquals("FATAL", e.severity());
    assertEquals("08006", e.sqlState());
    assertTrue(e.getMessage().contains("connection lost"));
  }

  @Test
  void dbConnectionExceptionWithCause() {
    var cause = new java.io.IOException("socket closed");
    var e = new DbConnectionException("connection lost", cause);
    assertSame(cause, e.getCause());
    assertEquals("FATAL", e.severity());
  }

  // =====================================================================
  // Row — typed getters (binary codec path)
  // =====================================================================

  @Test
  void getIntValue() {
    var row = textRow(new String[] {"n"}, new String[] {"1234"});
    assertEquals(1234, row.getIntValue(0));
  }

  @Test
  void getIntValueNull() {
    var row = textRow(new String[] {"n"}, new String[] {null});
    assertThrows(NullPointerException.class, () -> row.getIntValue(0));
  }

  @Test
  void getIntValueByName() {
    var row = textRow(new String[] {"n"}, new String[] {"5678"});
    assertEquals(5678, row.getIntValue("n"));
  }

  @Test
  void getLong() {
    var row = textRow(new String[] {"n"}, new String[] {"12345678"});
    assertEquals(12345678L, row.getLong(0));
  }

  @Test
  void getLongNull() {
    var row = textRow(new String[] {"n"}, new String[] {null});
    assertNull(row.getLong(0));
  }

  @Test
  void getLongByName() {
    var row = textRow(new String[] {"n"}, new String[] {"12345678"});
    assertEquals(12345678L, row.getLong("n"));
  }

  @Test
  void getLongValue() {
    var row = textRow(new String[] {"n"}, new String[] {"12345678"});
    assertEquals(12345678L, row.getLongValue(0));
  }

  @Test
  void getLongValueNull() {
    var row = textRow(new String[] {"n"}, new String[] {null});
    assertThrows(NullPointerException.class, () -> row.getLongValue(0));
  }

  @Test
  void getLongValueByName() {
    var row = textRow(new String[] {"n"}, new String[] {"12345678"});
    assertEquals(12345678L, row.getLongValue("n"));
  }

  @Test
  void getShort() {
    var row = textRow(new String[] {"n"}, new String[] {"42"});
    assertEquals((short) 42, row.getShort(0));
  }

  @Test
  void getShortNull() {
    var row = textRow(new String[] {"n"}, new String[] {null});
    assertNull(row.getShort(0));
  }

  @Test
  void getShortByName() {
    var row = textRow(new String[] {"n"}, new String[] {"42"});
    assertEquals((short) 42, row.getShort("n"));
  }

  @Test
  void getBoolValue() {
    var row = textRow(new String[] {"b"}, new String[] {"t"});
    assertTrue(row.getBoolValue(0));
  }

  @Test
  void getBoolValueFalse() {
    var row = textRow(new String[] {"b"}, new String[] {"f"});
    assertFalse(row.getBoolValue(0));
  }

  @Test
  void getBoolValueNull() {
    var row = textRow(new String[] {"b"}, new String[] {null});
    assertThrows(NullPointerException.class, () -> row.getBoolValue(0));
  }

  @Test
  void getBoolValueByName() {
    var row = textRow(new String[] {"b"}, new String[] {"t"});
    assertTrue(row.getBoolValue("b"));
  }

  @Test
  void getDouble() {
    var row = textRow(new String[] {"n"}, new String[] {"3.140000"});
    assertEquals(3.14, row.getDouble(0), 0.001);
  }

  @Test
  void getDoubleNull() {
    var row = textRow(new String[] {"n"}, new String[] {null});
    assertNull(row.getDouble(0));
  }

  @Test
  void getDoubleByName() {
    var row = textRow(new String[] {"n"}, new String[] {"3.140000"});
    assertEquals(3.14, row.getDouble("n"), 0.001);
  }

  @Test
  void getBytes() {
    var row = textRow(new String[] {"b"}, new String[] {"hello"});
    assertArrayEquals("hello".getBytes(), row.getBytes(0));
  }

  @Test
  void getBytesNull() {
    var row = textRow(new String[] {"b"}, new String[] {null});
    assertNull(row.getBytes(0));
  }

  @Test
  void getBytesByName() {
    var row = textRow(new String[] {"b"}, new String[] {"world"});
    assertArrayEquals("world".getBytes(), row.getBytes("b"));
  }

  // =====================================================================
  // Row — generic get(int, Class) via BinaryCodec
  // =====================================================================

  @Test
  void getTypedString() {
    var row = textRow(new String[] {"s"}, new String[] {"hello"});
    assertEquals("hello", row.get(0, String.class));
  }

  @Test
  void getTypedNull() {
    var row = textRow(new String[] {"s"}, new String[] {null});
    assertNull(row.get(0, String.class));
  }

  // =====================================================================
  // Row — null arrays
  // =====================================================================

  @Test
  void getArrayReturnsNullForNullColumn() {
    var row = textRow(new String[] {"a"}, new String[] {null});
    assertNull(row.getStringArray(0));
    assertNull(row.getIntegerArray(0));
    assertNull(row.getLongArray(0));
    assertNull(row.getDoubleArray(0));
    assertNull(row.getFloatArray(0));
    assertNull(row.getShortArray(0));
    assertNull(row.getBooleanArray(0));
  }
}
