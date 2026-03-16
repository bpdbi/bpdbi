package io.djb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RowAndRowSetTest {

    // =====================================================================
    // Helper to create text-format rows
    // =====================================================================

    private static ColumnDescriptor col(String name) {
        return new ColumnDescriptor(name, 0, (short) 0, 0, (short) 0, 0);
    }

    private static ColumnDescriptor col(String name, int typeOID) {
        return new ColumnDescriptor(name, 0, (short) 0, typeOID, (short) 0, 0);
    }

    private static byte[] textBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Row textRow(String[] colNames, String[] values) {
        ColumnDescriptor[] cols = new ColumnDescriptor[colNames.length];
        byte[][] vals = new byte[colNames.length][];
        for (int i = 0; i < colNames.length; i++) {
            cols[i] = col(colNames[i]);
            vals[i] = values[i] == null ? null : textBytes(values[i]);
        }
        return new Row(cols, vals, null, MapperRegistry.defaults());
    }

    // =====================================================================
    // Row — basic accessors
    // =====================================================================

    @Test
    void size() {
        var row = textRow(new String[]{"a", "b"}, new String[]{"1", "2"});
        assertEquals(2, row.size());
    }

    @Test
    void isNullByIndex() {
        var row = textRow(new String[]{"a"}, new String[]{null});
        assertTrue(row.isNull(0));
    }

    @Test
    void isNullByName() {
        var row = textRow(new String[]{"a"}, new String[]{null});
        assertTrue(row.isNull("a"));
    }

    @Test
    void isNotNull() {
        var row = textRow(new String[]{"a"}, new String[]{"hello"});
        assertFalse(row.isNull(0));
        assertFalse(row.isNull("a"));
    }

    // =====================================================================
    // Row — getString
    // =====================================================================

    @Test
    void getStringByIndex() {
        var row = textRow(new String[]{"name"}, new String[]{"Alice"});
        assertEquals("Alice", row.getString(0));
    }

    @Test
    void getStringByName() {
        var row = textRow(new String[]{"name"}, new String[]{"Bob"});
        assertEquals("Bob", row.getString("name"));
    }

    @Test
    void getStringNull() {
        var row = textRow(new String[]{"name"}, new String[]{null});
        assertNull(row.getString(0));
    }

    // =====================================================================
    // Row — numeric types (text mode)
    // =====================================================================

    @Test
    void getInteger() {
        var row = textRow(new String[]{"n"}, new String[]{"42"});
        assertEquals(42, row.getInteger(0));
        assertEquals(42, row.getInteger("n"));
    }

    @Test
    void getIntegerNull() {
        var row = textRow(new String[]{"n"}, new String[]{null});
        assertNull(row.getInteger(0));
    }

    @Test
    void getLong() {
        var row = textRow(new String[]{"n"}, new String[]{"9876543210"});
        assertEquals(9876543210L, row.getLong(0));
    }

    @Test
    void getShort() {
        var row = textRow(new String[]{"n"}, new String[]{"123"});
        assertEquals((short) 123, row.getShort(0));
    }

    @Test
    void getFloat() {
        var row = textRow(new String[]{"n"}, new String[]{"3.14"});
        assertEquals(3.14f, row.getFloat(0), 0.001f);
    }

    @Test
    void getDouble() {
        var row = textRow(new String[]{"n"}, new String[]{"3.14159"});
        assertEquals(3.14159, row.getDouble(0), 1e-6);
    }

    @Test
    void getBigDecimal() {
        var row = textRow(new String[]{"n"}, new String[]{"123456789.123456789"});
        assertEquals(new BigDecimal("123456789.123456789"), row.getBigDecimal(0));
    }

    @Test
    void getBigDecimalNull() {
        var row = textRow(new String[]{"n"}, new String[]{null});
        assertNull(row.getBigDecimal(0));
    }

    @Test
    void getBooleanTrue() {
        var row = textRow(new String[]{"b"}, new String[]{"t"});
        assertTrue(row.getBoolean(0));
    }

    @Test
    void getBooleanTrueWord() {
        var row = textRow(new String[]{"b"}, new String[]{"true"});
        assertTrue(row.getBoolean(0));
    }

    @Test
    void getBooleanOne() {
        var row = textRow(new String[]{"b"}, new String[]{"1"});
        assertTrue(row.getBoolean(0));
    }

    @Test
    void getBooleanFalse() {
        var row = textRow(new String[]{"b"}, new String[]{"f"});
        assertFalse(row.getBoolean(0));
    }

    @Test
    void getBooleanNull() {
        var row = textRow(new String[]{"b"}, new String[]{null});
        assertNull(row.getBoolean(0));
    }

    // =====================================================================
    // Row — date/time types (text mode)
    // =====================================================================

    @Test
    void getLocalDate() {
        var row = textRow(new String[]{"d"}, new String[]{"2025-06-15"});
        assertEquals(LocalDate.of(2025, 6, 15), row.getLocalDate(0));
    }

    @Test
    void getLocalTime() {
        var row = textRow(new String[]{"t"}, new String[]{"12:30:45"});
        assertEquals(LocalTime.of(12, 30, 45), row.getLocalTime(0));
    }

    @Test
    void getLocalDateTime() {
        var row = textRow(new String[]{"ts"}, new String[]{"2025-06-15 12:30:45"});
        assertEquals(LocalDateTime.of(2025, 6, 15, 12, 30, 45), row.getLocalDateTime(0));
    }

    @Test
    void getLocalDateTimeIsoFormat() {
        var row = textRow(new String[]{"ts"}, new String[]{"2025-06-15T12:30:45"});
        assertEquals(LocalDateTime.of(2025, 6, 15, 12, 30, 45), row.getLocalDateTime(0));
    }

    @Test
    void getOffsetDateTime() {
        var row = textRow(new String[]{"ts"}, new String[]{"2025-06-15T12:30:45+00:00"});
        assertEquals(OffsetDateTime.of(2025, 6, 15, 12, 30, 45, 0, ZoneOffset.UTC), row.getOffsetDateTime(0));
    }

    @Test
    void getLocalDateNull() {
        var row = textRow(new String[]{"d"}, new String[]{null});
        assertNull(row.getLocalDate(0));
    }

    // =====================================================================
    // Row — UUID
    // =====================================================================

    @Test
    void getUUID() {
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        var row = textRow(new String[]{"id"}, new String[]{uuid});
        assertEquals(UUID.fromString(uuid), row.getUUID(0));
    }

    @Test
    void getUUIDNull() {
        var row = textRow(new String[]{"id"}, new String[]{null});
        assertNull(row.getUUID(0));
    }

    // =====================================================================
    // Row — bytes (text mode hex)
    // =====================================================================

    @Test
    void getBytesHex() {
        var row = textRow(new String[]{"data"}, new String[]{"\\x48454c4c4f"});
        assertArrayEquals(new byte[]{0x48, 0x45, 0x4c, 0x4c, 0x4f}, row.getBytes(0));
    }

    @Test
    void getBytesRaw() {
        var row = textRow(new String[]{"data"}, new String[]{"raw"});
        assertArrayEquals("raw".getBytes(StandardCharsets.UTF_8), row.getBytes(0));
    }

    @Test
    void getBytesNull() {
        var row = textRow(new String[]{"data"}, new String[]{null});
        assertNull(row.getBytes(0));
    }

    // =====================================================================
    // Row — generic get with MapperRegistry
    // =====================================================================

    @Test
    void getTypedString() {
        var row = textRow(new String[]{"s"}, new String[]{"hello"});
        assertEquals("hello", row.get(0, String.class));
    }

    @Test
    void getTypedInteger() {
        var row = textRow(new String[]{"n"}, new String[]{"42"});
        assertEquals(42, row.get(0, Integer.class));
    }

    @Test
    void getTypedNull() {
        var row = textRow(new String[]{"n"}, new String[]{null});
        assertNull(row.get(0, Integer.class));
    }

    @Test
    void getTypedWithoutMapperRegistry() {
        ColumnDescriptor[] cols = {col("a")};
        byte[][] vals = {textBytes("hello")};
        var row = new Row(cols, vals, null, null);
        assertThrows(IllegalStateException.class, () -> row.get(0, String.class));
    }

    // =====================================================================
    // Row — column lookup errors
    // =====================================================================

    @Test
    void invalidColumnName() {
        var row = textRow(new String[]{"a"}, new String[]{"1"});
        assertThrows(IllegalArgumentException.class, () -> row.getString("nonexistent"));
    }

    // =====================================================================
    // Row — named access
    // =====================================================================

    @Test
    void allNamedAccessors() {
        var row = textRow(
            new String[]{"i", "l", "s", "f", "d", "bd", "b", "ld", "lt", "ldt"},
            new String[]{"42", "100", "5", "1.5", "2.5", "99.99", "t", "2025-01-01", "10:30:00", "2025-01-01T10:30:00"}
        );
        assertEquals(42, row.getInteger("i"));
        assertEquals(100L, row.getLong("l"));
        assertEquals((short) 5, row.getShort("s"));
        assertEquals(1.5f, row.getFloat("f"), 0.01f);
        assertEquals(2.5, row.getDouble("d"), 0.01);
        assertEquals(new BigDecimal("99.99"), row.getBigDecimal("bd"));
        assertTrue(row.getBoolean("b"));
        assertEquals(LocalDate.of(2025, 1, 1), row.getLocalDate("ld"));
        assertEquals(LocalTime.of(10, 30, 0), row.getLocalTime("lt"));
        assertEquals(LocalDateTime.of(2025, 1, 1, 10, 30, 0), row.getLocalDateTime("ldt"));
    }

    // =====================================================================
    // RowSet — basic operations
    // =====================================================================

    @Test
    void rowSetSize() {
        var row = textRow(new String[]{"a"}, new String[]{"1"});
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
        var row = textRow(new String[]{"a"}, new String[]{"1"});
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
        var r1 = textRow(new String[]{"a"}, new String[]{"1"});
        var r2 = textRow(new String[]{"a"}, new String[]{"2"});
        var rs = new RowSet(List.of(r1, r2), List.of(col("a")), 0);
        assertEquals(2, rs.stream().count());
    }

    @Test
    void rowSetIterator() {
        var r1 = textRow(new String[]{"a"}, new String[]{"1"});
        var rs = new RowSet(List.of(r1), List.of(col("a")), 0);
        int count = 0;
        for (var r : rs) count++;
        assertEquals(1, count);
    }

    // =====================================================================
    // RowSet — mapTo / mapFirst
    // =====================================================================

    @Test
    void rowSetMapTo() {
        var r1 = textRow(new String[]{"n"}, new String[]{"1"});
        var r2 = textRow(new String[]{"n"}, new String[]{"2"});
        var rs = new RowSet(List.of(r1, r2), List.of(col("n")), 0);

        List<Integer> result = rs.mapTo(row -> row.getInteger("n"));
        assertEquals(List.of(1, 2), result);
    }

    @Test
    void rowSetMapFirst() {
        var row = textRow(new String[]{"n"}, new String[]{"42"});
        var rs = new RowSet(List.of(row), List.of(col("n")), 0);
        assertEquals(Integer.valueOf(42), rs.mapFirst(r -> r.getInteger("n")));
    }

    @Test
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
        assertTrue(rs.hasError());
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
    void rowSetNoError() {
        var rs = new RowSet(List.of(), List.of(), 0);
        assertFalse(rs.hasError());
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
    void dbExceptionNullFields() {
        var e = new DbException(null, null, "oops");
        String s = e.toString();
        assertTrue(s.contains("oops"));
    }
}
