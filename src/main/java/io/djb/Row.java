package io.djb;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A single row in a query result. Values are decoded from text format on access.
 */
public final class Row {

    private final ColumnDescriptor[] columns;
    private final byte[][] values; // raw text-encoded values, null = SQL NULL

    public Row(ColumnDescriptor[] columns, byte[][] values) {
        this.columns = columns;
        this.values = values;
    }

    public int size() {
        return columns.length;
    }

    public boolean isNull(int index) {
        return values[index] == null;
    }

    public boolean isNull(String columnName) {
        return isNull(columnIndex(columnName));
    }

    // --- Text access ---

    public String getString(int index) {
        return values[index] == null ? null : new String(values[index], StandardCharsets.UTF_8);
    }

    public String getString(String columnName) {
        return getString(columnIndex(columnName));
    }

    // --- Numeric access ---

    public Integer getInteger(int index) {
        return values[index] == null ? null : Integer.parseInt(textValue(index));
    }

    public Integer getInteger(String columnName) {
        return getInteger(columnIndex(columnName));
    }

    public Long getLong(int index) {
        return values[index] == null ? null : Long.parseLong(textValue(index));
    }

    public Long getLong(String columnName) {
        return getLong(columnIndex(columnName));
    }

    public Short getShort(int index) {
        return values[index] == null ? null : Short.parseShort(textValue(index));
    }

    public Float getFloat(int index) {
        return values[index] == null ? null : Float.parseFloat(textValue(index));
    }

    public Double getDouble(int index) {
        return values[index] == null ? null : Double.parseDouble(textValue(index));
    }

    public Double getDouble(String columnName) {
        return getDouble(columnIndex(columnName));
    }

    public BigDecimal getBigDecimal(int index) {
        return values[index] == null ? null : new BigDecimal(textValue(index));
    }

    public BigDecimal getBigDecimal(String columnName) {
        return getBigDecimal(columnIndex(columnName));
    }

    public Boolean getBoolean(int index) {
        if (values[index] == null) return null;
        String v = textValue(index);
        return "t".equals(v) || "true".equalsIgnoreCase(v) || "1".equals(v);
    }

    public Boolean getBoolean(String columnName) {
        return getBoolean(columnIndex(columnName));
    }

    // --- Date/time access ---

    public LocalDate getLocalDate(int index) {
        return values[index] == null ? null : LocalDate.parse(textValue(index));
    }

    public LocalTime getLocalTime(int index) {
        return values[index] == null ? null : LocalTime.parse(textValue(index));
    }

    public LocalDateTime getLocalDateTime(int index) {
        return values[index] == null ? null : LocalDateTime.parse(textValue(index).replace(' ', 'T'));
    }

    public OffsetDateTime getOffsetDateTime(int index) {
        return values[index] == null ? null : OffsetDateTime.parse(textValue(index).replace(' ', 'T'));
    }

    // --- Other types ---

    public UUID getUUID(int index) {
        return values[index] == null ? null : UUID.fromString(textValue(index));
    }

    public UUID getUUID(String columnName) {
        return getUUID(columnIndex(columnName));
    }

    public byte[] getBytes(int index) {
        if (values[index] == null) return null;
        String hex = textValue(index);
        // PostgreSQL bytea hex format: \x followed by hex pairs
        if (hex.startsWith("\\x")) {
            hex = hex.substring(2);
            byte[] result = new byte[hex.length() / 2];
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return result;
        }
        return values[index]; // raw bytes fallback
    }

    public byte[] getBytes(String columnName) {
        return getBytes(columnIndex(columnName));
    }

    // --- Helpers ---

    private String textValue(int index) {
        return new String(values[index], StandardCharsets.UTF_8);
    }

    private int columnIndex(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No column named: " + name);
    }
}
