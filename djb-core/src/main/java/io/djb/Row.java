package io.djb;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

/**
 * A single row in a query result. Values are stored as raw bytes from the wire
 * (text or binary format) and decoded lazily on access.
 */
public final class Row {

    private final ColumnDescriptor[] columns;
    private final byte @Nullable [][] values; // raw bytes (text OR binary), null = SQL NULL
    private final @Nullable BinaryCodec binaryCodec;
    private final @Nullable MapperRegistry mapperRegistry;
    private final @Nullable JsonMapper jsonMapper;
    private final Set<Class<?>> jsonTypes;

    public Row(ColumnDescriptor[] columns, byte @Nullable [][] values, @Nullable BinaryCodec binaryCodec,
               @Nullable MapperRegistry mapperRegistry, @Nullable JsonMapper jsonMapper, Set<Class<?>> jsonTypes) {
        this.columns = columns;
        this.values = values;
        this.binaryCodec = binaryCodec;
        this.mapperRegistry = mapperRegistry;
        this.jsonMapper = jsonMapper;
        this.jsonTypes = jsonTypes;
    }

    /** Backward-compatible constructor (no JSON support). */
    public Row(ColumnDescriptor[] columns, byte @Nullable [][] values, @Nullable BinaryCodec binaryCodec, @Nullable MapperRegistry mapperRegistry) {
        this(columns, values, binaryCodec, mapperRegistry, null, Set.of());
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

    private boolean isBinary(int index) {
        return columns[index].format() == ColumnDescriptor.FORMAT_BINARY && binaryCodec != null;
    }

    // --- String ---

    public @Nullable String getString(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) {
            if (columns[index].isJsonType()) {
                return binaryCodec.decodeJson(values[index], columns[index].typeOID());
            }
            return binaryCodec.decodeString(values[index]);
        }
        return new String(values[index], StandardCharsets.UTF_8);
    }

    public @Nullable String getString(String columnName) {
        return getString(columnIndex(columnName));
    }

    // --- Numeric ---

    public @Nullable Integer getInteger(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeInt4(values[index]);
        return Integer.parseInt(textValue(index));
    }

    public @Nullable Integer getInteger(String columnName) {
        return getInteger(columnIndex(columnName));
    }

    public @Nullable Long getLong(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeInt8(values[index]);
        return Long.parseLong(textValue(index));
    }

    public @Nullable Long getLong(String columnName) {
        return getLong(columnIndex(columnName));
    }

    public @Nullable Short getShort(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeInt2(values[index]);
        return Short.parseShort(textValue(index));
    }

    public @Nullable Short getShort(String columnName) {
        return getShort(columnIndex(columnName));
    }

    public @Nullable Float getFloat(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeFloat4(values[index]);
        return Float.parseFloat(textValue(index));
    }

    public @Nullable Float getFloat(String columnName) {
        return getFloat(columnIndex(columnName));
    }

    public @Nullable Double getDouble(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeFloat8(values[index]);
        return Double.parseDouble(textValue(index));
    }

    public @Nullable Double getDouble(String columnName) {
        return getDouble(columnIndex(columnName));
    }

    public @Nullable BigDecimal getBigDecimal(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeNumeric(values[index]);
        return new BigDecimal(textValue(index));
    }

    public @Nullable BigDecimal getBigDecimal(String columnName) {
        return getBigDecimal(columnIndex(columnName));
    }

    public @Nullable Boolean getBoolean(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeBool(values[index]);
        String v = textValue(index);
        return "t".equals(v) || "true".equalsIgnoreCase(v) || "1".equals(v);
    }

    public @Nullable Boolean getBoolean(String columnName) {
        return getBoolean(columnIndex(columnName));
    }

    // --- Date/time ---

    public @Nullable LocalDate getLocalDate(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeDate(values[index]);
        return LocalDate.parse(textValue(index));
    }

    public @Nullable LocalDate getLocalDate(String columnName) {
        return getLocalDate(columnIndex(columnName));
    }

    public @Nullable LocalTime getLocalTime(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeTime(values[index]);
        return LocalTime.parse(textValue(index));
    }

    public @Nullable LocalTime getLocalTime(String columnName) {
        return getLocalTime(columnIndex(columnName));
    }

    public @Nullable LocalDateTime getLocalDateTime(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeTimestamp(values[index]);
        return LocalDateTime.parse(textValue(index).replace(' ', 'T'));
    }

    public @Nullable LocalDateTime getLocalDateTime(String columnName) {
        return getLocalDateTime(columnIndex(columnName));
    }

    public @Nullable OffsetDateTime getOffsetDateTime(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeTimestamptz(values[index]);
        return OffsetDateTime.parse(textValue(index).replace(' ', 'T'));
    }

    public @Nullable OffsetDateTime getOffsetDateTime(String columnName) {
        return getOffsetDateTime(columnIndex(columnName));
    }

    // --- Other types ---

    public @Nullable UUID getUUID(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeUuid(values[index]);
        return UUID.fromString(textValue(index));
    }

    public @Nullable UUID getUUID(String columnName) {
        return getUUID(columnIndex(columnName));
    }

    public byte @Nullable [] getBytes(int index) {
        if (values[index] == null) return null;
        if (isBinary(index)) return binaryCodec.decodeBytes(values[index]);
        // Text format: PostgreSQL bytea hex format \x...
        String hex = textValue(index);
        if (hex.startsWith("\\x")) {
            hex = hex.substring(2);
            byte[] result = new byte[hex.length() / 2];
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return result;
        }
        return values[index];
    }

    public byte @Nullable [] getBytes(String columnName) {
        return getBytes(columnIndex(columnName));
    }

    // --- Generic typed access via ColumnMapper ---

    public <T> @Nullable T get(int index, Class<T> type) {
        if (values[index] == null) return null;
        // JSON deserialization: auto-detect by column OID, or by explicit registration
        if (jsonMapper != null && (columns[index].isJsonType() || jsonTypes.contains(type))) {
            String jsonStr = getString(index);
            if (jsonStr == null) return null;
            return jsonMapper.fromJson(jsonStr, type);
        }
        if (mapperRegistry == null) {
            throw new IllegalStateException(
                "No MapperRegistry available. Use connection.setMapperRegistry() to configure one.");
        }
        // For generic typed access, always use text representation
        // (ColumnMappers expect text strings)
        String textVal;
        if (isBinary(index)) {
            textVal = binaryCodec.decodeString(values[index]);
        } else {
            textVal = textValue(index);
        }
        return mapperRegistry.map(type, textVal, columns[index].name());
    }

    public <T> @Nullable T get(String columnName, Class<T> type) {
        return get(columnIndex(columnName), type);
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
