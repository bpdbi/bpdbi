package io.github.bpdbi.core;

import io.github.bpdbi.core.impl.ColumnBuffer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A single row in a query result with lazy decoding.
 *
 * <p>Values are stored as raw bytes from the wire and decoded only when a typed getter is called
 * ({@link #getString(int)}, {@link #getInteger(int)}, etc.). Rows are returned in binary format and
 * decoded via {@link BinaryCodec}. Columns you never read are never decoded, keeping CPU overhead
 * minimal.
 *
 * <p>All getters accept either a column index ({@code int}, zero-based) or a column name ({@code
 * String}). NULL values return {@code null} — check with {@link #isNull(int)}.
 *
 * <p>For types not covered by the built-in getters, use {@link #get(int, Class)} which delegates to
 * the {@link ColumnMapperRegistry} or {@link JsonMapper} for deserialization.
 *
 * @see RowSet
 * @see ColumnDescriptor
 */
public final class Row {

  private static final byte[][] EMPTY_VALUES = new byte[0][];

  private final ColumnDescriptor[] columns;

  // Cached column name → index map, built lazily on first by-name access.
  // Shared across all Row objects that reference the same ColumnDescriptor[].
  private final @NonNull Map<String, Integer> columnNameIndex;

  // Backing mode 1: per-row byte[][] (used by streaming and legacy path).
  // Inner byte[] elements are nullable (SQL NULL) but NullAway cannot model this on
  // multidimensional primitive arrays, so null handling is done manually in accessors.
  // Non-final to allow streaming reuse via resetForStreaming().
  private byte[][] values;

  // Backing mode 2: column 'buffers' and 'rowIndex' (used by buffered result sets).
  // Nullable because it is null when backed by mode 1 (the 'byte[][]').
  private final ColumnBuffer @Nullable [] buffers;
  private final int rowIndex;

  private final @NonNull BinaryCodec binaryCodec;
  private final @Nullable ColumnMapperRegistry mapperRegistry;
  private final @Nullable JsonMapper jsonMapper;
  private final Set<Class<?>> jsonTypes;

  /**
   * Per-row byte[][] constructor. Each row owns an independent {@code byte[][]} with one entry per
   * column. Used by streaming paths and the direct-row response path where per-row allocation is
   * acceptable. This and the other multi-arg constructor are intended for driver internals ({@link
   * io.github.bpdbi.core.impl.BaseConnection} factory methods). Prefer the 4-arg convenience
   * constructor for tests.
   *
   * <p>Pass a pre-built {@code columnNameIndex} when creating many rows from the same column set
   * (e.g. streaming) to avoid rebuilding the map per row. Pass {@code null} to build it
   * automatically from the column descriptors.
   */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      @Nullable Map<String, Integer> columnNameIndex,
      byte @NonNull [][] values,
      @NonNull BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex =
        columnNameIndex != null ? columnNameIndex : buildColumnNameIndex(columns);
    this.values = values;
    this.buffers = null;
    this.rowIndex = -1;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /**
   * Column-buffer-backed constructor. Row is a lightweight view into shared {@link ColumnBuffer}s —
   * all values for each column are packed contiguously, reducing GC pressure for large result sets.
   *
   * <p>Pass a pre-built {@code columnNameIndex} when creating many rows from the same column set to
   * share the map across rows. Pass {@code null} to build it automatically from the column
   * descriptors.
   */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      @Nullable Map<String, Integer> columnNameIndex,
      @NonNull ColumnBuffer[] buffers,
      int rowIndex,
      @NonNull BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex =
        columnNameIndex != null ? columnNameIndex : buildColumnNameIndex(columns);
    this.values = EMPTY_VALUES;
    this.buffers = buffers;
    this.rowIndex = rowIndex;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /** Convenience constructor for tests and simple usage (no JSON support). */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      byte @NonNull [][] values,
      @NonNull BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry) {
    this(columns, null, values, binaryCodec, mapperRegistry, null, Set.of());
  }

  /**
   * Reset this row's backing byte arrays for reuse in streaming. Not thread-safe — matches
   * single-threaded connection design. Callers must not retain references to a recycled row.
   */
  public void resetForStreaming(byte @NonNull [][] newValues) {
    this.values = newValues;
  }

  public int size() {
    return columns.length;
  }

  // ---- Column value access ----
  // Performance note: column value access is the innermost hot loop for result-set processing.
  // We read buffer, offset, and length in a single pass through readValue(). The buf/off/len triple
  // is
  // stored in mutable thread-local-like fields (this class is not thread-safe by design) to avoid
  // allocating a holder object per access.

  // Scratch fields for readValue() — avoids allocating a holder object on every getter call.
  // Safe because Row is single-threaded (one connection = one thread).
  private byte[] vBuf;
  private int vOff;
  private int vLen;

  /**
   * Read buffer, offset, and length for a column into scratch fields in one pass. Returns false if
   * the value is SQL NULL. After a true return, vBuf/vOff/vLen hold the value coordinates.
   */
  //noinspection BooleanMethodIsAlwaysInverted
  private boolean readValue(int index) {
    ColumnBuffer[] b = this.buffers;
    if (b != null) {
      ColumnBuffer col = b[index];
      if (col.isNull(rowIndex)) {
        return false;
      }
      vBuf = col.buffer(rowIndex);
      vOff = col.offset(rowIndex);
      vLen = col.length(rowIndex);
    } else {
      byte[] v = values[index];
      if (v == null) {
        return false;
      }
      vBuf = v;
      vOff = 0;
      vLen = v.length;
    }
    return true;
  }

  public boolean isNull(int index) {
    ColumnBuffer[] b = this.buffers;
    if (b != null) {
      return b[index].isNull(rowIndex);
    }
    return values[index] == null;
  }

  public boolean isNull(@NonNull String columnName) {
    return isNull(columnIndex(columnName));
  }

  // --- Decode helpers ---

  // The decode() helper with lambda dispatch is kept for less-frequently-called getters
  // (date/time, UUID, BigDecimal, etc.) where the overhead is negligible relative to the
  // parsing cost. High-frequency getters (getInteger, getLong, getString, getBoolean) are
  // inlined below to eliminate functional interface dispatch and autoboxing on the hot path.

  private <T> T decode(int index, BinaryCodec.BinaryDecoder<T> binaryDecode) {
    if (!readValue(index)) {
      return null;
    }
    return binaryDecode.decode(binaryCodec, vBuf, vOff, vLen);
  }

  // --- String ---

  public @Nullable String getString(int index) {
    if (!readValue(index)) {
      return null;
    }
    // Fast path for text/varchar: in binary protocol the wire format for text-like
    // types is raw UTF-8 bytes, identical to text protocol. Skipping the type OID switch in
    // decodeToString() avoids unnecessary dispatch for the most common getString() use case.
    if (columns[index].isTextLikeType()) {
      return new String(vBuf, vOff, vLen, StandardCharsets.UTF_8);
    }
    if (columns[index].isJsonType()) {
      return binaryCodec.decodeJson(vBuf, vOff, vLen, columns[index].typeOID());
    }
    return binaryCodec.decodeToString(vBuf, vOff, vLen, columns[index].typeOID());
  }

  public @Nullable String getString(@NonNull String columnName) {
    return getString(columnIndex(columnName));
  }

  // --- Numeric ---
  // Inlined (no lambda dispatch) because these are called in tight result-set loops.
  // Eliminating the BinaryDecoder functional interface dispatch and the
  // generic decode() method saves ~2-3 virtual calls per getter invocation.

  public @Nullable Integer getInteger(int index) {
    if (!readValue(index)) {
      return null;
    }
    return switch (vLen) {
      case 4 -> binaryCodec.decodeInt4(vBuf, vOff);
      case 2 -> (int) binaryCodec.decodeInt2(vBuf, vOff);
      case 8 -> (int) binaryCodec.decodeInt8(vBuf, vOff);
      default -> binaryCodec.decodeInt4(vBuf, vOff);
    };
  }

  public @Nullable Integer getInteger(@NonNull String columnName) {
    return getInteger(columnIndex(columnName));
  }

  /** Primitive int getter — avoids autoboxing. Throws if the column is NULL. */
  public int getIntValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    return switch (vLen) {
      case 4 -> binaryCodec.decodeInt4(vBuf, vOff);
      case 2 -> (int) binaryCodec.decodeInt2(vBuf, vOff);
      case 8 -> (int) binaryCodec.decodeInt8(vBuf, vOff);
      default -> binaryCodec.decodeInt4(vBuf, vOff);
    };
  }

  public int getIntValue(@NonNull String columnName) {
    return getIntValue(columnIndex(columnName));
  }

  public @Nullable Long getLong(int index) {
    if (!readValue(index)) {
      return null;
    }
    return switch (vLen) {
      case 8 -> binaryCodec.decodeInt8(vBuf, vOff);
      case 4 -> (long) binaryCodec.decodeInt4(vBuf, vOff);
      case 2 -> (long) binaryCodec.decodeInt2(vBuf, vOff);
      default -> binaryCodec.decodeInt8(vBuf, vOff);
    };
  }

  public @Nullable Long getLong(@NonNull String columnName) {
    return getLong(columnIndex(columnName));
  }

  /** Primitive long getter — avoids autoboxing. Throws if the column is NULL. */
  public long getLongValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    return switch (vLen) {
      case 8 -> binaryCodec.decodeInt8(vBuf, vOff);
      case 4 -> (long) binaryCodec.decodeInt4(vBuf, vOff);
      case 2 -> (long) binaryCodec.decodeInt2(vBuf, vOff);
      default -> binaryCodec.decodeInt8(vBuf, vOff);
    };
  }

  public long getLongValue(@NonNull String columnName) {
    return getLongValue(columnIndex(columnName));
  }

  public @Nullable Short getShort(int index) {
    if (!readValue(index)) {
      return null;
    }
    return binaryCodec.decodeInt2(vBuf, vOff);
  }

  public @Nullable Short getShort(@NonNull String columnName) {
    return getShort(columnIndex(columnName));
  }

  public @Nullable Float getFloat(int index) {
    if (!readValue(index)) return null;
    return binaryCodec.decodeFloat4(vBuf, vOff);
  }

  public @Nullable Float getFloat(@NonNull String columnName) {
    return getFloat(columnIndex(columnName));
  }

  public @Nullable Double getDouble(int index) {
    if (!readValue(index)) return null;
    return switch (vLen) {
      case 8 -> binaryCodec.decodeFloat8(vBuf, vOff);
      case 4 -> (double) binaryCodec.decodeFloat4(vBuf, vOff);
      default -> binaryCodec.decodeFloat8(vBuf, vOff);
    };
  }

  public @Nullable Double getDouble(@NonNull String columnName) {
    return getDouble(columnIndex(columnName));
  }

  public @Nullable BigDecimal getBigDecimal(int index) {
    return decode(index, BinaryCodec::decodeNumeric);
  }

  public @Nullable BigDecimal getBigDecimal(@NonNull String columnName) {
    return getBigDecimal(columnIndex(columnName));
  }

  public @Nullable Boolean getBoolean(int index) {
    if (!readValue(index)) {
      return null;
    }
    return binaryCodec.decodeBool(vBuf, vOff);
  }

  public @Nullable Boolean getBoolean(@NonNull String columnName) {
    return getBoolean(columnIndex(columnName));
  }

  /** Primitive boolean getter — avoids autoboxing. Throws if the column is NULL. */
  public boolean getBoolValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    return binaryCodec.decodeBool(vBuf, vOff);
  }

  public boolean getBoolValue(@NonNull String columnName) {
    return getBoolValue(columnIndex(columnName));
  }

  // --- Date/time ---

  public @Nullable LocalDate getLocalDate(int index) {
    return decode(index, BinaryCodec::decodeDate);
  }

  public @Nullable LocalDate getLocalDate(@NonNull String columnName) {
    return getLocalDate(columnIndex(columnName));
  }

  public @Nullable LocalTime getLocalTime(int index) {
    return decode(index, BinaryCodec::decodeTime);
  }

  public @Nullable LocalTime getLocalTime(@NonNull String columnName) {
    return getLocalTime(columnIndex(columnName));
  }

  public @Nullable LocalDateTime getLocalDateTime(int index) {
    return decode(index, BinaryCodec::decodeTimestamp);
  }

  public @Nullable LocalDateTime getLocalDateTime(@NonNull String columnName) {
    return getLocalDateTime(columnIndex(columnName));
  }

  public @Nullable OffsetDateTime getOffsetDateTime(int index) {
    return decode(index, BinaryCodec::decodeTimestamptz);
  }

  public @Nullable OffsetDateTime getOffsetDateTime(@NonNull String columnName) {
    return getOffsetDateTime(columnIndex(columnName));
  }

  public @Nullable Instant getInstant(int index) {
    return decode(index, (c, buf, off, len) -> c.decodeTimestamptz(buf, off, len).toInstant());
  }

  public @Nullable Instant getInstant(@NonNull String columnName) {
    return getInstant(columnIndex(columnName));
  }

  public @Nullable OffsetTime getOffsetTime(int index) {
    return decode(index, BinaryCodec::decodeTimetz);
  }

  public @Nullable OffsetTime getOffsetTime(@NonNull String columnName) {
    return getOffsetTime(columnIndex(columnName));
  }

  // --- Other types ---

  public @Nullable UUID getUUID(int index) {
    return decode(index, BinaryCodec::decodeUuid);
  }

  public @Nullable UUID getUUID(@NonNull String columnName) {
    return getUUID(columnIndex(columnName));
  }

  public byte @Nullable [] getBytes(int index) {
    if (!readValue(index)) {
      return null;
    }
    return binaryCodec.decodeBytes(vBuf, vOff, vLen);
  }

  public byte @Nullable [] getBytes(@NonNull String columnName) {
    return getBytes(columnIndex(columnName));
  }

  // --- Generic typed access via ColumnMapper ---

  public <T> @Nullable T get(int index, @NonNull Class<T> type) {
    if (!readValue(index)) {
      return null;
    }
    // JSON deserialization: auto-detect by column OID, or by explicit registration
    if (jsonMapper != null && (columns[index].isJsonType() || jsonTypes.contains(type))) {
      String jsonStr = getString(index);
      if (jsonStr == null) {
        return null;
      }
      return jsonMapper.fromJson(jsonStr, type);
    }
    // Binary path: decode directly when codec supports this type
    if (binaryCodec.canDecode(type)) {
      return binaryCodec.decode(vBuf, vOff, vLen, type);
    }
    // Fallback via ColumnMapper (for custom user-registered types)
    if (mapperRegistry == null) {
      throw new IllegalStateException(
          "No ColumnMapperRegistry available. Use connection.setMapperRegistry() to configure one.");
    }
    String textVal = binaryCodec.decodeToString(vBuf, vOff, vLen, columns[index].typeOID());
    return mapperRegistry.map(type, textVal, columns[index].name());
  }

  public <T> @Nullable T get(@NonNull String columnName, @NonNull Class<T> type) {
    return get(columnIndex(columnName), type);
  }

  // --- Array getters ---

  /**
   * Parse an array column into a list of strings. Uses the binary array decoder. Returns {@code
   * null} if the column is SQL NULL.
   */
  public @Nullable List<String> getStringArray(int index) {
    if (isNull(index)) {
      return null;
    }
    return binaryCodec.decodeArrayElements(getRawBytes(index));
  }

  public @Nullable List<String> getStringArray(@NonNull String columnName) {
    return getStringArray(columnIndex(columnName));
  }

  /** Parse an integer array ({@code int[]}) column. */
  public @Nullable List<Integer> getIntegerArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeInt4(buf, off));
  }

  public @Nullable List<Integer> getIntegerArray(@NonNull String columnName) {
    return getIntegerArray(columnIndex(columnName));
  }

  /** Parse a bigint array ({@code bigint[]}) column. */
  public @Nullable List<Long> getLongArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeInt8(buf, off));
  }

  public @Nullable List<Long> getLongArray(@NonNull String columnName) {
    return getLongArray(columnIndex(columnName));
  }

  /** Parse a double precision array ({@code float8[]}) column. */
  public @Nullable List<Double> getDoubleArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeFloat8(buf, off));
  }

  public @Nullable List<Double> getDoubleArray(@NonNull String columnName) {
    return getDoubleArray(columnIndex(columnName));
  }

  /** Parse a real array ({@code float4[]}) column. */
  public @Nullable List<Float> getFloatArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeFloat4(buf, off));
  }

  public @Nullable List<Float> getFloatArray(@NonNull String columnName) {
    return getFloatArray(columnIndex(columnName));
  }

  /** Parse a smallint array ({@code int2[]}) column. */
  public @Nullable List<Short> getShortArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeInt2(buf, off));
  }

  public @Nullable List<Short> getShortArray(@NonNull String columnName) {
    return getShortArray(columnIndex(columnName));
  }

  /** Parse a boolean array ({@code boolean[]}) column. */
  public @Nullable List<Boolean> getBooleanArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeBool(buf, off));
  }

  public @Nullable List<Boolean> getBooleanArray(@NonNull String columnName) {
    return getBooleanArray(columnIndex(columnName));
  }

  /** Parse a UUID array ({@code uuid[]}) column. */
  public @Nullable List<UUID> getUuidArray(int index) {
    return getTypedArray(index, (buf, off, len) -> binaryCodec.decodeUuid(buf, off, len));
  }

  public @Nullable List<UUID> getUuidArray(@NonNull String columnName) {
    return getUuidArray(columnIndex(columnName));
  }

  /**
   * Typed array getter helper. Uses zero-copy element decoding directly from wire bytes via the
   * binary codec.
   */
  private <T> @Nullable List<T> getTypedArray(
      int index, BinaryCodec.ElementDecoder<T> binaryElementDecoder) {
    if (isNull(index)) {
      return null;
    }
    byte[] raw = getRawBytes(index);
    if (raw != null) {
      return binaryCodec.decodeArray(raw, binaryElementDecoder);
    }
    return null;
  }

  // --- Helpers ---

  /** Get the raw column bytes, or null if NULL. Copies from buffer if needed. */
  private byte @Nullable [] getRawBytes(int index) {
    if (!readValue(index)) {
      return null;
    }
    return copySlice(vBuf, vOff, vLen);
  }

  /** Copy a slice from a buffer into an owned byte[]. Used for binary codec calls. */
  private static byte[] copySlice(byte[] buf, int off, int len) {
    if (off == 0 && len == buf.length) {
      return buf;
    }
    byte[] result = new byte[len];
    System.arraycopy(buf, off, result, 0, len);
    return result;
  }

  private int columnIndex(String name) {
    Integer idx = columnNameIndex.get(name);
    if (idx != null) {
      return idx;
    }
    throw new IllegalArgumentException("No column named: " + name);
  }

  /** Build a column name → index map. O(1) lookups instead of linear scan per getter call. */
  public static @NonNull Map<String, Integer> buildColumnNameIndex(
      @NonNull ColumnDescriptor[] columns) {
    // Size for no rehash: n / 0.75 + 1
    Map<String, Integer> map = new HashMap<>((columns.length * 4 / 3) + 1);
    for (int i = 0; i < columns.length; i++) {
      map.putIfAbsent(columns[i].name(), i);
    }
    return map;
  }
}
