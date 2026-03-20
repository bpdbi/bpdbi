package io.github.bpdbi.core;

import io.github.bpdbi.core.impl.PgArrayParser;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A single row in a query result with lazy decoding.
 *
 * <p>Values are stored as raw bytes from the wire and decoded only when a typed getter is called
 * ({@link #getString(int)}, {@link #getInteger(int)}, etc.). Postgres returns binary-format rows
 * (decoded via {@link BinaryCodec}); MySQL returns text-format rows (decoded from UTF-8 strings).
 * Columns you never read are never decoded, keeping CPU overhead minimal.
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

  private final ColumnDescriptor[] columns;
  // Cached column name → index map, built lazily on first by-name access.
  // Shared across all Row objects that reference the same ColumnDescriptor[].
  private final @Nullable Map<String, Integer> columnNameIndex;
  // Backing mode 1: per-row byte[][] (used by streaming and legacy path).
  // Inner byte[] elements are nullable (SQL NULL) but NullAway cannot model this on
  // multi-dimensional primitive arrays, so null handling is done manually in accessors.
  // Non-final to allow streaming reuse via resetForStreaming().
  private byte[][] values;
  // Backing mode 2: column buffers + row index (used by buffered result sets)
  private final ColumnData @Nullable [] buffers;
  private final int rowIndex;

  private final @Nullable BinaryCodec binaryCodec;
  private final @Nullable ColumnMapperRegistry mapperRegistry;
  private final @Nullable JsonMapper jsonMapper;
  private final Set<Class<?>> jsonTypes;

  /**
   * Per-row byte[][] constructor (used by streaming paths). This and the other multi-arg
   * constructors are intended for driver internals ({@link
   * io.github.bpdbi.core.impl.BaseConnection} factory methods). Prefer the 4-arg convenience
   * constructor for tests.
   */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      byte @NonNull [][] values,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex = buildColumnNameIndex(columns);
    this.values = values;
    this.buffers = null;
    this.rowIndex = -1;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /**
   * Per-row byte[][] constructor with a pre-built column name index. Use this when creating many
   * rows from the same column set (e.g. streaming) to avoid rebuilding the map per row.
   */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      @NonNull Map<String, Integer> columnNameIndex,
      byte @NonNull [][] values,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex = columnNameIndex;
    this.values = values;
    this.buffers = null;
    this.rowIndex = -1;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /** Column-buffer-backed constructor. Row is a lightweight view into shared buffers. */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      @NonNull ColumnData[] buffers,
      int rowIndex,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex = buildColumnNameIndex(columns);
    this.values = new byte[0][];
    this.buffers = buffers;
    this.rowIndex = rowIndex;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /**
   * Column-buffer-backed constructor with a pre-built column name index. Use this when creating
   * many rows from the same column set to share the map across rows.
   */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      @NonNull Map<String, Integer> columnNameIndex,
      @NonNull ColumnData[] buffers,
      int rowIndex,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
    this.columnNameIndex = columnNameIndex;
    this.values = new byte[0][];
    this.buffers = buffers;
    this.rowIndex = rowIndex;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /** Convenience constructor for tests and simple usage (no JSON support, no binary codec). */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      byte @NonNull [][] values,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry) {
    this(columns, values, binaryCodec, mapperRegistry, null, Set.of());
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
  // We read buffer, offset, and length in a single pass through readValue() to avoid 3 separate
  // interface dispatches per column through the ColumnData interface. The buf/off/len triple is
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
  private boolean readValue(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      ColumnData col = b[index];
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

  /**
   * Get the backing byte array for a column value (zero-copy for ColumnBuffer path). Returns null
   * if SQL NULL. Used by getBytes() and array getters that need the raw buffer.
   */
  private byte[] getBuffer(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      return b[index].buffer(rowIndex);
    }
    return values[index];
  }

  public boolean isNull(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      return b[index].isNull(rowIndex);
    }
    return values[index] == null;
  }

  public boolean isNull(@NonNull String columnName) {
    return isNull(columnIndex(columnName));
  }

  /** Returns the non-null binary codec. Only call when binaryCodec is known to be set. */
  private BinaryCodec requireBinaryCodec() {
    BinaryCodec bc = this.binaryCodec;
    if (bc == null) {
      throw new IllegalStateException("binaryCodec is null");
    }
    return bc;
  }

  // --- Decode helpers ---

  // The decode() helper with lambda dispatch is kept for less-frequently-called getters
  // (date/time, UUID, BigDecimal, etc.) where the overhead is negligible relative to the
  // parsing cost. High-frequency getters (getInteger, getLong, getString, getBoolean) are
  // inlined below to eliminate functional interface dispatch and autoboxing on the hot path.

  @FunctionalInterface
  private interface RawTextDecoder<T> {

    T decode(byte[] buf, int off, int len);
  }

  private <T> T decode(
      int index, BinaryCodec.BinaryDecoder<T> binaryDecode, RawTextDecoder<T> textDecode) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return binaryDecode.decode(bc, vBuf, vOff, vLen);
    }
    return textDecode.decode(vBuf, vOff, vLen);
  }

  private static <T> RawTextDecoder<T> parseText(Function<String, T> parser) {
    return (buf, off, len) -> parser.apply(new String(buf, off, len, StandardCharsets.UTF_8));
  }

  private static <T> RawTextDecoder<T> parseAscii(Function<String, T> parser) {
    return (buf, off, len) -> parser.apply(new String(buf, off, len, StandardCharsets.US_ASCII));
  }

  // --- String ---

  public @Nullable String getString(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      // Fast path for text/varchar: in Postgres binary protocol the wire format for text-like
      // types is raw UTF-8 bytes, identical to text protocol. Skipping the type OID switch in
      // decodeToString() avoids unnecessary dispatch for the most common getString() use case.
      if (columns[index].isTextLikeType()) {
        return new String(vBuf, vOff, vLen, StandardCharsets.UTF_8);
      }
      if (columns[index].isJsonType()) {
        return bc.decodeJson(vBuf, vOff, vLen, columns[index].typeOID());
      }
      return bc.decodeToString(vBuf, vOff, vLen, columns[index].typeOID());
    }
    return new String(vBuf, vOff, vLen, StandardCharsets.UTF_8);
  }

  public @Nullable String getString(@NonNull String columnName) {
    return getString(columnIndex(columnName));
  }

  // --- Numeric ---
  // Inlined (no lambda dispatch) because these are called in tight result-set loops.
  // Eliminating the BinaryDecoder/RawTextDecoder functional interface dispatch and the
  // generic decode() method saves ~2-3 virtual calls per getter invocation.

  public @Nullable Integer getInteger(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return switch (vLen) {
        case 4 -> bc.decodeInt4(vBuf, vOff);
        case 2 -> (int) bc.decodeInt2(vBuf, vOff);
        case 8 -> (int) bc.decodeInt8(vBuf, vOff);
        default -> bc.decodeInt4(vBuf, vOff);
      };
    }
    return parseIntFromBytes(vBuf, vOff, vLen);
  }

  public @Nullable Integer getInteger(@NonNull String columnName) {
    return getInteger(columnIndex(columnName));
  }

  /** Primitive int getter — avoids autoboxing. Throws if the column is NULL. */
  public int getIntValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return switch (vLen) {
        case 4 -> bc.decodeInt4(vBuf, vOff);
        case 2 -> (int) bc.decodeInt2(vBuf, vOff);
        case 8 -> (int) bc.decodeInt8(vBuf, vOff);
        default -> bc.decodeInt4(vBuf, vOff);
      };
    }
    return parseIntFromBytes(vBuf, vOff, vLen);
  }

  public int getIntValue(@NonNull String columnName) {
    return getIntValue(columnIndex(columnName));
  }

  public @Nullable Long getLong(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return switch (vLen) {
        case 8 -> bc.decodeInt8(vBuf, vOff);
        case 4 -> (long) bc.decodeInt4(vBuf, vOff);
        case 2 -> (long) bc.decodeInt2(vBuf, vOff);
        default -> bc.decodeInt8(vBuf, vOff);
      };
    }
    return parseLongFromBytes(vBuf, vOff, vLen);
  }

  public @Nullable Long getLong(@NonNull String columnName) {
    return getLong(columnIndex(columnName));
  }

  /** Primitive long getter — avoids autoboxing. Throws if the column is NULL. */
  public long getLongValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return switch (vLen) {
        case 8 -> bc.decodeInt8(vBuf, vOff);
        case 4 -> (long) bc.decodeInt4(vBuf, vOff);
        case 2 -> (long) bc.decodeInt2(vBuf, vOff);
        default -> bc.decodeInt8(vBuf, vOff);
      };
    }
    return parseLongFromBytes(vBuf, vOff, vLen);
  }

  public long getLongValue(@NonNull String columnName) {
    return getLongValue(columnIndex(columnName));
  }

  public @Nullable Short getShort(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return bc.decodeInt2(vBuf, vOff);
    }
    return (short) parseIntFromBytes(vBuf, vOff, vLen);
  }

  public @Nullable Short getShort(@NonNull String columnName) {
    return getShort(columnIndex(columnName));
  }

  public @Nullable Float getFloat(int index) {
    return decode(
        index, (c, buf, off, len) -> c.decodeFloat4(buf, off), parseAscii(Float::parseFloat));
  }

  public @Nullable Float getFloat(@NonNull String columnName) {
    return getFloat(columnIndex(columnName));
  }

  public @Nullable Double getDouble(int index) {
    return decode(
        index,
        (c, buf, off, len) ->
            switch (len) {
              case 8 -> c.decodeFloat8(buf, off);
              case 4 -> (double) c.decodeFloat4(buf, off);
              default -> c.decodeFloat8(buf, off);
            },
        parseAscii(Double::parseDouble));
  }

  public @Nullable Double getDouble(@NonNull String columnName) {
    return getDouble(columnIndex(columnName));
  }

  public @Nullable BigDecimal getBigDecimal(int index) {
    return decode(
        index, (c, buf, off, len) -> c.decodeNumeric(buf, off, len), parseAscii(BigDecimal::new));
  }

  public @Nullable BigDecimal getBigDecimal(@NonNull String columnName) {
    return getBigDecimal(columnIndex(columnName));
  }

  public @Nullable Boolean getBoolean(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return bc.decodeBool(vBuf, vOff);
    }
    return parseBoolFromBytes(vBuf, vOff, vLen);
  }

  public @Nullable Boolean getBoolean(@NonNull String columnName) {
    return getBoolean(columnIndex(columnName));
  }

  /** Primitive boolean getter — avoids autoboxing. Throws if the column is NULL. */
  public boolean getBoolValue(int index) {
    if (!readValue(index)) {
      throw new NullPointerException("Column " + index + " is NULL");
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return bc.decodeBool(vBuf, vOff);
    }
    return parseBoolFromBytes(vBuf, vOff, vLen);
  }

  public boolean getBoolValue(@NonNull String columnName) {
    return getBoolValue(columnIndex(columnName));
  }

  // --- Date/time ---

  public @Nullable LocalDate getLocalDate(int index) {
    return decode(
        index, (c, buf, off, len) -> c.decodeDate(buf, off, len), parseText(LocalDate::parse));
  }

  public @Nullable LocalDate getLocalDate(@NonNull String columnName) {
    return getLocalDate(columnIndex(columnName));
  }

  public @Nullable LocalTime getLocalTime(int index) {
    return decode(
        index, (c, buf, off, len) -> c.decodeTime(buf, off, len), parseText(LocalTime::parse));
  }

  public @Nullable LocalTime getLocalTime(@NonNull String columnName) {
    return getLocalTime(columnIndex(columnName));
  }

  public @Nullable LocalDateTime getLocalDateTime(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeTimestamp(buf, off, len),
        Row::parseTimestampFromBytes);
  }

  public @Nullable LocalDateTime getLocalDateTime(@NonNull String columnName) {
    return getLocalDateTime(columnIndex(columnName));
  }

  public @Nullable OffsetDateTime getOffsetDateTime(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeTimestamptz(buf, off, len),
        Row::parseOffsetTimestampFromBytes);
  }

  public @Nullable OffsetDateTime getOffsetDateTime(@NonNull String columnName) {
    return getOffsetDateTime(columnIndex(columnName));
  }

  public @Nullable Instant getInstant(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeTimestamptz(buf, off, len).toInstant(),
        (buf, off, len) -> {
          String s = new String(buf, off, len, StandardCharsets.UTF_8).replace(' ', 'T');
          // Try as OffsetDateTime first (has timezone), fall back to LocalDateTime (assume UTC)
          try {
            return OffsetDateTime.parse(s).toInstant();
          } catch (java.time.format.DateTimeParseException e) {
            return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
          }
        });
  }

  public @Nullable Instant getInstant(@NonNull String columnName) {
    return getInstant(columnIndex(columnName));
  }

  public @Nullable OffsetTime getOffsetTime(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeTimetz(buf, off, len),
        (buf, off, len) -> {
          String s = new String(buf, off, len, StandardCharsets.UTF_8);
          // PG returns short offsets like +02 but Java requires +02:00
          int sLen = s.length();
          if (sLen >= 3) {
            char sign = s.charAt(sLen - 3);
            if ((sign == '+' || sign == '-')
                && Character.isDigit(s.charAt(sLen - 2))
                && Character.isDigit(s.charAt(sLen - 1))) {
              s = s + ":00";
            }
          }
          return OffsetTime.parse(s);
        });
  }

  public @Nullable OffsetTime getOffsetTime(@NonNull String columnName) {
    return getOffsetTime(columnIndex(columnName));
  }

  // --- Other types ---

  public @Nullable UUID getUUID(int index) {
    return decode(
        index, (c, buf, off, len) -> c.decodeUuid(buf, off, len), parseText(UUID::fromString));
  }

  public @Nullable UUID getUUID(@NonNull String columnName) {
    return getUUID(columnIndex(columnName));
  }

  public byte @Nullable [] getBytes(int index) {
    if (!readValue(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      return bc.decodeBytes(vBuf, vOff, vLen);
    }
    // Text format: PostgreSQL bytea hex format \x...
    if (vLen >= 2 && vBuf[vOff] == '\\' && vBuf[vOff + 1] == 'x') {
      int hexLen = vLen - 2;
      byte[] result = new byte[hexLen / 2];
      int hexOff = vOff + 2;
      for (int i = 0; i < result.length; i++) {
        int hi = Character.digit(vBuf[hexOff + i * 2], 16);
        int lo = Character.digit(vBuf[hexOff + i * 2 + 1], 16);
        result[i] = (byte) ((hi << 4) | lo);
      }
      return result;
    }
    return copySlice(vBuf, vOff, vLen);
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
    BinaryCodec bc = this.binaryCodec;
    if (bc != null && bc.canDecode(type)) {
      return bc.decode(vBuf, vOff, vLen, type);
    }
    // Text fallback via ColumnMapper (for custom user-registered types)
    if (mapperRegistry == null) {
      throw new IllegalStateException(
          "No ColumnMapperRegistry available. Use connection.setMapperRegistry() to configure one.");
    }
    String textVal;
    if (bc != null) {
      textVal = bc.decodeToString(vBuf, vOff, vLen, columns[index].typeOID());
    } else {
      textVal = new String(vBuf, vOff, vLen, StandardCharsets.UTF_8);
    }
    return mapperRegistry.map(type, textVal, columns[index].name());
  }

  public <T> @Nullable T get(@NonNull String columnName, @NonNull Class<T> type) {
    return get(columnIndex(columnName), type);
  }

  // --- Array getters ---

  /**
   * Parse a Postgres array column into a list of strings. Uses the binary array decoder when
   * available (parameterized queries), falls back to text-format parsing. Returns {@code null} if
   * the column is SQL NULL.
   */
  public @Nullable List<String> getStringArray(int index) {
    if (isNull(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      byte[] raw = getRawBytes(index);
      if (raw != null) {
        List<String> elements = bc.decodeArrayElements(raw);
        if (elements != null) {
          return elements;
        }
      }
    }
    // Text format fallback — only parse PG array syntax {val1,val2,...}
    String text = getString(index);
    if (text != null && !text.isEmpty() && text.charAt(0) == '{') {
      return PgArrayParser.parse(text);
    }
    return null;
  }

  public @Nullable List<String> getStringArray(@NonNull String columnName) {
    return getStringArray(columnIndex(columnName));
  }

  /** Parse a Postgres integer array ({@code int[]}) column. */
  public @Nullable List<Integer> getIntegerArray(int index) {
    return getTypedArray(
        index, (buf, off, len) -> requireBinaryCodec().decodeInt4(buf, off), Integer::parseInt);
  }

  public @Nullable List<Integer> getIntegerArray(@NonNull String columnName) {
    return getIntegerArray(columnIndex(columnName));
  }

  /** Parse a Postgres bigint array ({@code bigint[]}) column. */
  public @Nullable List<Long> getLongArray(int index) {
    return getTypedArray(
        index, (buf, off, len) -> requireBinaryCodec().decodeInt8(buf, off), Long::parseLong);
  }

  public @Nullable List<Long> getLongArray(@NonNull String columnName) {
    return getLongArray(columnIndex(columnName));
  }

  /** Parse a Postgres double precision array ({@code float8[]}) column. */
  public @Nullable List<Double> getDoubleArray(int index) {
    return getTypedArray(
        index, (buf, off, len) -> requireBinaryCodec().decodeFloat8(buf, off), Double::parseDouble);
  }

  public @Nullable List<Double> getDoubleArray(@NonNull String columnName) {
    return getDoubleArray(columnIndex(columnName));
  }

  /** Parse a Postgres real array ({@code float4[]}) column. */
  public @Nullable List<Float> getFloatArray(int index) {
    return getTypedArray(
        index, (buf, off, len) -> requireBinaryCodec().decodeFloat4(buf, off), Float::parseFloat);
  }

  public @Nullable List<Float> getFloatArray(@NonNull String columnName) {
    return getFloatArray(columnIndex(columnName));
  }

  /** Parse a Postgres smallint array ({@code int2[]}) column. */
  public @Nullable List<Short> getShortArray(int index) {
    return getTypedArray(
        index, (buf, off, len) -> requireBinaryCodec().decodeInt2(buf, off), Short::parseShort);
  }

  public @Nullable List<Short> getShortArray(@NonNull String columnName) {
    return getShortArray(columnIndex(columnName));
  }

  /** Parse a Postgres boolean array ({@code boolean[]}) column. */
  public @Nullable List<Boolean> getBooleanArray(int index) {
    return getTypedArray(
        index,
        (buf, off, len) -> requireBinaryCodec().decodeBool(buf, off),
        s -> "t".equals(s) || "true".equalsIgnoreCase(s));
  }

  public @Nullable List<Boolean> getBooleanArray(@NonNull String columnName) {
    return getBooleanArray(columnIndex(columnName));
  }

  /**
   * Typed array getter helper. Binary path uses zero-copy element decoding directly from wire
   * bytes. Text path parses PG array syntax and converts each element string.
   */
  private <T> @Nullable List<T> getTypedArray(
      int index,
      BinaryCodec.ElementDecoder<T> binaryElementDecoder,
      Function<String, T> textParser) {
    if (isNull(index)) {
      return null;
    }
    BinaryCodec bc = this.binaryCodec;
    if (bc != null) {
      byte[] raw = getRawBytes(index);
      if (raw != null) {
        List<T> result = bc.decodeArray(raw, binaryElementDecoder);
        if (result != null) {
          return result;
        }
      }
    }
    // Text format fallback — only parse PG array syntax {val1,val2,...}
    String text = getString(index);
    if (text != null && !text.isEmpty() && text.charAt(0) == '{') {
      return PgArrayParser.parse(text).stream().map(textParser).toList();
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

  /** Parse a boolean from text wire format. Handles 't', '1', and case-insensitive "true". */
  private static boolean parseBoolFromBytes(byte[] buf, int off, int len) {
    if (len == 1) {
      byte b = buf[off];
      return b == 't' || b == '1';
    }
    // "true" / "false" — only "true" (4 chars) is truthy
    return len == 4
        && (buf[off] == 't' || buf[off] == 'T')
        && (buf[off + 1] == 'r' || buf[off + 1] == 'R')
        && (buf[off + 2] == 'u' || buf[off + 2] == 'U')
        && (buf[off + 3] == 'e' || buf[off + 3] == 'E');
  }

  /** Parse an int directly from ASCII bytes without creating an intermediate String. */
  private static int parseIntFromBytes(byte[] buf, int off, int len) {
    if (len == 0) {
      throw new NumberFormatException("Empty string");
    }
    int end = off + len;
    boolean negative = false;
    int i = off;
    if (buf[i] == '-') {
      negative = true;
      i++;
    } else if (buf[i] == '+') {
      i++;
    }
    if (i == end) {
      throw new NumberFormatException("No digits");
    }
    int result = 0;
    while (i < end) {
      int digit = buf[i++] - '0';
      if (digit < 0 || digit > 9) {
        throw new NumberFormatException(
            "Invalid digit at position "
                + (i - 1 - off)
                + " in: "
                + new String(buf, off, len, StandardCharsets.UTF_8));
      }
      result = result * 10 + digit;
    }
    return negative ? -result : result;
  }

  /** Parse a long directly from ASCII bytes without creating an intermediate String. */
  private static long parseLongFromBytes(byte[] buf, int off, int len) {
    if (len == 0) {
      throw new NumberFormatException("Empty string");
    }
    int end = off + len;
    boolean negative = false;
    int i = off;
    if (buf[i] == '-') {
      negative = true;
      i++;
    } else if (buf[i] == '+') {
      i++;
    }
    if (i == end) {
      throw new NumberFormatException("No digits");
    }
    long result = 0;
    while (i < end) {
      int digit = buf[i++] - '0';
      if (digit < 0 || digit > 9) {
        throw new NumberFormatException(
            "Invalid digit at position "
                + (i - 1 - off)
                + " in: "
                + new String(buf, off, len, StandardCharsets.UTF_8));
      }
      result = result * 10 + digit;
    }
    return negative ? -result : result;
  }

  /**
   * Parse a Postgres timestamp text representation directly from bytes without intermediate String
   * allocation. Format: "2025-06-01 10:30:45" or "2025-06-01 10:30:45.123456".
   */
  private static LocalDateTime parseTimestampFromBytes(byte[] buf, int off, int len) {
    // Minimum: "YYYY-MM-DD HH:MM:SS" = 19 chars
    if (len < 19) {
      // Fall back for short/unusual formats
      return LocalDateTime.parse(
          new String(buf, off, len, StandardCharsets.UTF_8).replace(' ', 'T'));
    }
    int year = parseDigits4(buf, off);
    int month = parseDigits2(buf, off + 5);
    int day = parseDigits2(buf, off + 8);
    int hour = parseDigits2(buf, off + 11);
    int minute = parseDigits2(buf, off + 14);
    int second = parseDigits2(buf, off + 17);
    int nano = len > 19 && buf[off + 19] == '.' ? parseFraction(buf, off + 20, len - 20) : 0;
    return LocalDateTime.of(year, month, day, hour, minute, second, nano);
  }

  /**
   * Parse a Postgres timestamptz text representation directly from bytes. Format: "2025-06-01
   * 10:30:45+02" or "2025-06-01 10:30:45.123456+02:00".
   */
  private static OffsetDateTime parseOffsetTimestampFromBytes(byte[] buf, int off, int len) {
    // Find the timezone offset: scan backward for +/-
    int tzPos = -1;
    for (int i = off + len - 1; i >= off + 19; i--) {
      if (buf[i] == '+' || buf[i] == '-') {
        tzPos = i;
        break;
      }
    }
    if (tzPos == -1) {
      // No timezone found — fall back
      return OffsetDateTime.parse(
          new String(buf, off, len, StandardCharsets.UTF_8).replace(' ', 'T'));
    }
    // Parse the datetime portion
    int year = parseDigits4(buf, off);
    int month = parseDigits2(buf, off + 5);
    int day = parseDigits2(buf, off + 8);
    int hour = parseDigits2(buf, off + 11);
    int minute = parseDigits2(buf, off + 14);
    int second = parseDigits2(buf, off + 17);
    int dotEnd = tzPos;
    int nano =
        dotEnd > off + 19 && buf[off + 19] == '.'
            ? parseFraction(buf, off + 20, dotEnd - off - 20)
            : 0;
    // Parse timezone: +HH, +HH:MM, or +HH:MM:SS
    int tzLen = off + len - tzPos;
    int sign = buf[tzPos] == '-' ? -1 : 1;
    int tzHour = parseDigits2(buf, tzPos + 1);
    int tzMin = tzLen >= 6 ? parseDigits2(buf, tzPos + 4) : 0;
    ZoneOffset offset = ZoneOffset.ofHoursMinutes(sign * tzHour, sign * tzMin);
    return OffsetDateTime.of(year, month, day, hour, minute, second, nano, offset);
  }

  private static int parseDigits4(byte[] buf, int off) {
    return (buf[off] - '0') * 1000
        + (buf[off + 1] - '0') * 100
        + (buf[off + 2] - '0') * 10
        + (buf[off + 3] - '0');
  }

  private static int parseDigits2(byte[] buf, int off) {
    return (buf[off] - '0') * 10 + (buf[off + 1] - '0');
  }

  /** Parse fractional seconds (digits after '.') into nanoseconds. */
  private static int parseFraction(byte[] buf, int off, int maxLen) {
    int nanos = 0;
    int digits = 0;
    for (int i = off; i < off + maxLen && digits < 9; i++) {
      int d = buf[i] - '0';
      if (d < 0 || d > 9) break;
      nanos = nanos * 10 + d;
      digits++;
    }
    // Pad to 9 digits (nanosecond precision)
    for (int i = digits; i < 9; i++) {
      nanos *= 10;
    }
    return nanos;
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
