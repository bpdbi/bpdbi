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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A single row in a query result with lazy decoding.
 *
 * <p>Values are stored as raw bytes from the wire and decoded only when a typed getter is called
 * ({@link #getString(int)}, {@link #getInteger(int)}, etc.). Parameterized queries produce
 * binary-format rows (decoded via {@link BinaryCodec}); parameterless queries produce text-format
 * rows (decoded from UTF-8 strings). Columns you never read are never decoded, keeping CPU overhead
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

  private final ColumnDescriptor[] columns;
  // Backing mode 1: per-row byte[][] (used by streaming and legacy path).
  // Inner byte[] elements are nullable (SQL NULL) but NullAway cannot model this on
  // multi-dimensional primitive arrays, so null handling is done manually in accessors.
  private final byte[][] values;
  // Backing mode 2: column buffers + row index (used by buffered result sets)
  private final ColumnData @Nullable [] buffers;
  private final int rowIndex;

  private final @Nullable BinaryCodec binaryCodec;
  private final @Nullable ColumnMapperRegistry mapperRegistry;
  private final @Nullable JsonMapper jsonMapper;
  private final Set<Class<?>> jsonTypes;

  /** Per-row byte[][] constructor (original path, used by streaming). */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      byte @NonNull [][] values,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry,
      @Nullable JsonMapper jsonMapper,
      @NonNull Set<Class<?>> jsonTypes) {
    this.columns = columns;
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
    this.values = new byte[0][];
    this.buffers = buffers;
    this.rowIndex = rowIndex;
    this.binaryCodec = binaryCodec;
    this.mapperRegistry = mapperRegistry;
    this.jsonMapper = jsonMapper;
    this.jsonTypes = jsonTypes;
  }

  /** Backward-compatible constructor (no JSON support). */
  public Row(
      @NonNull ColumnDescriptor[] columns,
      byte @NonNull [][] values,
      @Nullable BinaryCodec binaryCodec,
      @Nullable ColumnMapperRegistry mapperRegistry) {
    this(columns, values, binaryCodec, mapperRegistry, null, Set.of());
  }

  public int size() {
    return columns.length;
  }

  /**
   * Get the backing byte array for a column value (zero-copy for ColumnBuffer path). Returns null
   * if SQL NULL.
   */
  private byte[] getBuffer(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      return b[index].buffer(rowIndex);
    }
    return values[index];
  }

  /** Offset into the buffer returned by {@link #getBuffer}. */
  private int getOffset(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      return b[index].offset(rowIndex);
    }
    return 0;
  }

  /** Length of the value in the buffer returned by {@link #getBuffer}, or -1 for NULL. */
  private int getLength(int index) {
    ColumnData[] b = this.buffers;
    if (b != null) {
      return b[index].length(rowIndex);
    }
    byte[] v = values[index];
    return v == null ? -1 : v.length;
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

  private boolean isBinary() {
    return binaryCodec != null;
  }

  /** Returns the non-null binary codec. Only call when {@link #isBinary} returned true. */
  private BinaryCodec requireBinaryCodec() {
    return Objects.requireNonNull(binaryCodec, "binaryCodec is null but isBinary was true");
  }

  // --- Decode helpers ---

  /**
   * Decodes a raw byte buffer from a column. Handles null checks, binary vs text branching, and
   * buffer slicing. Used by most typed getters to avoid repeating this boilerplate.
   */
  @FunctionalInterface
  private interface RawTextDecoder<T> {

    T decode(byte[] buf, int off, int len);
  }

  private <T> T decode(
      int index, BinaryCodec.BinaryDecoder<T> binaryDecode, RawTextDecoder<T> textDecode) {
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    int off = getOffset(index);
    int len = getLength(index);
    if (isBinary()) {
      return binaryDecode.decode(requireBinaryCodec(), buf, off, len);
    }
    return textDecode.decode(buf, off, len);
  }

  private static <T> RawTextDecoder<T> parseText(Function<String, T> parser) {
    return (buf, off, len) -> parser.apply(new String(buf, off, len, StandardCharsets.UTF_8));
  }

  private static <T> RawTextDecoder<T> parseAscii(Function<String, T> parser) {
    return (buf, off, len) -> parser.apply(new String(buf, off, len, StandardCharsets.US_ASCII));
  }

  // --- String ---

  public @Nullable String getString(int index) {
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    int off = getOffset(index);
    int len = getLength(index);
    if (isBinary()) {
      BinaryCodec bc = requireBinaryCodec();
      if (columns[index].isJsonType()) {
        return bc.decodeJson(buf, off, len, columns[index].typeOID());
      }
      return bc.decodeString(buf, off, len);
    }
    return new String(buf, off, len, StandardCharsets.UTF_8);
  }

  public @Nullable String getString(@NonNull String columnName) {
    return getString(columnIndex(columnName));
  }

  // --- Numeric ---

  public @Nullable Integer getInteger(int index) {
    return decode(index, (c, buf, off, len) -> c.decodeInt4(buf, off), Row::parseIntFromBytes);
  }

  public @Nullable Integer getInteger(@NonNull String columnName) {
    return getInteger(columnIndex(columnName));
  }

  public @Nullable Long getLong(int index) {
    return decode(index, (c, buf, off, len) -> c.decodeInt8(buf, off), Row::parseLongFromBytes);
  }

  public @Nullable Long getLong(@NonNull String columnName) {
    return getLong(columnIndex(columnName));
  }

  public @Nullable Short getShort(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeInt2(buf, off),
        (buf, off, len) -> (short) parseIntFromBytes(buf, off, len));
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
        index, (c, buf, off, len) -> c.decodeFloat8(buf, off), parseAscii(Double::parseDouble));
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
    return decode(index, (c, buf, off, len) -> c.decodeBool(buf, off), Row::parseBoolFromBytes);
  }

  public @Nullable Boolean getBoolean(@NonNull String columnName) {
    return getBoolean(columnIndex(columnName));
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
        parseText(s -> LocalDateTime.parse(s.replace(' ', 'T'))));
  }

  public @Nullable LocalDateTime getLocalDateTime(@NonNull String columnName) {
    return getLocalDateTime(columnIndex(columnName));
  }

  public @Nullable OffsetDateTime getOffsetDateTime(int index) {
    return decode(
        index,
        (c, buf, off, len) -> c.decodeTimestamptz(buf, off, len),
        parseText(s -> OffsetDateTime.parse(s.replace(' ', 'T'))));
  }

  public @Nullable OffsetDateTime getOffsetDateTime(@NonNull String columnName) {
    return getOffsetDateTime(columnIndex(columnName));
  }

  public @Nullable Instant getInstant(int index) {
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    if (isBinary()) {
      return requireBinaryCodec()
          .decodeTimestamptz(buf, getOffset(index), getLength(index))
          .toInstant();
    }
    String s =
        new String(buf, getOffset(index), getLength(index), StandardCharsets.UTF_8)
            .replace(' ', 'T');
    // Try as OffsetDateTime first (has timezone), fall back to LocalDateTime (assume UTC)
    try {
      return OffsetDateTime.parse(s).toInstant();
    } catch (java.time.format.DateTimeParseException e) {
      return LocalDateTime.parse(s).toInstant(java.time.ZoneOffset.UTC);
    }
  }

  public @Nullable Instant getInstant(@NonNull String columnName) {
    return getInstant(columnIndex(columnName));
  }

  public @Nullable OffsetTime getOffsetTime(int index) {
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    if (isBinary()) {
      return requireBinaryCodec().decodeTimetz(buf, getOffset(index), getLength(index));
    }
    String s = new String(buf, getOffset(index), getLength(index), StandardCharsets.UTF_8);
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
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    int off = getOffset(index);
    int len = getLength(index);
    if (isBinary()) {
      return requireBinaryCodec().decodeBytes(buf, off, len);
    }
    // Text format: PostgreSQL bytea hex format \x...
    if (len >= 2 && buf[off] == '\\' && buf[off + 1] == 'x') {
      int hexLen = len - 2;
      byte[] result = new byte[hexLen / 2];
      int hexOff = off + 2;
      for (int i = 0; i < result.length; i++) {
        int hi = Character.digit(buf[hexOff + i * 2], 16);
        int lo = Character.digit(buf[hexOff + i * 2 + 1], 16);
        result[i] = (byte) ((hi << 4) | lo);
      }
      return result;
    }
    return copySlice(buf, off, len);
  }

  public byte @Nullable [] getBytes(@NonNull String columnName) {
    return getBytes(columnIndex(columnName));
  }

  // --- Generic typed access via ColumnMapper ---

  public <T> @Nullable T get(int index, @NonNull Class<T> type) {
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    int off = getOffset(index);
    int len = getLength(index);
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
    if (bc != null && isBinary() && bc.canDecode(type)) {
      return bc.decode(buf, off, len, type);
    }
    // Text fallback via ColumnMapper (for custom user-registered types)
    if (mapperRegistry == null) {
      throw new IllegalStateException(
          "No ColumnMapperRegistry available. Use connection.setColumnMapperRegistry() to configure one.");
    }
    String textVal;
    if (bc != null && isBinary()) {
      textVal = bc.decodeToString(buf, off, len, columns[index].typeOID());
    } else {
      textVal = new String(buf, off, len, StandardCharsets.UTF_8);
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
    byte[] buf = getBuffer(index);
    if (buf == null) {
      return null;
    }
    return copySlice(buf, getOffset(index), getLength(index));
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

  private int columnIndex(String name) {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].name().equals(name)) {
        return i;
      }
    }
    throw new IllegalArgumentException("No column named: " + name);
  }
}
