package io.github.bpdbi.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Decodes binary-format column values from raw bytes received on the wire.
 *
 * <p>Each database driver provides its own implementation: Postgres uses big-endian encoding, MySQL
 * uses little-endian. The codec is selected automatically based on the connection type and is used
 * internally by {@link Row} getters to decode binary wire data. Only active for parameterized
 * queries — parameterless queries use text format and bypass the binary codec entirely (see {@link
 * Row} javadoc for why).
 *
 * <p><b>Not user-extensible.</b> This interface is implemented per-driver and handles the fixed set
 * of wire-format types defined by each database protocol. To decode custom types (e.g. Postgres
 * domains, composite types, or enums), use the text-format extension points instead:
 *
 * <ul>
 *   <li>{@link ColumnMapperRegistry} — register a {@link ColumnMapper} to convert text column
 *       values to Java types via {@link Row#get(int, Class)}
 *   <li>{@link BinderRegistry} — register a {@link Binder} to convert Java objects to SQL parameter
 *       strings when binding query parameters
 *   <li>{@link JsonMapper} — plug in a JSON library (Jackson, Gson, etc.) for automatic
 *       serialization of JSON/JSONB columns
 * </ul>
 */
public interface BinaryCodec {

  boolean decodeBool(byte @NonNull [] value);

  short decodeInt2(byte @NonNull [] value);

  int decodeInt4(byte @NonNull [] value);

  long decodeInt8(byte @NonNull [] value);

  float decodeFloat4(byte @NonNull [] value);

  double decodeFloat8(byte @NonNull [] value);

  @NonNull String decodeString(byte @NonNull [] value);

  @NonNull UUID decodeUuid(byte @NonNull [] value);

  @NonNull LocalDate decodeDate(byte @NonNull [] value);

  @NonNull LocalTime decodeTime(byte @NonNull [] value);

  @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] value);

  @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] value);

  @NonNull OffsetTime decodeTimetz(byte @NonNull [] value);

  byte @NonNull [] decodeBytes(byte @NonNull [] value);

  @NonNull BigDecimal decodeNumeric(byte @NonNull [] value);

  /**
   * Decode a JSON/JSONB column to a string. PG JSONB has a 1-byte version prefix that needs
   * stripping.
   */
  @NonNull String decodeJson(byte @NonNull [] value, int typeOID);

  /**
   * Decode a binary array value, applying the given function to each non-null element's raw bytes.
   * The binary array header is parsed by the codec; NULL elements are skipped. Returns {@code null}
   * if this codec does not support binary arrays (e.g. MySQL has no array type).
   */
  default <T> @Nullable List<T> decodeArray(
      byte @NonNull [] value, @NonNull Function<byte[], T> elementDecoder) {
    return null;
  }

  /**
   * Decode a binary array value into a list of element strings. Convenience wrapper around {@link
   * #decodeArray} that converts each element to its text representation based on the element type
   * OID embedded in the binary payload.
   */
  default @Nullable List<String> decodeArrayElements(byte @NonNull [] value) {
    return null;
  }

  /**
   * Decode a binary column value to its text representation, using the type OID to select the
   * correct decoding strategy. Used by {@link Row#get(int, Class)} as the text fallback for column
   * mappers on binary-format rows. The default implementation ignores the OID and delegates to
   * {@link #decodeString(byte[])}; drivers should override this to produce correct text for
   * non-string types (dates, numerics, etc.).
   */
  default @NonNull String decodeToString(byte @NonNull [] value, int typeOID) {
    return decodeString(value);
  }

  /**
   * Whether this codec can decode binary bytes directly to the given Java type. Used by {@link
   * Row#get(int, Class)} to bypass the text roundtrip through {@link ColumnMapper} for types the
   * binary protocol handles natively.
   */
  default boolean canDecode(@NonNull Class<?> type) {
    return type == String.class
        || type == Integer.class
        || type == Long.class
        || type == Short.class
        || type == Float.class
        || type == Double.class
        || type == Boolean.class
        || type == BigDecimal.class
        || type == UUID.class
        || type == LocalDate.class
        || type == LocalTime.class
        || type == LocalDateTime.class
        || type == OffsetDateTime.class
        || type == OffsetTime.class
        || type == Instant.class
        || type == byte[].class;
  }

  /**
   * Decode binary bytes directly to the given Java type, bypassing text conversion.
   *
   * @throws IllegalArgumentException if the type is not supported (check {@link #canDecode} first)
   */
  @SuppressWarnings("unchecked") // For the cast on the return statement
  @NonNull
  default <T> T decode(byte @NonNull [] value, @NonNull Class<T> type) {
    Object result;
    if (type == String.class) {
      result = decodeString(value);
    } else if (type == Integer.class) {
      result = decodeInt4(value);
    } else if (type == Long.class) {
      result = decodeInt8(value);
    } else if (type == Short.class) {
      result = decodeInt2(value);
    } else if (type == Float.class) {
      result = decodeFloat4(value);
    } else if (type == Double.class) {
      result = decodeFloat8(value);
    } else if (type == Boolean.class) {
      result = decodeBool(value);
    } else if (type == BigDecimal.class) {
      result = decodeNumeric(value);
    } else if (type == UUID.class) {
      result = decodeUuid(value);
    } else if (type == LocalDate.class) {
      result = decodeDate(value);
    } else if (type == LocalTime.class) {
      result = decodeTime(value);
    } else if (type == LocalDateTime.class) {
      result = decodeTimestamp(value);
    } else if (type == OffsetDateTime.class) {
      result = decodeTimestamptz(value);
    } else if (type == OffsetTime.class) {
      result = decodeTimetz(value);
    } else if (type == Instant.class) {
      result = decodeTimestamptz(value).toInstant();
    } else if (type == byte[].class) {
      result = decodeBytes(value);
    } else {
      throw new IllegalArgumentException("BinaryCodec cannot decode type: " + type.getName());
    }
    return (T) result;
  }

  // --- Functional interfaces for offset-based decoding ---

  /**
   * Decodes a binary value from a shared buffer at the given offset. Used by {@link Row} for
   * zero-copy binary decoding without intermediate byte[] allocation.
   */
  @FunctionalInterface
  interface BinaryDecoder<T> {

    T decode(@NonNull BinaryCodec codec, byte @NonNull [] buf, int offset, int length);
  }

  /** Decodes an array element from a shared buffer at the given offset. */
  @FunctionalInterface
  interface ElementDecoder<T> {

    T decode(byte @NonNull [] buf, int offset, int length);
  }

  // --- Offset-based decode methods (zero-copy path) ---
  // Default implementations copy a slice and delegate to the original methods.
  // Drivers override these with direct offset-based decoding to eliminate copies.

  default boolean decodeBool(byte @NonNull [] buf, int offset) {
    return decodeBool(copySlice(buf, offset, 1));
  }

  default short decodeInt2(byte @NonNull [] buf, int offset) {
    return decodeInt2(copySlice(buf, offset, 2));
  }

  default int decodeInt4(byte @NonNull [] buf, int offset) {
    return decodeInt4(copySlice(buf, offset, 4));
  }

  default long decodeInt8(byte @NonNull [] buf, int offset) {
    return decodeInt8(copySlice(buf, offset, 8));
  }

  default float decodeFloat4(byte @NonNull [] buf, int offset) {
    return decodeFloat4(copySlice(buf, offset, 4));
  }

  default double decodeFloat8(byte @NonNull [] buf, int offset) {
    return decodeFloat8(copySlice(buf, offset, 8));
  }

  default @NonNull UUID decodeUuid(byte @NonNull [] buf, int offset, int length) {
    return decodeUuid(copySlice(buf, offset, length));
  }

  default @NonNull String decodeString(byte @NonNull [] buf, int offset, int length) {
    return decodeString(copySlice(buf, offset, length));
  }

  /**
   * Decode bytes from a shared buffer. Always copies — the caller owns the returned array. Drivers
   * need not override this; the default is correct.
   */
  default byte @NonNull [] decodeBytes(byte @NonNull [] buf, int offset, int length) {
    byte[] result = new byte[length];
    System.arraycopy(buf, offset, result, 0, length);
    return result;
  }

  default @NonNull BigDecimal decodeNumeric(byte @NonNull [] buf, int offset, int length) {
    return decodeNumeric(copySlice(buf, offset, length));
  }

  default @NonNull String decodeJson(byte @NonNull [] buf, int offset, int length, int typeOID) {
    return decodeJson(copySlice(buf, offset, length), typeOID);
  }

  default @NonNull LocalDate decodeDate(byte @NonNull [] buf, int offset, int length) {
    return decodeDate(copySlice(buf, offset, length));
  }

  default @NonNull LocalTime decodeTime(byte @NonNull [] buf, int offset, int length) {
    return decodeTime(copySlice(buf, offset, length));
  }

  default @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] buf, int offset, int length) {
    return decodeTimestamp(copySlice(buf, offset, length));
  }

  default @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] buf, int offset, int length) {
    return decodeTimestamptz(copySlice(buf, offset, length));
  }

  default @NonNull OffsetTime decodeTimetz(byte @NonNull [] buf, int offset, int length) {
    return decodeTimetz(copySlice(buf, offset, length));
  }

  default @NonNull String decodeToString(
      byte @NonNull [] buf, int offset, int length, int typeOID) {
    return decodeToString(copySlice(buf, offset, length), typeOID);
  }

  @SuppressWarnings("unchecked") // delegates to decode(byte[], Class) which handles the cast
  default <T> @NonNull T decode(
      byte @NonNull [] buf, int offset, int length, @NonNull Class<T> type) {
    return decode(copySlice(buf, offset, length), type);
  }

  /** Zero-copy array decode using offset-based element decoder. */
  default <T> @Nullable List<T> decodeArray(
      byte @NonNull [] value, @NonNull ElementDecoder<T> elementDecoder) {
    return null;
  }

  private static byte[] copySlice(byte[] buf, int offset, int length) {
    byte[] slice = new byte[length];
    System.arraycopy(buf, offset, slice, 0, length);
    return slice;
  }
}
