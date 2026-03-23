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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Decodes binary-format column values from raw bytes received on the wire.
 *
 * <p>The codec is used internally by {@link Row} getters to decode binary wire data into Java
 * types.
 *
 * <p><b>Not user-extensible.</b> This interface is implemented per-driver and handles the fixed set
 * of wire-format types defined by each database protocol. To handle custom types (e.g. domain
 * types, composite types, or enums), use these extension points:
 *
 * <ul>
 *   <li>{@link ColumnMapperRegistry} — register a {@link ColumnMapper} to convert column values to
 *       Java types via {@link Row#get(int, Class)}
 *   <li>{@link BinderRegistry} — register a {@link Binder} to convert Java objects to SQL parameter
 *       strings when binding query parameters
 *   <li>{@link JsonMapper} — plug in a JSON library (Jackson, Gson, etc.) for automatic
 *       serialization of JSON/JSONB columns
 * </ul>
 */
public interface BinaryCodec {

  // --- Offset-based decode methods (primary API) ---

  boolean decodeBool(byte @NonNull [] buf, int offset);

  short decodeInt2(byte @NonNull [] buf, int offset);

  int decodeInt4(byte @NonNull [] buf, int offset);

  long decodeInt8(byte @NonNull [] buf, int offset);

  float decodeFloat4(byte @NonNull [] buf, int offset);

  double decodeFloat8(byte @NonNull [] buf, int offset);

  @NonNull String decodeString(byte @NonNull [] buf, int offset, int length);

  @NonNull UUID decodeUuid(byte @NonNull [] buf, int offset, int length);

  @NonNull LocalDate decodeDate(byte @NonNull [] buf, int offset, int length);

  @NonNull LocalTime decodeTime(byte @NonNull [] buf, int offset, int length);

  @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] buf, int offset, int length);

  @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] buf, int offset, int length);

  @NonNull OffsetTime decodeTimetz(byte @NonNull [] buf, int offset, int length);

  @NonNull BigDecimal decodeNumeric(byte @NonNull [] buf, int offset, int length);

  /**
   * Decode a JSON/JSONB column to a string. The type OID distinguishes JSON from JSONB (which may
   * have a version prefix that needs stripping).
   */
  @NonNull String decodeJson(byte @NonNull [] buf, int offset, int length, int typeOID);

  /**
   * Decode a binary column value to its text representation, using the type OID to select the
   * correct decoding strategy. Used by {@link Row#get(int, Class)} as the text fallback for column
   * mappers on binary-format rows. Drivers should override this to produce correct text for
   * non-string types (dates, numerics, etc.).
   */
  @NonNull String decodeToString(byte @NonNull [] buf, int offset, int length, int typeOID);

  /**
   * Decode bytes from a shared buffer. Always copies — the caller owns the returned array. Drivers
   * need not override this; the default is correct.
   */
  default byte @NonNull [] decodeBytes(byte @NonNull [] buf, int offset, int length) {
    byte[] result = new byte[length];
    System.arraycopy(buf, offset, result, 0, length);
    return result;
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
   * Decode binary bytes directly to the given Java type, bypassing text conversion. Delegates to
   * the offset-based methods using offset 0 and full array length.
   *
   * @throws IllegalArgumentException if the type is not supported (check {@link #canDecode} first)
   */
  @SuppressWarnings("unchecked") // safe: each branch returns the exact type requested
  @NonNull
  default <T> T decode(byte @NonNull [] value, @NonNull Class<T> type) {
    Object result;
    if (type == String.class) {
      result = decodeString(value, 0, value.length);
    } else if (type == Integer.class) {
      result = decodeInt4(value, 0);
    } else if (type == Long.class) {
      result = decodeInt8(value, 0);
    } else if (type == Short.class) {
      result = decodeInt2(value, 0);
    } else if (type == Float.class) {
      result = decodeFloat4(value, 0);
    } else if (type == Double.class) {
      result = decodeFloat8(value, 0);
    } else if (type == Boolean.class) {
      result = decodeBool(value, 0);
    } else if (type == BigDecimal.class) {
      result = decodeNumeric(value, 0, value.length);
    } else if (type == UUID.class) {
      result = decodeUuid(value, 0, value.length);
    } else if (type == LocalDate.class) {
      result = decodeDate(value, 0, value.length);
    } else if (type == LocalTime.class) {
      result = decodeTime(value, 0, value.length);
    } else if (type == LocalDateTime.class) {
      result = decodeTimestamp(value, 0, value.length);
    } else if (type == OffsetDateTime.class) {
      result = decodeTimestamptz(value, 0, value.length);
    } else if (type == OffsetTime.class) {
      result = decodeTimetz(value, 0, value.length);
    } else if (type == Instant.class) {
      result = decodeTimestamptz(value, 0, value.length).toInstant();
    } else if (type == byte[].class) {
      result = decodeBytes(value, 0, value.length);
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
