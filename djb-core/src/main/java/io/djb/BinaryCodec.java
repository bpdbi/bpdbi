package io.djb;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

/**
 * Decodes binary-format column values from raw bytes received on the wire.
 *
 * <p>Each database driver provides its own implementation: Postgres uses big-endian
 * encoding, MySQL uses little-endian. The codec is selected automatically based on the connection
 * type and is used internally by {@link Row} getters to decode binary wire data.
 * Only active for parameterized queries — parameterless queries use text format
 * and bypass the binary codec entirely (see {@link Row} javadoc for why).
 *
 * <p><b>Not user-extensible.</b> This interface is implemented per-driver and handles
 * the fixed set of wire-format types defined by each database protocol. To decode custom types
 * (e.g. Postgres domains, composite types, or enums), use the text-format extension points
 * instead:
 * <ul>
 *   <li>{@link ColumnMapperRegistry} — register a {@link ColumnMapper} to convert text column
 *       values to Java types via {@link Row#get(int, Class)}
 *   <li>{@link BinderRegistry} — register a {@link Binder} to convert Java objects to
 *       SQL parameter strings when binding query parameters
 *   <li>{@link JsonMapper} — plug in a JSON library (Jackson, Gson, etc.) for automatic
 *       serialization of JSON/JSONB columns
 * </ul>
 */
public interface BinaryCodec {

  boolean decodeBool(byte[] value);

  short decodeInt2(byte[] value);

  int decodeInt4(byte[] value);

  long decodeInt8(byte[] value);

  float decodeFloat4(byte[] value);

  double decodeFloat8(byte[] value);


  String decodeString(byte[] value);


  UUID decodeUuid(byte[] value);


  LocalDate decodeDate(byte[] value);


  LocalTime decodeTime(byte[] value);


  LocalDateTime decodeTimestamp(byte[] value);


  OffsetDateTime decodeTimestamptz(byte[] value);


  OffsetTime decodeTimetz(byte[] value);

  byte[] decodeBytes(byte[] value);


  BigDecimal decodeNumeric(byte[] value);

  /**
   * Decode a JSON/JSONB column to a string. PG JSONB has a 1-byte version prefix that needs
   * stripping.
   */

  String decodeJson(byte[] value, int typeOID);

  /**
   * Whether this codec can decode binary bytes directly to the given Java type. Used by
   * {@link Row#get(int, Class)} to bypass the text roundtrip through {@link ColumnMapper} for types
   * the binary protocol handles natively.
   */
  default boolean canDecode(Class<?> type) {
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
  default <T> T decode(byte[] value, Class<T> type) {
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
}
