package io.djb;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Shared type-to-extractor registry used by row mappers (record mapper, JavaBean mapper, etc.)
 * to pull typed values from a {@link Row} by column index.
 *
 * <p>Each entry maps a Java type to a {@code BiFunction<Row, Integer, Object>} that calls the
 * appropriate typed getter on {@link Row}. Primitive types use null-safe defaults (0, false).
 *
 * <p>This class lives in djb-core so that all mapper modules can share it. If the mapper modules
 * are ever split into a separate repository, this class should move to a shared
 * {@code djb-mapper-common} module.
 */
public final class RowExtractors {

  private RowExtractors() {
  }

  /**
   * Extractors for all built-in scalar types. Primitive types map to their null-safe defaults.
   */
  public static final Map<Class<?>, BiFunction<Row, Integer, Object>> EXTRACTORS = Map.ofEntries(
      Map.entry(String.class, Row::getString),
      Map.entry(
          int.class, (r, c) -> {
            Integer v = r.getInteger(c);
            return v == null ? 0 : v;
          }
      ),
      Map.entry(Integer.class, Row::getInteger),
      Map.entry(
          long.class, (r, c) -> {
            Long v = r.getLong(c);
            return v == null ? 0L : v;
          }
      ),
      Map.entry(Long.class, Row::getLong),
      Map.entry(
          short.class, (r, c) -> {
            Short v = r.getShort(c);
            return v == null ? (short) 0 : v;
          }
      ),
      Map.entry(Short.class, Row::getShort),
      Map.entry(
          float.class, (r, c) -> {
            Float v = r.getFloat(c);
            return v == null ? 0f : v;
          }
      ),
      Map.entry(Float.class, Row::getFloat),
      Map.entry(
          double.class, (r, c) -> {
            Double v = r.getDouble(c);
            return v == null ? 0d : v;
          }
      ),
      Map.entry(Double.class, Row::getDouble),
      Map.entry(
          boolean.class, (r, c) -> {
            Boolean v = r.getBoolean(c);
            return v != null && v;
          }
      ),
      Map.entry(Boolean.class, Row::getBoolean),
      Map.entry(BigDecimal.class, Row::getBigDecimal),
      Map.entry(UUID.class, Row::getUUID),
      Map.entry(byte[].class, Row::getBytes),
      Map.entry(LocalDate.class, Row::getLocalDate),
      Map.entry(LocalTime.class, Row::getLocalTime),
      Map.entry(LocalDateTime.class, Row::getLocalDateTime),
      Map.entry(OffsetDateTime.class, Row::getOffsetDateTime)
  );

  /**
   * Resolve an extractor for the given type, including enum support. Returns {@code null} if the
   * type is not a supported scalar type.
   */
  @SuppressWarnings({"unchecked", "rawtypes"}) // safe: type.isEnum() guarantees Enum subclass
  public static BiFunction<Row, Integer, Object> extractorFor(Class<?> type) {
    BiFunction<Row, Integer, Object> extractor = EXTRACTORS.get(type);
    if (extractor != null) {
      return extractor;
    }
    if (type.isEnum()) {
      Class<? extends Enum> enumType = (Class<? extends Enum>) type;
      return (r, c) -> {
        String v = r.getString(c);
        if (v == null) {
          return null;
        }
        @SuppressWarnings("unchecked") // safe: enumType is a concrete enum class
        Object result = Enum.valueOf(enumType, v);
        return result;
      };
    }
    return null;
  }

  /**
   * The supported-type list for error messages.
   */
  public static final String SUPPORTED_TYPES_MESSAGE =
      "String, int, long, short, float, double, boolean (and boxed), "
          + "BigDecimal, UUID, byte[], LocalDate, LocalTime, LocalDateTime, OffsetDateTime, enums";
}
