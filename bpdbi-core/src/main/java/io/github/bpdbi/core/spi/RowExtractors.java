package io.github.bpdbi.core.spi;

import io.github.bpdbi.core.Row;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Shared type-to-extractor registry used by row mappers (record mapper, JavaBean mapper, etc.) to
 * pull typed values from a {@link Row} by column index.
 *
 * <p>Each entry maps a Java type to a {@code BiFunction<Row, Integer, Object>} that calls the
 * appropriate typed getter on {@link Row}. Primitive types use null-safe defaults (0, false).
 *
 * <p>This class lives in bpdbi-core so that all mapper modules can share it. If the mapper modules
 * are ever split into a separate repository, this class should move to a shared {@code
 * bpdbi-mapper-common} module.
 */
public final class RowExtractors {

  private RowExtractors() {}

  /** Extractors for all built-in scalar types. Primitive types map to their null-safe defaults. */
  public static final Map<Class<?>, BiFunction<Row, Integer, Object>> EXTRACTORS =
      Map.ofEntries(
          Map.entry(String.class, Row::getString),
          Map.entry(
              int.class,
              (r, c) -> {
                Integer v = r.getInteger(c);
                return v == null ? 0 : v;
              }),
          Map.entry(Integer.class, Row::getInteger),
          Map.entry(
              long.class,
              (r, c) -> {
                Long v = r.getLong(c);
                return v == null ? 0L : v;
              }),
          Map.entry(Long.class, Row::getLong),
          Map.entry(
              short.class,
              (r, c) -> {
                Short v = r.getShort(c);
                return v == null ? (short) 0 : v;
              }),
          Map.entry(Short.class, Row::getShort),
          Map.entry(
              float.class,
              (r, c) -> {
                Float v = r.getFloat(c);
                return v == null ? 0f : v;
              }),
          Map.entry(Float.class, Row::getFloat),
          Map.entry(
              double.class,
              (r, c) -> {
                Double v = r.getDouble(c);
                return v == null ? 0d : v;
              }),
          Map.entry(Double.class, Row::getDouble),
          Map.entry(
              boolean.class,
              (r, c) -> {
                Boolean v = r.getBoolean(c);
                return v != null && v;
              }),
          Map.entry(Boolean.class, Row::getBoolean),
          Map.entry(BigDecimal.class, Row::getBigDecimal),
          Map.entry(UUID.class, Row::getUUID),
          Map.entry(byte[].class, Row::getBytes),
          Map.entry(LocalDate.class, Row::getLocalDate),
          Map.entry(LocalTime.class, Row::getLocalTime),
          Map.entry(LocalDateTime.class, Row::getLocalDateTime),
          Map.entry(OffsetDateTime.class, Row::getOffsetDateTime),
          Map.entry(OffsetTime.class, Row::getOffsetTime),
          Map.entry(Instant.class, Row::getInstant));

  /**
   * Resolve an extractor for the given type, including enum support. Returns {@code null} if the
   * type is not a supported scalar type.
   */
  public static @Nullable BiFunction<Row, Integer, Object> extractorFor(@NonNull Class<?> type) {
    BiFunction<Row, Integer, Object> extractor = EXTRACTORS.get(type);
    if (extractor != null) {
      return extractor;
    }
    if (type.isEnum()) {
      return (r, c) -> {
        String v = r.getString(c);
        return v == null ? null : enumValueOf(type, v);
      };
    }
    return null;
  }

  /**
   * Type-safe enum lookup. Raw {@code Enum} type is unavoidable here because Java's type system
   * cannot express "an unknown concrete enum class" — there is no way to call {@code
   * Enum.valueOf()} without a raw type when the enum class is only known at runtime.
   */
  @SuppressWarnings({"unchecked", "rawtypes"}) // safe: callers guard with type.isEnum()
  private static Object enumValueOf(Class<?> enumType, String name) {
    return Enum.valueOf((Class<? extends Enum>) enumType, name);
  }

  /** The supported-type list for error messages. */
  public static final String SUPPORTED_TYPES_MESSAGE =
      "String, int, long, short, float, double, boolean (and boxed), "
          + "BigDecimal, UUID, byte[], LocalDate, LocalTime, LocalDateTime, OffsetDateTime, "
          + "OffsetTime, Instant, enums";
}
