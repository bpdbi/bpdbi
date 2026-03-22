package io.github.bpdbi.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Registry of {@link ColumnMapper}s for converting database text values to Java types.
 *
 * <p>Used by {@link Row#get(int, Class)} and {@link Row#get(String, Class)} to deserialize column
 * values into typed Java objects. The default registry (from {@link #defaults()}) handles common
 * types: {@code String}, primitive wrappers, {@code BigDecimal}, {@code UUID}, {@code LocalDate},
 * {@code LocalDateTime}, {@code OffsetDateTime}.
 *
 * <p>Register custom mappers for domain types:
 *
 * <pre>{@code
 * conn.mapperRegistry().register(Money.class, (value, col) -> new Money(new BigDecimal(value)));
 * Money price = row.get("price", Money.class);
 * }</pre>
 *
 * @see ColumnMapper
 * @see BinderRegistry
 */
public final class ColumnMapperRegistry {

  private final Map<Class<?>, ColumnMapper<?>> mappers = new LinkedHashMap<>();

  public <T> @NonNull ColumnMapperRegistry register(
      @NonNull Class<T> type, @NonNull ColumnMapper<T> mapper) {
    mappers.put(type, mapper);
    return this;
  }

  @SuppressWarnings("unchecked") // safe: mappers are registered with matching Class<T> keys
  @NonNull
  public <T> T map(@NonNull Class<T> type, @NonNull String value, @NonNull String columnName) {
    ColumnMapper<T> mapper = (ColumnMapper<T>) findMapper(type);
    if (mapper != null) {
      return mapper.map(value, columnName);
    }
    // Enum fallback: match by constant name
    if (type.isEnum()) {
      @SuppressWarnings({"unchecked", "rawtypes"}) // safe: type.isEnum() guarantees Enum subclass
      T result = (T) Enum.valueOf((Class<? extends Enum>) type, value);
      return result;
    }
    throw new IllegalArgumentException(
        "No ColumnMapper registered for type "
            + type.getName()
            + " (column: "
            + columnName
            + "). Register one via ColumnMapperRegistry.register().");
  }

  public boolean hasMapper(@NonNull Class<?> type) {
    return findMapper(type) != null || type.isEnum();
  }

  /** Find a mapper for the given type, checking exact match first then supertypes. */
  private @Nullable ColumnMapper<?> findMapper(@NonNull Class<?> type) {
    ColumnMapper<?> mapper = mappers.get(type);
    if (mapper != null) {
      return mapper;
    }
    for (var entry : mappers.entrySet()) {
      if (entry.getKey().isAssignableFrom(type)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /** Create a registry with built-in mappers for common types. */
  public static @NonNull ColumnMapperRegistry defaults() {
    var reg = new ColumnMapperRegistry();
    reg.register(String.class, (v, c) -> v);
    reg.register(Integer.class, (v, c) -> Integer.parseInt(v));
    reg.register(Long.class, (v, c) -> Long.parseLong(v));
    reg.register(Short.class, (v, c) -> Short.parseShort(v));
    reg.register(Float.class, (v, c) -> Float.parseFloat(v));
    reg.register(Double.class, (v, c) -> Double.parseDouble(v));
    reg.register(
        Boolean.class, (v, c) -> "t".equals(v) || "true".equalsIgnoreCase(v) || "1".equals(v));
    reg.register(BigDecimal.class, (v, c) -> new BigDecimal(v));
    reg.register(UUID.class, (v, c) -> UUID.fromString(v));
    reg.register(LocalDate.class, (v, c) -> LocalDate.parse(v));
    reg.register(LocalTime.class, (v, c) -> LocalTime.parse(v));
    reg.register(LocalDateTime.class, (v, c) -> LocalDateTime.parse(v.replace(' ', 'T')));
    reg.register(OffsetDateTime.class, (v, c) -> OffsetDateTime.parse(v.replace(' ', 'T')));
    reg.register(
        Instant.class,
        (v, c) -> {
          String s = v.replace(' ', 'T');
          try {
            return OffsetDateTime.parse(s).toInstant();
          } catch (DateTimeParseException e) {
            return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
          }
        });
    reg.register(OffsetTime.class, (v, c) -> OffsetTime.parse(v));
    return reg;
  }
}
