package io.github.bpdbi.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Registry of {@link Binder}s for converting Java objects to SQL parameter strings.
 *
 * <p>Used internally by the connection when encoding query parameters. The default registry (from
 * {@link #defaults()}) handles common types: {@code String}, {@code Integer}, {@code Long}, {@code
 * Boolean}, {@code BigDecimal}, {@code UUID}, {@code LocalDate}, {@code LocalDateTime}, {@code
 * OffsetDateTime}, {@code byte[]}, and more.
 *
 * <p>Register custom binders for domain types:
 *
 * <pre>{@code
 * conn.binderRegistry().register(Money.class, m -> m.amount().toPlainString());
 * conn.query("INSERT INTO prices VALUES ($1)", new Money(new BigDecimal("9.99")));
 * }</pre>
 *
 * <p>Types can also be registered as JSON via {@link #registerAsJson(Class)}, causing values of
 * that type to be serialized via the connection's {@link JsonMapper}.
 *
 * @see Binder
 * @see ColumnMapperRegistry
 */
public final class BinderRegistry {

  private final Map<Class<?>, Binder<?>> binders = new LinkedHashMap<>();
  private final Set<Class<?>> jsonTypes = new LinkedHashSet<>();

  public <T> @NonNull BinderRegistry register(@NonNull Class<T> type, @NonNull Binder<T> binder) {
    binders.put(type, binder);
    return this;
  }

  /**
   * Register a type as JSON. When binding parameters, values of this type will be serialized via
   * the connection's {@link JsonMapper} instead of {@link Binder}. When reading results, columns
   * mapped to this type will be deserialized as JSON even if the column is not a JSON/JSONB type in
   * the database.
   */
  public @NonNull BinderRegistry registerAsJson(@NonNull Class<?> type) {
    jsonTypes.add(type);
    return this;
  }

  public @NonNull Set<Class<?>> jsonTypes() {
    return Collections.unmodifiableSet(jsonTypes);
  }

  public boolean isJsonType(@NonNull Class<?> type) {
    return jsonTypes.contains(type);
  }

  /**
   * Convert a value to its SQL string representation using the registered binder. Returns null if
   * the value is null.
   */
  @SuppressWarnings("unchecked")
  public @Nullable String bind(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    Class<?> type = value.getClass();
    Binder<Object> binder = (Binder<Object>) binders.get(type);
    if (binder != null) {
      return binder.bind(value);
    }
    // Check supertypes
    for (var entry : binders.entrySet()) {
      if (entry.getKey().isAssignableFrom(type)) {
        return ((Binder<Object>) entry.getValue()).bind(value);
      }
    }
    // Fallback to toString
    return value.toString();
  }

  /** Create a registry with built-in binders for common types. */
  public static @NonNull BinderRegistry defaults() {
    var reg = new BinderRegistry();
    reg.register(String.class, v -> v);
    reg.register(Integer.class, Object::toString);
    reg.register(Long.class, Object::toString);
    reg.register(Short.class, Object::toString);
    reg.register(Float.class, Object::toString);
    reg.register(Double.class, Object::toString);
    reg.register(Boolean.class, Object::toString);
    reg.register(BigDecimal.class, BigDecimal::toPlainString);
    reg.register(UUID.class, Object::toString);
    reg.register(LocalDate.class, Object::toString);
    reg.register(LocalTime.class, Object::toString);
    reg.register(LocalDateTime.class, Object::toString);
    reg.register(OffsetDateTime.class, Object::toString);
    reg.register(
        Instant.class,
        i -> LocalDateTime.ofInstant(i, ZoneOffset.UTC).toString().replace('T', ' '));
    reg.register(OffsetTime.class, Object::toString);
    reg.register(byte[].class, BinderRegistry::hexEncode);
    return reg;
  }

  static @NonNull String hexEncode(byte @NonNull [] bytes) {
    var sb = new StringBuilder(2 + bytes.length * 2);
    sb.append("\\x");
    for (byte b : bytes) {
      int val = b & 0xFF;
      sb.append(Character.forDigit(val >> 4, 16));
      sb.append(Character.forDigit(val & 0xF, 16));
    }
    return sb.toString();
  }
}
