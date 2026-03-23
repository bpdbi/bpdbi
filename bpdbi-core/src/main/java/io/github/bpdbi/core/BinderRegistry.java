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
 * Registry of {@link Binder}s for converting Java objects to SQL parameter strings. This is the
 * <b>text fallback</b> path — most built-in types ({@code Integer}, {@code Long}, {@code UUID},
 * {@code LocalDate}, {@code BigDecimal}, arrays, etc.) are binary-encoded directly into the
 * Postgres wire protocol for better performance and zero intermediate String allocations. Text
 * encoding via this registry is only used for types that the binary encoder does not handle: {@code
 * String} (inherently text), custom domain types, and JSON-mapped types.
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
  private final Map<QualifiedType<?>, Binder<?>> qualifiedBinders = new LinkedHashMap<>();
  private final Map<Class<?>, ParamEncoder<?>> encoders = new LinkedHashMap<>();
  private final Set<Class<?>> jsonTypes = new LinkedHashSet<>();

  public <T> @NonNull BinderRegistry register(@NonNull Class<T> type, @NonNull Binder<T> binder) {
    binders.put(type, binder);
    return this;
  }

  /**
   * Register a parameter encoder that converts a domain type into a type the binary encoder
   * handles. This enables binary wire encoding for custom types without exposing wire-format
   * details.
   *
   * @see ParamEncoder
   */
  public <T> @NonNull BinderRegistry registerEncoder(
      @NonNull Class<T> type, @NonNull ParamEncoder<T> encoder) {
    encoders.put(type, encoder);
    return this;
  }

  /**
   * Register a binder for a qualified type. The qualified binder is consulted when {@link
   * #bind(QualifiedType, Object)} is called with a matching qualified type.
   */
  public <T> @NonNull BinderRegistry register(
      @NonNull QualifiedType<T> qualifiedType, @NonNull Binder<T> binder) {
    qualifiedBinders.put(qualifiedType, binder);
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
   * Convert a value to its SQL string representation using a qualified binder. Falls back to the
   * unqualified {@link #bind(Object)} if no qualified binder matches.
   */
  @SuppressWarnings(
      "unchecked") // safe: qualifiedBinders are registered with matching QualifiedType<T> keys
  public <T> @Nullable String bind(@NonNull QualifiedType<T> qualifiedType, @Nullable T value) {
    if (value == null) {
      return null;
    }
    Binder<T> binder = (Binder<T>) qualifiedBinders.get(qualifiedType);
    if (binder != null) {
      return binder.bind(value);
    }
    return bind(value);
  }

  /**
   * Convert a value to its SQL string representation using the registered binder. Returns null if
   * the value is null.
   *
   * <p>If no binder is registered for the value's exact type or any of its supertypes, the value is
   * converted via {@link Object#toString()} as a fallback. Register explicit binders for custom
   * types to avoid relying on this fallback.
   */
  @SuppressWarnings("unchecked") // safe: binders are registered with matching Class<T> keys
  public @Nullable String bind(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    Binder<Object> binder = (Binder<Object>) findByType(binders, value.getClass());
    if (binder != null) {
      return binder.bind(value);
    }
    // Fallback: no registered binder found, use toString()
    return value.toString();
  }

  /**
   * Apply a registered {@link ParamEncoder} to convert the value to a binary-encodable type.
   * Returns the original value unchanged if no encoder is registered or the value is null.
   */
  @SuppressWarnings("unchecked") // safe: encoders are registered with matching Class<T> keys
  public @Nullable Object encode(@Nullable Object value) {
    if (value == null || encoders.isEmpty()) {
      return value;
    }
    ParamEncoder<Object> encoder = (ParamEncoder<Object>) findByType(encoders, value.getClass());
    return encoder != null ? encoder.encode(value) : value;
  }

  /** Returns true if any param encoders have been registered. */
  public boolean hasEncoders() {
    return !encoders.isEmpty();
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

  /** Find a value by exact type match first, then by supertype scan. */
  private static <V> @Nullable V findByType(Map<Class<?>, V> map, Class<?> type) {
    V exact = map.get(type);
    if (exact != null) {
      return exact;
    }
    for (var entry : map.entrySet()) {
      if (entry.getKey().isAssignableFrom(type)) {
        return entry.getValue();
      }
    }
    return null;
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
