package io.github.bpdbi.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Registry for custom type mappings between domain types and standard types that the binary codec
 * understands. Replaces the separate {@code BinderRegistry} and {@code ColumnMapperRegistry} with a
 * single, binary-first extension point.
 *
 * <p>Each registration maps a domain type {@code T} to a standard type {@code S} (one that {@link
 * BinaryCodec} can encode/decode natively — e.g. {@code Integer}, {@code UUID}, {@code BigDecimal},
 * {@code String}). The encoder converts {@code T → S} for param binding; the decoder converts
 * {@code S → T} for result decoding. Either may be null to indicate that direction is not supported
 * — using a null direction at runtime throws a descriptive error.
 *
 * <pre>{@code
 * // Full codec — both directions
 * conn.typeRegistry().register(
 *     Money.class,
 *     BigDecimal.class,
 *     m -> m.amount(),
 *     bd -> new Money(bd)
 * );
 *
 * // Encode-only — can bind as param, but can't read from result row
 * conn.typeRegistry().register(WriteAuditEvent.class, String.class, e -> e.toJson(), null);
 * }</pre>
 *
 * @see Connection#typeRegistry()
 */
public final class TypeRegistry {

  private final Map<Class<?>, Entry<?>> entries = new LinkedHashMap<>();
  private final Set<Class<?>> jsonTypes = new LinkedHashSet<>();
  private boolean hasEncodersCached = false;

  /**
   * Register a custom type mapping. The encoder converts domain → standard type (for params), the
   * decoder converts standard → domain type (for results). Pass null for either function to
   * indicate that direction is not supported — using the null direction at runtime throws a
   * descriptive {@link IllegalStateException}.
   *
   * <p>Lookup supports supertype matching: registering {@code Animal.class} also matches {@code
   * Dog.class} if {@code Dog extends Animal}. Exact matches take priority over supertypes.
   *
   * @param domainType the custom type (e.g., {@code Money.class})
   * @param standardType the binary-codec-supported type it maps to (e.g., {@code BigDecimal.class})
   * @param paramTypeEncoder domain → standard (for param binding), or null if not supported
   * @param resultTypeDecoder standard → domain (for result decoding), or null if not supported
   * @param <T> the domain type
   * @param <S> the standard type
   * @return this registry for chaining
   */
  public <T, S> @NonNull TypeRegistry register(
      @NonNull Class<T> domainType,
      @NonNull Class<S> standardType,
      @Nullable Function<T, S> paramTypeEncoder,
      @Nullable Function<S, T> resultTypeDecoder) {
    entries.put(
        domainType, new Entry<>(domainType, standardType, paramTypeEncoder, resultTypeDecoder));
    if (paramTypeEncoder != null) {
      hasEncodersCached = true;
    }
    return this;
  }

  /**
   * Register a type as JSON. When binding parameters, values of this type will be serialized via
   * the connection's {@link JsonMapper}. When reading results, columns mapped to this type will be
   * deserialized as JSON even if the column is not a JSON/JSONB type in the database.
   */
  public @NonNull TypeRegistry registerAsJson(@NonNull Class<?> type) {
    jsonTypes.add(type);
    return this;
  }

  /** The set of types registered for JSON serialization. */
  public @NonNull Set<Class<?>> jsonTypes() {
    return Collections.unmodifiableSet(jsonTypes);
  }

  /** Check if a type is registered for JSON serialization. */
  public boolean isJsonType(@NonNull Class<?> type) {
    return jsonTypes.contains(type);
  }

  /**
   * Apply a registered encoder to convert the value to a standard type for binary encoding. Returns
   * the original value unchanged if no encoder is registered or the value is null.
   */
  @SuppressWarnings("unchecked") // safe: entries are registered with matching Class<T> keys
  public @Nullable Object encode(@Nullable Object value) {
    if (value == null || entries.isEmpty()) {
      return value;
    }
    Entry<Object> entry = (Entry<Object>) findEntry(value.getClass());
    if (entry == null || entry.paramTypeEncoder == null) {
      return value;
    }
    return entry.paramTypeEncoder.apply(value);
  }

  /** Returns true if any type encoders have been registered. */
  public boolean hasEncoders() {
    return hasEncodersCached;
  }

  /**
   * Decode a value from the standard type to the domain type. The binary codec decodes wire bytes
   * to the standard type first, then this method applies the registered decoder.
   *
   * @param type the target domain type
   * @param binaryCodec the binary codec for decoding wire bytes
   * @param buf the raw byte buffer
   * @param off offset into the buffer
   * @param len length of the value
   * @return the decoded domain-type value
   * @throws IllegalStateException if no entry is registered for the type, or if the entry was
   *     registered with a null decoder (encode-only)
   */
  @SuppressWarnings("unchecked") // safe: entries keyed by Class<T>, decoder returns T
  public <T> @NonNull T decode(
      @NonNull Class<T> type,
      @NonNull BinaryCodec binaryCodec,
      byte @NonNull [] buf,
      int off,
      int len) {
    Entry<?> entry = findEntry(type);
    if (entry == null) {
      throw new IllegalStateException(
          "No TypeRegistry entry for "
              + type.getName()
              + ". Register one via typeRegistry().register("
              + type.getSimpleName()
              + ".class, ...).");
    }
    if (entry.resultTypeDecoder == null) {
      throw new IllegalStateException(
          "No result decoder for "
              + type.getName()
              + ". It is registered with encode-only support ("
              + type.getSimpleName()
              + " → "
              + entry.standardType.getSimpleName()
              + "). Add a decoder via typeRegistry().register("
              + type.getSimpleName()
              + ".class, "
              + entry.standardType.getSimpleName()
              + ".class, encoder, decoder).");
    }
    Object standardValue = binaryCodec.decode(buf, off, len, entry.standardType);
    return (T) ((Function<Object, ?>) entry.resultTypeDecoder).apply(standardValue);
  }

  /**
   * Return {@code true} if a decoder is registered for the given type (or a supertype). Use this to
   * check before calling {@link #decode} to avoid {@link IllegalStateException}.
   */
  public boolean canDecode(@NonNull Class<?> type) {
    Entry<?> entry = findEntry(type);
    return entry != null && entry.resultTypeDecoder != null;
  }

  /**
   * Find an entry by exact type match first, then by supertype scan. Supertype matches are promoted
   * to exact matches, so later lookups for the same type are O(1).
   */
  private @Nullable Entry<?> findEntry(@NonNull Class<?> type) {
    Entry<?> exact = entries.get(type);
    if (exact != null) {
      return exact;
    }
    for (var entry : entries.entrySet()) {
      if (entry.getKey().isAssignableFrom(type)) {
        Entry<?> found = entry.getValue();
        entries.put(type, found); // promote to exact match for future lookups
        return found;
      }
    }
    return null;
  }

  /** Internal storage for a single type mapping. */
  private record Entry<T>(
      Class<T> domainType,
      Class<?> standardType,
      @Nullable Function<T, ?> paramTypeEncoder,
      @Nullable Function<?, T> resultTypeDecoder) {}
}
