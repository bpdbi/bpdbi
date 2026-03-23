package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * Converts a domain type into a type that the driver's binary encoder understands, enabling
 * efficient binary wire encoding for custom types without exposing wire-format details.
 *
 * <p>The returned object should be a type the binary encoder handles natively ({@code Integer},
 * {@code Long}, {@code UUID}, {@code LocalDate}, {@code BigDecimal}, {@code byte[]}, {@code
 * List<Integer>}, etc.). If the returned type is not binary-encodable, it falls through to the text
 * {@link Binder} path.
 *
 * <pre>{@code
 * // Domain type → binary-encodable type
 * registry.registerEncoder(UserId.class, id -> id.uuid());       // UserId → UUID (binary)
 * registry.registerEncoder(Money.class, m -> m.cents());          // Money → Long (binary)
 * registry.registerEncoder(KotlinUuid.class, u -> u.toJavaUuid()); // Kotlin UUID → Java UUID
 * }</pre>
 *
 * @see BinderRegistry#registerEncoder(Class, ParamEncoder)
 */
@FunctionalInterface
public interface ParamEncoder<T> {

  /**
   * Encode a domain value into a type the binary encoder can handle.
   *
   * @param value the domain value (never null — null is handled before this is called)
   * @return a value of a binary-encodable type
   */
  @NonNull Object encode(@NonNull T value);
}
