package io.djb;

import org.jspecify.annotations.NonNull;

/**
 * Converts a Java object to a SQL parameter string for binding. Register with {@link
 * BinderRegistry} for use with parameterized queries.
 *
 * <pre>{@code
 * registry.register(Money.class, m -> m.amount().toPlainString());
 * conn.query("INSERT INTO prices VALUES ($1)", new Money(new BigDecimal("9.99")));
 * }</pre>
 */
@FunctionalInterface
public interface Binder<T> {

  /**
   * Bind a Java value to its SQL parameter string representation.
   *
   * @param value the Java value (never null — null is handled before this is called)
   * @return the string representation for the SQL parameter
   */
  @NonNull String bind(@NonNull T value);
}
