package io.djb;

/**
 * Converts a Java object to a SQL parameter string for binding.
 * Register with {@link TypeRegistry} for use with parameterized queries.
 *
 * <pre>{@code
 * registry.register(Money.class, m -> m.amount().toPlainString());
 * conn.query("INSERT INTO prices VALUES ($1)", new Money(new BigDecimal("9.99")));
 * }</pre>
 */
@FunctionalInterface
public interface TypeBinder<T> {
    /**
     * @param value the Java value (never null — null is handled before this is called)
     * @return the string representation for the SQL parameter
     */
    String bind(T value);
}
