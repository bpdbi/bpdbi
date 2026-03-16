package io.djb;


/**
 * Maps a single column value (as text) to a typed Java object. Register with
 * {@link ColumnMapperRegistry} for use with {@link Row#get(int, Class)}.
 *
 * <pre>{@code
 * registry.register(Money.class, (value, column) -> new Money(new BigDecimal(value)));
 * Money price = row.get("price", Money.class);
 * }</pre>
 */
@FunctionalInterface
public interface ColumnMapper<T> {

  /**
   * Map a text column value to a typed Java object.
   *
   * @param value      the raw text value from the database (never null — null is handled before
   *                   this is called)
   * @param columnName the column name (to create descriptive error messages)
   */

  T map(String value, String columnName);
}
