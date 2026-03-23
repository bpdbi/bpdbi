package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * Maps a single column value (as text) to a typed Java object. Register with {@link
 * ColumnMapperRegistry} for use with {@link Row#get(int, Class)}.
 *
 * <pre>{@code
 * registry.register(Money.class, (value, column) -> new Money(new BigDecimal(value)));
 * Money price = row.get("price", Money.class);
 * }</pre>
 *
 * <p><b>How column mappers fit into the decoding pipeline</b>
 *
 * <p>When {@link Row#get(int, Class)} is called, it first tries the {@link BinaryCodec} for types
 * it knows natively (primitives, dates, UUID, etc.). Column mappers are the <em>fallback</em>: they
 * receive a text representation of the value and convert it to the requested type.
 *
 * <p>The text representation is produced by {@link BinaryCodec#decodeToString(byte[], int, int,
 * int)}, which uses the column's type OID to correctly decode binary bytes into text (e.g., a
 * binary date becomes {@code "2026-03-18"}). Since the binary codec already handles all standard
 * types directly, column mappers are primarily useful for:
 *
 * <ul>
 *   <li>User-defined wrapper types — e.g., a {@code Money} wrapper around {@code BigDecimal}
 *   <li>Non-Java types — e.g., {@code kotlinx.datetime.LocalDate} mapped from a {@code date} column
 *   <li>Enum mapping — converting a text column to a Java enum
 * </ul>
 */
@FunctionalInterface
public interface ColumnMapper<T> {

  /**
   * Map a text column value to a typed Java object.
   *
   * @param value the raw text value from the database (never null — null is handled before this is
   *     called)
   * @param columnName the column name (to create descriptive error messages)
   */
  @NonNull T map(@NonNull String value, @NonNull String columnName);
}
