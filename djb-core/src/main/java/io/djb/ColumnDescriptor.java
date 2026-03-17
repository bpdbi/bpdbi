package io.djb;

import org.jspecify.annotations.NonNull;

/**
 * Metadata describing a column in a query result.
 *
 * <p>Column descriptors are available via {@link RowSet#columnDescriptors()} and are used
 * internally by {@link Row} to decode values.
 *
 * @param name the column name (or alias)
 * @param tableOID the OID of the source table (0 if not from a table)
 * @param columnAttributeNumber the column position within the source table
 * @param typeOID the OID of the column's data type
 * @param typeSize the data type size (negative for variable-length)
 * @param typeModifier type-specific modifier (e.g. precision for numeric)
 */
public record ColumnDescriptor(
    @NonNull String name,
    int tableOID,
    short columnAttributeNumber,
    int typeOID,
    short typeSize,
    int typeModifier) {

  // Known JSON type OIDs
  public static final int OID_PG_JSON = 114;
  public static final int OID_PG_JSONB = 3802;
  public static final int OID_MYSQL_JSON = 245;

  public boolean isJsonType() {
    return typeOID == OID_PG_JSON || typeOID == OID_PG_JSONB || typeOID == OID_MYSQL_JSON;
  }
}
