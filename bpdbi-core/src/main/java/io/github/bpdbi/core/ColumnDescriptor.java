package io.github.bpdbi.core;

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

  // Postgres OIDs for text-like types where binary wire format is identical to UTF-8 text
  private static final int OID_TEXT = 25;
  private static final int OID_VARCHAR = 1043;
  private static final int OID_BPCHAR = 1042;
  private static final int OID_CHAR = 18;
  private static final int OID_NAME = 19;
  private static final int OID_XML = 142;

  /**
   * True if this column's binary wire format is raw UTF-8 text, making binary-to-string decoding a
   * plain {@code new String(buf, off, len, UTF_8)} without any type-specific conversion.
   */
  public boolean isTextLikeType() {
    return switch (typeOID) {
      case OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_CHAR, OID_NAME, OID_XML -> true;
      default -> false;
    };
  }
}
