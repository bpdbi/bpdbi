package io.djb;

/**
 * Describes a column in a query result.
 */
public record ColumnDescriptor(
    String name,
    int tableOID,
    short columnAttributeNumber,
    int typeOID,
    short typeSize,
    int typeModifier,
    int format          // 0=text, 1=binary
) {
    /** Backward-compatible constructor (defaults to text format). */
    public ColumnDescriptor(String name, int tableOID, short columnAttributeNumber,
                            int typeOID, short typeSize, int typeModifier) {
        this(name, tableOID, columnAttributeNumber, typeOID, typeSize, typeModifier, 0);
    }

    public static final int FORMAT_TEXT = 0;
    public static final int FORMAT_BINARY = 1;

    // Known JSON type OIDs
    public static final int OID_PG_JSON = 114;
    public static final int OID_PG_JSONB = 3802;
    public static final int OID_MYSQL_JSON = 245;

    public boolean isJsonType() {
        return typeOID == OID_PG_JSON || typeOID == OID_PG_JSONB || typeOID == OID_MYSQL_JSON;
    }
}
