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
    int typeModifier
) {}
