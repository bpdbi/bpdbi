package io.github.bpdbi.core.test;

import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.TypeRegistry;
import java.nio.charset.StandardCharsets;

/**
 * Shared test utilities for building {@link Row} instances from column names and string values.
 * Used by mapper tests across modules to avoid duplicating this helper.
 */
public final class TestRows {

  private TestRows() {}

  /** OID for the Postgres TEXT type — used as default for test columns. */
  private static final int OID_TEXT = 25;

  /** Create a minimal ColumnDescriptor with a name and TEXT type OID. */
  public static ColumnDescriptor col(String name) {
    return new ColumnDescriptor(name, 0, (short) 0, OID_TEXT, (short) 0, 0);
  }

  /** Build a Row from column names and string values. Null values produce SQL NULL. */
  public static Row row(String[] columnNames, String[] values) {
    return row(columnNames, values, new TypeRegistry());
  }

  /** Build a Row with a TypeRegistry. Null values produce SQL NULL. */
  public static Row row(String[] columnNames, String[] values, TypeRegistry typeRegistry) {
    ColumnDescriptor[] cols = new ColumnDescriptor[columnNames.length];
    byte[][] rawValues = new byte[values.length][];
    for (int i = 0; i < columnNames.length; i++) {
      cols[i] = col(columnNames[i]);
      rawValues[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
    }
    return new Row(cols, rawValues, StubBinaryCodec.INSTANCE, typeRegistry);
  }
}
