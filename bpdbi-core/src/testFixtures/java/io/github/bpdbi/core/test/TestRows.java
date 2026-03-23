package io.github.bpdbi.core.test;

import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.ColumnMapperRegistry;
import io.github.bpdbi.core.Row;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.Nullable;

/**
 * Shared test utilities for building {@link Row} instances from column names and string values.
 * Used by mapper tests across modules to avoid duplicating this helper.
 */
public final class TestRows {

  private TestRows() {}

  /** Create a minimal ColumnDescriptor with just a name and no type info. */
  public static ColumnDescriptor col(String name) {
    return new ColumnDescriptor(name, 0, (short) 0, 0, (short) 0, 0);
  }

  /** Build a Row from column names and string values. Null values produce SQL NULL. */
  public static Row row(String[] columnNames, String[] values) {
    return row(columnNames, values, null);
  }

  /** Build a Row with optional ColumnMapperRegistry. Null values produce SQL NULL. */
  public static Row row(
      String[] columnNames, String[] values, @Nullable ColumnMapperRegistry registry) {
    ColumnDescriptor[] cols = new ColumnDescriptor[columnNames.length];
    byte[][] rawValues = new byte[values.length][];
    for (int i = 0; i < columnNames.length; i++) {
      cols[i] = col(columnNames[i]);
      rawValues[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
    }
    return new Row(cols, rawValues, StubBinaryCodec.INSTANCE, registry);
  }
}
