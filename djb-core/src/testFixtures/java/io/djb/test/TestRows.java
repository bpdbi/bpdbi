package io.djb.test;

import io.djb.ColumnDescriptor;
import io.djb.Row;
import java.nio.charset.StandardCharsets;

/**
 * Shared test utilities for building {@link Row} instances from column names and text values. Used
 * by mapper tests across modules to avoid duplicating this helper.
 */
public final class TestRows {

  private TestRows() {}

  /** Build a text-format Row from column names and string values. Null values produce SQL NULL. */
  public static Row row(String[] columnNames, String[] values) {
    ColumnDescriptor[] cols = new ColumnDescriptor[columnNames.length];
    byte[][] rawValues = new byte[values.length][];
    for (int i = 0; i < columnNames.length; i++) {
      cols[i] = new ColumnDescriptor(columnNames[i], 0, (short) 0, 0, (short) 0, 0);
      rawValues[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
    }
    return new Row(cols, rawValues, null, null);
  }
}
