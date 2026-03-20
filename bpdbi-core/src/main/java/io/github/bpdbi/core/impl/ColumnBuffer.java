package io.github.bpdbi.core.impl;

import io.github.bpdbi.core.ColumnData;
import java.io.IOException;
import java.io.InputStream;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Column-oriented contiguous byte storage. Holds all raw values for a single column across all rows
 * in a result set, packed into one {@code byte[]} with offset/length metadata per row.
 *
 * <p>This replaces the per-row {@code byte[][]} allocation pattern. For a result set with N rows ×
 * M columns, this creates M ColumnBuffers instead of N×M individual {@code byte[]} arrays —
 * dramatically reducing GC pressure for large result sets.
 *
 * <p>Not thread-safe. Designed for single-threaded append-then-read usage.
 */
public final class ColumnBuffer implements ColumnData {

  private byte[] data;
  private int[] offsets;
  private int[] lengths; // -1 = NULL
  private int rowCount;
  private int dataPos;
  private long totalValueBytes; // tracks total non-null bytes appended for average calculation

  public ColumnBuffer(int initialRows, int estimatedAvgSize) {
    this.data = new byte[initialRows * estimatedAvgSize];
    this.offsets = new int[initialRows];
    this.lengths = new int[initialRows];
  }

  /**
   * Append a column value for the next row.
   *
   * @param value the raw bytes, or null for SQL NULL
   */
  public void append(byte @Nullable [] value) {
    if (rowCount == offsets.length) {
      grow();
    }
    if (value == null) {
      offsets[rowCount] = dataPos;
      lengths[rowCount] = -1;
    } else {
      ensureDataCapacity(value.length);
      offsets[rowCount] = dataPos;
      lengths[rowCount] = value.length;
      System.arraycopy(value, 0, data, dataPos, value.length);
      dataPos += value.length;
      totalValueBytes += value.length;
    }
    rowCount++;
  }

  /** Append a SQL NULL value for the next row. */
  public void appendNull() {
    if (rowCount == offsets.length) {
      grow();
    }
    offsets[rowCount] = dataPos;
    lengths[rowCount] = -1;
    rowCount++;
  }

  /**
   * Append a column value by reading directly from an InputStream into the backing array. Avoids
   * allocating an intermediate byte[] — the data goes straight from the stream into the buffer.
   */
  public void appendFromStream(@NonNull InputStream in, int length) throws IOException {
    if (rowCount == offsets.length) {
      grow();
    }
    ensureDataCapacity(length);
    offsets[rowCount] = dataPos;
    lengths[rowCount] = length;
    int pos = dataPos;
    int end = pos + length;
    while (pos < end) {
      int read = in.read(data, pos, end - pos);
      if (read == -1) {
        throw new IOException(
            "Unexpected end of stream (needed " + length + " bytes, got " + (pos - dataPos) + ")");
      }
      pos += read;
    }
    dataPos += length;
    totalValueBytes += length;
    rowCount++;
  }

  /**
   * Get the raw bytes for a given row, or null if the value is SQL NULL. Returns a copy — the
   * caller owns the returned array.
   */
  @Override
  public byte[] get(int rowIndex) {
    if (lengths[rowIndex] == -1) {
      return null;
    }
    int len = lengths[rowIndex];
    byte[] result = new byte[len];
    System.arraycopy(data, offsets[rowIndex], result, 0, len);
    return result;
  }

  @Override
  public boolean isNull(int rowIndex) {
    return lengths[rowIndex] == -1;
  }

  @Override
  public byte[] buffer(int rowIndex) {
    if (lengths[rowIndex] == -1) {
      return null;
    }
    return data;
  }

  @Override
  public int offset(int rowIndex) {
    return offsets[rowIndex];
  }

  @Override
  public int length(int rowIndex) {
    return lengths[rowIndex];
  }

  public int rowCount() {
    return rowCount;
  }

  /**
   * Average byte size of non-null values appended so far. Returns 0 if no non-null values have been
   * appended. Useful for sizing future ColumnBuffers based on observed data.
   */
  public int averageValueSize() {
    int nonNullCount = 0;
    for (int i = 0; i < rowCount; i++) {
      if (lengths[i] != -1) {
        nonNullCount++;
      }
    }
    return nonNullCount == 0 ? 0 : (int) (totalValueBytes / nonNullCount);
  }

  /**
   * Reset this buffer for reuse, retaining the current backing arrays. This avoids reallocating
   * buffers when reading successive cursor batches of similar size.
   */
  public void reset() {
    rowCount = 0;
    dataPos = 0;
    totalValueBytes = 0;
  }

  /**
   * Initial metadata capacity when starting from zero. Small to avoid waste for single-row results.
   */
  private static final int INITIAL_ROW_CAPACITY = 16;

  private void grow() {
    int newCap = offsets.length == 0 ? INITIAL_ROW_CAPACITY : offsets.length * 2;
    int[] newOffsets = new int[newCap];
    int[] newLengths = new int[newCap];
    System.arraycopy(offsets, 0, newOffsets, 0, rowCount);
    System.arraycopy(lengths, 0, newLengths, 0, rowCount);
    offsets = newOffsets;
    lengths = newLengths;
  }

  private void ensureDataCapacity(int needed) {
    if (dataPos + needed > data.length) {
      int newCap = Math.max(data.length * 2, dataPos + needed);
      byte[] newData = new byte[newCap];
      System.arraycopy(data, 0, newData, 0, dataPos);
      data = newData;
    }
  }
}
