package io.djb;

import org.jspecify.annotations.Nullable;

/**
 * Read-only access to raw column bytes for a single column across all rows in a buffered result
 * set. This is the public abstraction over the internal column-oriented storage; {@link Row}
 * references this interface instead of the implementation class.
 *
 * @see Row
 */
public interface ColumnData {

  /**
   * Get the raw bytes for the given row, or null if SQL NULL. Returns a copy — the caller owns the
   * returned array.
   */
  byte @Nullable [] get(int rowIndex);

  /** Check if the value at the given row is SQL NULL. */
  boolean isNull(int rowIndex);

  /**
   * Get the backing byte array containing the value for the given row. The value bytes start at
   * {@link #offset(int)} and have length {@link #length(int)}. The returned array is shared —
   * callers must not modify it.
   *
   * @return the backing buffer, or null if SQL NULL
   */
  byte @Nullable [] buffer(int rowIndex);

  /** Get the offset into {@link #buffer(int)} where the value bytes start. */
  int offset(int rowIndex);

  /** Get the byte length of the value at the given row, or -1 if SQL NULL. */
  int length(int rowIndex);
}
