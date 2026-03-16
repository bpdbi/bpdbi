package io.djb;


/**
 * A cursor for progressively reading rows from a large result set. Must be used within a
 * transaction.
 *
 * <pre>{@code
 * conn.query("BEGIN");
 * try (var cursor = conn.cursor("SELECT * FROM big_table")) {
 *     while (cursor.hasMore()) {
 *         RowSet batch = cursor.read(100);
 *         processBatch(batch);
 *     }
 * }
 * conn.query("COMMIT");
 * }</pre>
 */
public interface Cursor extends AutoCloseable {

  /**
   * Fetch the next batch of rows.
   */

  RowSet read(int count);

  /**
   * Whether there are more rows to read.
   */
  boolean hasMore();

  /**
   * Close the cursor and release server resources.
   */
  @Override
  void close();
}
