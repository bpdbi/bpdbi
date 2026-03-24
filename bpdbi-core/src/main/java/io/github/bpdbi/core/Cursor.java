package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * A server-side cursor for progressively reading rows from a large result set in explicit batches.
 * Must be used within a transaction (Postgres requirement for cursors).
 *
 * <p><b>When to use Cursor vs {@link RowStream}:</b> Use a Cursor when you need explicit batch
 * control (e.g., processing N rows at a time with batch commits). Use {@link RowStream} (via {@link
 * Connection#stream}) when you just need constant-memory iteration without a transaction.
 *
 * <pre>{@code
 * try (var tx = conn.begin()) {
 *     try (var cursor = tx.cursor("SELECT * FROM big_table")) {
 *         while (cursor.hasMore()) {
 *             RowSet batch = cursor.read(100);
 *             processBatch(batch);
 *         }
 *     }
 *     tx.commit();
 * }
 * }</pre>
 */
public interface Cursor extends AutoCloseable {

  /**
   * Fetch the next batch of rows. The returned RowSet is only valid until the next call to {@code
   * read()} — subsequent reads may reuse internal buffers, invalidating previous rows.
   */
  @NonNull RowSet read(int count);

  /** Whether there are more rows to read. */
  boolean hasMore();

  /** Close the cursor and release server resources. */
  @Override
  void close();
}
