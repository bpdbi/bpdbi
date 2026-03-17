package io.github.bpdbi.core;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A closeable, iterable stream of rows backed by direct wire-protocol reading. Rows are fetched
 * lazily from the server — constant memory regardless of result size.
 *
 * <p>Must be closed after use to drain any remaining server messages and keep the connection in a
 * clean state. Use with try-with-resources:
 *
 * <pre>{@code
 * try (var rows = conn.stream("SELECT * FROM big_table")) {
 *     for (Row row : rows) {
 *         process(row);
 *     }
 * }
 * }</pre>
 *
 * <p>Row objects are only valid during iteration. Do not retain references to rows after advancing
 * the iterator or closing the stream.
 *
 * <p>Only one pass is supported — calling {@link #iterator()}, {@link #stream()}, or {@link
 * #forEach(Consumer)} consumes the rows. A second call will see no remaining rows.
 */
public final class RowStream implements AutoCloseable, Iterable<Row> {

  private final Supplier<Row> nextRow;
  private final Runnable onClose;
  private boolean closed;

  /**
   * @param nextRow supplies the next row, or null when exhausted
   * @param onClose called on close to drain remaining messages and clean up
   */
  public RowStream(@NonNull Supplier<@Nullable Row> nextRow, @NonNull Runnable onClose) {
    this.nextRow = nextRow;
    this.onClose = onClose;
  }

  @Override
  public void forEach(@NonNull Consumer<? super Row> action) {
    Row row;
    while ((row = nextRow.get()) != null) {
      action.accept(row);
    }
  }

  /**
   * Return a {@link Stream}{@code <Row>} for filter/map/collect composition. The returned stream
   * does not need separate closing — closing this RowStream suffices.
   */
  public @NonNull Stream<Row> stream() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED | Spliterator.NONNULL),
        false);
  }

  @Override
  @NonNull
  public Iterator<Row> iterator() {
    return new Iterator<>() {
      private Row next = advance();

      private Row advance() {
        if (closed) {
          return null;
        }
        return nextRow.get();
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Row next() {
        Row current = next;
        if (current == null) {
          throw new NoSuchElementException();
        }
        next = advance();
        return current;
      }
    };
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      onClose.run();
    }
  }
}
