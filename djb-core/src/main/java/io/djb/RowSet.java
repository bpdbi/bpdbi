package io.djb;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The result of executing a single SQL statement. Contains rows (for SELECT queries), metadata
 * (column descriptors, rows affected count), and optionally an error (for failed statements in a
 * pipelined batch).
 *
 * <p>Implements {@link Iterable}{@code <Row>} so results can be used in enhanced for-loops:
 * <pre>{@code
 * for (Row row : conn.query("SELECT * FROM users")) {
 *     System.out.println(row.getString("name"));
 * }
 * }</pre>
 *
 * <p>In pipelined execution ({@link Connection#flush()}), each RowSet may independently
 * succeed or fail. Check {@link #getError()} is not null before accessing rows, or let
 * the getters throw automatically via the internal {@code checkError()} guard.
 *
 * @see Row
 * @see Connection#query(String)
 * @see Connection#flush()
 */
public final class RowSet implements Iterable<Row> {

  @NonNull
  private final List<Row> rows;

  @NonNull
  private final List<ColumnDescriptor> columns;

  private final int rowsAffected;

  @Nullable
  private final DbException error;

  public RowSet(
      @NonNull List<Row> rows,
      @NonNull List<ColumnDescriptor> columns,
      int rowsAffected
  ) {
    this.rows = rows;
    this.columns = columns;
    this.rowsAffected = rowsAffected;
    this.error = null;
  }

  /**
   * Create a RowSet representing a failed statement in a pipeline.
   */
  public RowSet(@NonNull DbException error) {
    this.rows = Collections.emptyList();
    this.columns = Collections.emptyList();
    this.rowsAffected = 0;
    this.error = error;
  }

  private void checkError() {
    if (error != null) {
      String msg = error.getMessage();
      throw new DbException(error.severity(), error.sqlState(), msg != null ? msg : "", error);
    }
  }

  public int size() {
    checkError();
    return rows.size();
  }

  public int rowsAffected() {
    checkError();
    return rowsAffected;
  }

  public List<ColumnDescriptor> columnDescriptors() {
    checkError();
    return columns;
  }

  public Row first() {
    checkError();
    if (rows.isEmpty()) {
      throw new IllegalStateException("No rows in result");
    }
    return rows.getFirst();
  }

  public Stream<Row> stream() {
    checkError();
    return rows.stream();
  }

  @Override
  @NonNull
  public Iterator<Row> iterator() {
    checkError();
    return rows.iterator();
  }

  /**
   * Map all rows to typed objects using the given mapper.
   */
  public <T> List<T> mapTo(RowMapper<T> mapper) {
    checkError();
    return rows.stream().map(mapper::map).toList();
  }

  /**
   * Map the first row to a typed object using the given mapper.
   *
   * @throws IllegalStateException if no rows
   */
  public <T> T mapFirst(RowMapper<T> mapper) {
    return mapper.map(first());
  }

  @Nullable
  public DbException getError() {
    return error;
  }
}
