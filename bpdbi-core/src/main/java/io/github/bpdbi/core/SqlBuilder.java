package io.github.bpdbi.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Fluent builder for constructing and executing SQL statements with named parameters. Created via
 * {@link Connection#sql(String)}.
 *
 * <p>Parameters are bound by name using {@link #bind(String, Object)} and the statement is executed
 * via one of the terminal methods ({@link #query()}, {@link #enqueue()}, {@link #stream()}, etc.).
 *
 * <pre>{@code
 * List<User> users = conn.sql("SELECT * FROM users WHERE name = :name AND age > :age")
 *     .bind("name", "Alice")
 *     .bind("age", 30)
 *     .mapTo(userMapper);
 *
 * // Pipelining
 * int idx = conn.sql("INSERT INTO log (msg) VALUES (:msg)")
 *     .bind("msg", "hello")
 *     .enqueue();
 * List<RowSet> results = conn.flush();
 * }</pre>
 *
 * @see Connection#sql(String)
 */
public final class SqlBuilder {

  private final Connection conn;
  private final String sql;
  private @Nullable Map<String, Object> namedParams;

  SqlBuilder(@NonNull Connection conn, @NonNull String sql) {
    this.conn = conn;
    this.sql = sql;
  }

  // --- Parameter binding ---

  /**
   * Bind a named parameter. The parameter name corresponds to a {@code :name} placeholder in the
   * SQL string. Null values become SQL NULL.
   *
   * @param name the parameter name (without the leading colon)
   * @param value the parameter value, or null for SQL NULL
   * @return this builder for chaining
   */
  public @NonNull SqlBuilder bind(@NonNull String name, @Nullable Object value) {
    if (namedParams == null) {
      namedParams = new LinkedHashMap<>();
    }
    namedParams.put(name, value);
    return this;
  }

  // --- Terminal: query ---

  /**
   * Execute the statement and return the result set. If parameters were bound, they are passed as
   * named parameters; otherwise the statement is executed without parameters.
   *
   * @return the result set
   * @throws DbException if the statement fails
   */
  public @NonNull RowSet query() {
    if (namedParams != null) {
      return conn.query(sql, namedParams);
    }
    return conn.query(sql);
  }

  /**
   * Execute the statement and map all result rows using the given mapper.
   *
   * @param mapper the row mapper
   * @param <T> the mapped type
   * @return a list of mapped objects
   */
  public <T> @NonNull List<T> mapTo(@NonNull RowMapper<T> mapper) {
    return query().mapTo(mapper);
  }

  /**
   * Execute the statement and map the first result row. Throws if the result is empty.
   *
   * @param mapper the row mapper
   * @param <T> the mapped type
   * @return the mapped first row
   * @throws IllegalStateException if no rows in result
   */
  public <T> @NonNull T mapFirst(@NonNull RowMapper<T> mapper) {
    return query().mapFirst(mapper);
  }

  /**
   * Execute the statement and map the first result row, or return null if empty.
   *
   * @param mapper the row mapper
   * @param <T> the mapped type
   * @return the mapped first row, or null if no rows
   */
  public <T> @Nullable T mapFirstOrNull(@NonNull RowMapper<T> mapper) {
    return query().mapFirstOrNull(mapper);
  }

  // --- Terminal: enqueue (pipeline) ---

  /**
   * Enqueue the statement for pipelined execution. The statement is not sent until {@link
   * Connection#flush()} or a subsequent {@link Connection#query(String)} is called.
   *
   * @return the index into the result list that {@link Connection#flush()} will return
   */
  public int enqueue() {
    if (namedParams != null) {
      return conn.enqueue(sql, namedParams);
    }
    return conn.enqueue(sql);
  }

  // --- Terminal: streaming ---

  /**
   * Execute the statement and stream each result row to the given consumer. Rows are not
   * materialized in memory. The consumer must not retain references to the {@link Row} objects.
   *
   * @param consumer called once per result row
   */
  public void queryStream(@NonNull Consumer<Row> consumer) {
    conn.queryStream(sql, consumer);
  }

  /**
   * Execute the statement and return a closeable, iterable stream of rows. Must be closed after use
   * (use try-with-resources).
   *
   * @return a closeable row stream
   */
  public @NonNull RowStream stream() {
    return conn.stream(sql);
  }
}
