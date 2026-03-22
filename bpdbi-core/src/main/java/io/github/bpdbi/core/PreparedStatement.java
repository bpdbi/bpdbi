package io.github.bpdbi.core;

import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A prepared statement that can be executed multiple times with different parameters. More
 * efficient than repeated query() calls for the same SQL — the database parses and plans the query
 * once.
 *
 * <pre>{@code
 * try (var stmt = conn.prepare("SELECT * FROM users WHERE id = $1")) {
 *     var alice = stmt.query(1);
 *     var bob = stmt.query(2);
 * }
 * }</pre>
 *
 * <p>Named parameters are supported when the SQL uses {@code :name} syntax:
 *
 * <pre>{@code
 * try (var stmt = conn.prepare("SELECT * FROM users WHERE name = :name AND age = :age")) {
 *     var rs = stmt.query(Map.of("name", "Alice", "age", 30));
 * }
 * }</pre>
 *
 * <p><b>Array parameters:</b> Instead of IN-list expansion (which is incompatible with prepared
 * statements because the SQL changes with collection size), you can pass a Collection or array as a
 * single parameter and use {@code = ANY(:param)}:
 *
 * <pre>{@code
 * try (var stmt = conn.prepare("SELECT * FROM users WHERE id = ANY(:ids::int[])")) {
 *     var rs = stmt.query(Map.of("ids", List.of(1, 2, 3)));
 * }
 * }</pre>
 *
 * Collections and arrays are automatically formatted as array literals ({1,2,3}).
 */
public interface PreparedStatement extends AutoCloseable {

  /** Execute the prepared statement with the given positional parameters. */
  @NonNull RowSet query(@Nullable Object... params);

  /**
   * Execute the prepared statement with named parameters. The statement must have been prepared
   * with {@code :name} syntax.
   *
   * <p>Collection and array values are automatically converted to array literals for use with
   * {@code = ANY(:param::type[])} syntax.
   *
   * @param params parameter values keyed by name
   * @throws IllegalStateException if the statement was not prepared with named parameters
   * @throws IllegalArgumentException if a required parameter is missing
   */
  @NonNull RowSet query(@NonNull Map<String, Object> params);

  /** Close the prepared statement and release server resources. */
  @Override
  void close();
}
