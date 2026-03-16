package io.djb;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A blocking database connection with first-class pipelining support.
 *
 * <p>The core API consists of three patterns:
 * <ul>
 *   <li><b>Immediate:</b> {@link #query(String)} executes a statement and returns the result.
 *   <li><b>Pipelined:</b> {@link #enqueue(String)} queues a statement, {@link #flush()} sends all
 *       queued statements in a single network write and reads all responses.
 *   <li><b>Prepared:</b> {@link #prepare(String)} creates a server-side prepared statement for
 *       repeated execution with different parameters.
 * </ul>
 *
 * <p>Calling {@code query()} implicitly flushes any previously enqueued statements, so pipelining
 * composes naturally with immediate queries.
 *
 * <p><b>Not thread-safe.</b> Each connection must be used by a single thread at a time.
 * Designed for one-connection-per-(virtual-)thread usage with Java 21+ virtual threads.
 * For concurrent access, use a connection pool where each thread borrows its own connection.
 *
 * @see RowSet
 * @see Transaction
 * @see PreparedStatement
 */
public interface Connection extends AutoCloseable {

  /**
   * Execute a single SQL statement and return the result. Also flushes any previously enqueued
   * pipeline statements.
   *
   * @param sql the SQL statement to execute
   * @return the result set
   * @throws DbException           if the statement fails
   * @throws DbConnectionException if a network/transport error occurs
   */

  RowSet query(String sql);

  /**
   * Execute a parameterized SQL statement and return the result. Also flushes any previously
   * enqueued pipeline statements.
   *
   * <p>Postgres uses {@code $1, $2, ...} placeholders; MySQL uses {@code ?}.
   * Parameters are text-encoded via {@link BinderRegistry} before sending.
   *
   * @param sql    the SQL statement with positional placeholders
   * @param params the parameter values (null elements become SQL NULL)
   * @return the result set
   * @throws DbException           if the statement fails
   * @throws DbConnectionException if a network/transport error occurs
   */

  RowSet query(String sql, Object... params);

  /**
   * Enqueue a simple (non-parameterized) SQL statement for pipelining. The statement is not sent
   * until {@link #flush()} or {@link #query(String)} is called.
   *
   * @param sql the SQL statement
   * @return the index into the results list that {@link #flush()} will return
   */
  int enqueue(String sql);

  /**
   * Enqueue a parameterized SQL statement for pipelining. The statement is not sent until
   * {@link #flush()} or {@link #query(String)} is called.
   *
   * @param sql    the SQL statement with positional placeholders
   * @param params the parameter values
   * @return the index into the results list that {@link #flush()} will return
   */
  int enqueue(String sql, Object... params);

  /**
   * Execute a query with named parameters ({@code :name} style). Named parameters are rewritten to
   * positional placeholders before execution. Collections and arrays are automatically expanded for
   * IN-list queries.
   *
   * @param sql    the SQL with {@code :name} placeholders
   * @param params map of parameter name to value
   * @return the result set
   * @throws DbException if the statement fails
   */

  RowSet query(String sql, Map<String, Object> params);

  /**
   * Enqueue a statement with named parameters for pipelining.
   *
   * @param sql    the SQL with {@code :name} placeholders
   * @param params map of parameter name to value
   * @return the index into the results list that {@link #flush()} will return
   */
  int enqueue(String sql, Map<String, Object> params);

  /**
   * Flush all enqueued statements in a single network write, then read all responses. Returns one
   * {@link RowSet} per enqueued statement, in order. If a statement fails, its RowSet wraps the
   * error and subsequent statements still execute.
   *
   * @return a list of RowSet results, one per enqueued statement
   * @throws DbConnectionException if a network/transport error occurs
   */

  List<RowSet> flush();

  /**
   * Create a server-side prepared statement. The SQL is parsed and planned once; subsequent
   * executions with different parameters skip the parse/plan phase. Close the statement when done
   * to release server resources.
   *
   * @param sql the SQL to prepare
   * @return the prepared statement
   * @throws DbException if preparation fails
   */

  PreparedStatement prepare(String sql);

  /**
   * Create a cursor for progressive row reading. Must be used within a transaction (Postgres
   * requirement). Reads rows in batches via {@link Cursor#read(int)}.
   *
   * @param sql    the query SQL
   * @param params optional query parameters
   * @return the cursor
   * @throws DbException if cursor creation fails
   */

  Cursor cursor(String sql, Object... params);

  /**
   * Begin a transaction. Use with try-with-resources for automatic rollback if
   * {@link Transaction#commit()} is not called.
   *
   * <pre>{@code
   * try (var tx = conn.begin()) {
   *     tx.query("INSERT INTO t VALUES (1)");
   *     tx.commit();
   * } // auto-rollback if commit() not called
   * }</pre>
   *
   * @return a new transaction
   */
  default Transaction begin() {
    return new Transaction(this);
  }

  /**
   * Execute a function within a transaction. Commits if the function returns normally, rolls back
   * if it throws. The function receives a {@link Transaction} (which implements {@link Connection})
   * and can return a result.
   *
   * <p>This is the blocking equivalent of Vert.x's {@code Pool.withTransaction()}.
   *
   * <pre>{@code
   * long id = conn.withTransaction(tx -> {
   *     tx.query("INSERT INTO users (name) VALUES ($1)", "Alice");
   *     return tx.query("SELECT lastval()").first().getLong(0);
   * });
   * }</pre>
   *
   * @param function the code to execute within the transaction
   * @param <T>      the return type
   * @return the value returned by the function
   * @throws DbException if the function throws (transaction is rolled back)
   */
  default <T> T withTransaction(Function<Transaction, T> function) {
    try (var tx = begin()) {
      T result = function.apply(tx);
      tx.commit();
      return result;
    }
    // If function threw, tx.close() auto-rollbacks
  }

  /**
   * Execute the same SQL statement with multiple parameter sets in a single pipeline. More
   * efficient than calling {@code enqueue()} in a loop for the same statement, because the server
   * can parse the statement once and execute it multiple times.
   *
   * <pre>{@code
   * List<RowSet> results = conn.executeMany(
   *     "INSERT INTO users (name, age) VALUES ($1, $2)",
   *     List.of(
   *         new Object[]{"Alice", 30},
   *         new Object[]{"Bob", 25},
   *         new Object[]{"Carol", 35}
   *     ));
   * }</pre>
   *
   * @param sql       the SQL statement with positional parameters
   * @param paramSets list of parameter arrays, one per execution
   * @return list of RowSet results, one per parameter set
   */

  List<RowSet> executeMany(
      String sql,
      List<Object[]> paramSets
  );

  /**
   * Check that this connection is alive and usable.
   *
   * <p>Postgres sends an empty query ({@code ""}), MySQL sends {@code COM_PING}.
   * Both are lightweight server roundtrips. Throws {@link DbException} (typically
   * {@link DbConnectionException}) if the connection is broken.
   */
  void ping();

  /**
   * Server parameters received during connection startup. For Postgres, includes settings like
   * {@code server_version}, {@code client_encoding}, etc. For MySQL, includes connection
   * attributes.
   *
   * @return an unmodifiable map of parameter name to value
   */

  Map<String, String> parameters();

  /**
   * Execute a query and process each row via the callback. Rows are not materialized in memory —
   * constant memory usage regardless of result size. The callback is invoked once per row; Row
   * objects must not be retained after the callback returns.
   *
   * @param sql      the SQL statement
   * @param consumer called once per result row
   * @throws DbException if the statement fails
   */
  void queryStream(String sql, Consumer<Row> consumer);

  /**
   * Execute a parameterized query and process each row via the callback.
   *
   * @param sql      the SQL with positional placeholders
   * @param consumer called once per result row
   * @param params   the parameter values
   * @throws DbException if the statement fails
   */
  void queryStream(
      String sql,
      Consumer<Row> consumer,
      Object... params
  );

  /**
   * Return a closeable, iterable stream of rows. Must be closed after use (use try-with-resources)
   * to keep the connection in a clean state. Rows are fetched lazily from the server — constant
   * memory usage.
   *
   * <pre>{@code
   * try (var rows = conn.stream("SELECT * FROM big_table")) {
   *     for (Row row : rows) {
   *         process(row);
   *     }
   * }
   * }</pre>
   *
   * @param sql    the SQL statement
   * @param params optional positional parameters
   * @return a closeable row stream
   */

  RowStream stream(String sql, Object... params);

  // --- Configuration ---

  /**
   * Return the type registry used for encoding query parameters.
   */
  BinderRegistry binderRegistry();

  /**
   * Set the type registry used for encoding query parameters.
   */
  void setBinderRegistry(BinderRegistry registry);

  /**
   * Return the mapper registry used for typed column access via {@link Row#get(int, Class)}.
   */

  ColumnMapperRegistry mapperRegistry();

  /**
   * Set the mapper registry used for typed column access.
   */
  void setColumnMapperRegistry(ColumnMapperRegistry registry);

  /**
   * Return the JSON mapper, or null if none is configured.
   */

  JsonMapper jsonMapper();

  /**
   * Set the JSON mapper for serializing/deserializing JSON columns.
   */
  void setJsonMapper(JsonMapper mapper);

  /**
   * Close this connection and release all resources.
   */
  @Override
  void close();
}
