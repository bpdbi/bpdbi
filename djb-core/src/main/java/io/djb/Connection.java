package io.djb;

import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * A blocking database connection with first-class pipelining support.
 *
 * <p><b>Not thread-safe.</b> Each connection must be used by a single thread at a time.
 * Designed for one-connection-per-(virtual-)thread usage with Java 21+ virtual threads.
 * For concurrent access, use a connection pool (e.g. Apache Commons Pool 2) where each
 * thread borrows its own connection.</p>
 */
public interface Connection extends AutoCloseable {

    /**
     * Execute a single SQL statement and return the result.
     * Also flushes any previously enqueued pipeline statements.
     * Throws {@link DbException} if the statement fails.
     */
    RowSet query(String sql);

    /**
     * Execute a parameterized SQL statement and return the result.
     * Also flushes any previously enqueued pipeline statements.
     * Throws {@link DbException} if the statement fails.
     */
    RowSet query(String sql, @Nullable Object... params);

    /**
     * Enqueue a simple (non-parameterized) SQL statement for pipelining.
     * @return the index into the results list that flush() will return
     */
    int enqueue(String sql);

    /**
     * Enqueue a parameterized SQL statement for pipelining.
     * @return the index into the results list that flush() will return
     */
    int enqueue(String sql, @Nullable Object... params);

    /**
     * Execute a query with named parameters (:name style).
     */
    RowSet query(String sql, Map<String, @Nullable Object> params);

    /**
     * Enqueue a statement with named parameters for pipelining.
     * @return the index into the results list that flush() will return
     */
    int enqueue(String sql, Map<String, @Nullable Object> params);

    /**
     * Flush all enqueued statements in a single network write, then read all responses.
     * @return a list of RowSet, one per enqueued statement, in order
     */
    List<RowSet> flush();

    /**
     * Create a prepared statement. The SQL is parsed and planned once by the server.
     * Close the statement when done to release server resources.
     */
    PreparedStatement prepare(String sql);

    /**
     * Create a cursor for progressive row reading. Must be used within a transaction.
     * @param sql the query SQL
     * @param params optional query parameters
     */
    Cursor cursor(String sql, @Nullable Object... params);

    /**
     * Begin a transaction. Use with try-with-resources for auto-rollback.
     */
    default Transaction begin() {
        return new Transaction(this);
    }

    /** Server parameters received during connection startup. */
    Map<String, String> parameters();

    @Override
    void close();
}
