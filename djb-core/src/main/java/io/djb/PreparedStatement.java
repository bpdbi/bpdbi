package io.djb;

import org.jspecify.annotations.Nullable;

/**
 * A prepared statement that can be executed multiple times with different parameters.
 * More efficient than repeated query() calls for the same SQL — the database
 * parses and plans the query once.
 *
 * <pre>{@code
 * try (var stmt = conn.prepare("SELECT * FROM users WHERE id = $1")) {
 *     var alice = stmt.query(1);
 *     var bob = stmt.query(2);
 * }
 * }</pre>
 */
public interface PreparedStatement extends AutoCloseable {

    /** Execute the prepared statement with the given parameters. */
    RowSet query(@Nullable Object... params);

    /** Close the prepared statement and release server resources. */
    @Override
    void close();
}
