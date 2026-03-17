package io.github.bpdbi.core;

import org.jspecify.annotations.NonNull;

/**
 * Maps a {@link Row} to a typed object.
 *
 * <pre>{@code
 * RowMapper<User> mapper = row -> new User(
 *     row.getInteger("id"),
 *     row.getString("name"),
 *     row.getString("email")
 * );
 * List<User> users = conn.query("SELECT * FROM users").mapTo(mapper);
 * }</pre>
 */
@FunctionalInterface
public interface RowMapper<T> {

  @NonNull T map(@NonNull Row row);
}
