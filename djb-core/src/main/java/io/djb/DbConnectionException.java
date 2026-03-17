package io.djb;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a connection-level I/O error occurs, such as a network failure, socket timeout, or
 * unexpected disconnect.
 *
 * <p>This is a subclass of {@link DbException} so callers can catch all database errors uniformly,
 * while still distinguishing transport failures from SQL errors when needed:
 *
 * <pre>{@code
 * try {
 *     conn.query("SELECT 1");
 * } catch (DbConnectionException e) {
 *     // network/transport error — connection is likely unusable
 * } catch (DbException e) {
 *     // SQL error — connection may still be usable
 * }
 * }</pre>
 */
@SuppressWarnings("serial")
public class DbConnectionException extends DbException {

  public DbConnectionException(@NonNull String message, @Nullable Throwable cause) {
    super("FATAL", "08006", message, cause);
  }

  public DbConnectionException(@NonNull String message) {
    super("FATAL", "08006", message);
  }
}
