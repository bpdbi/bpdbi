package io.djb;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Base exception for all database errors (unchecked).
 *
 * <p>Database-specific drivers extend this with additional fields:
 *
 * <ul>
 *   <li>{@code PgException} — adds detail, hint, schema, table, column, constraint
 *   <li>{@code MysqlException} — adds MySQL error code
 * </ul>
 *
 * <p>{@link DbConnectionException} is a subclass for transport and I/O errors, allowing callers to
 * distinguish SQL errors from connectivity failures.
 *
 * @see DbConnectionException
 */
@SuppressWarnings("serial") // serialization not supported for database exceptions
public class DbException extends RuntimeException {

  private final String severity;
  private final String sqlState;

  public DbException(
      @Nullable String severity, @Nullable String sqlState, @NonNull String message) {
    super(message);
    this.severity = severity;
    this.sqlState = sqlState;
  }

  public DbException(
      @Nullable String severity,
      @Nullable String sqlState,
      @NonNull String message,
      @Nullable Throwable cause) {
    super(message, cause);
    this.severity = severity;
    this.sqlState = sqlState;
  }

  public @Nullable String severity() {
    return severity;
  }

  public @Nullable String sqlState() {
    return sqlState;
  }

  @Override
  public String toString() {
    var sb = new StringBuilder("DbException: ");
    if (severity != null) {
      sb.append(severity).append(": ");
    }
    sb.append(getMessage());
    if (sqlState != null) {
      sb.append(" [").append(sqlState).append("]");
    }
    return sb.toString();
  }
}
