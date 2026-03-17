package io.github.bpdbi.pg;

import io.github.bpdbi.core.DbException;
import io.github.bpdbi.pg.impl.codec.BackendMessage;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Postgres-specific exception with additional error fields. */
@SuppressWarnings("serial") // serialization not supported for database exceptions
public class PgException extends DbException {

  private final String detail;
  private final String hint;
  private final String schema;
  private final String table;
  private final String column;
  private final String constraint;

  public PgException(
      @Nullable String severity,
      @Nullable String sqlState,
      @NonNull String message,
      @Nullable String detail,
      @Nullable String hint,
      @Nullable String schema,
      @Nullable String table,
      @Nullable String column,
      @Nullable String constraint) {
    super(severity, sqlState, message);
    this.detail = detail;
    this.hint = hint;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.constraint = constraint;
  }

  public PgException(
      @Nullable String severity,
      @Nullable String sqlState,
      @NonNull String message,
      @Nullable String detail,
      @Nullable String hint) {
    this(severity, sqlState, message, detail, hint, null, null, null, null);
  }

  public static @NonNull PgException fromErrorResponse(BackendMessage.ErrorResponse err) {
    return new PgException(
        err.severity(),
        err.code(),
        err.message(),
        err.detail(),
        err.hint(),
        err.schema(),
        err.table(),
        err.column(),
        err.constraint());
  }

  public @Nullable String detail() {
    return detail;
  }

  public @Nullable String hint() {
    return hint;
  }

  public @Nullable String schema() {
    return schema;
  }

  public @Nullable String table() {
    return table;
  }

  public @Nullable String column() {
    return column;
  }

  public @Nullable String constraint() {
    return constraint;
  }

  @Override
  public @NonNull String toString() {
    var sb = new StringBuilder("PgException: ");
    if (severity() != null) {
      sb.append(severity()).append(": ");
    }
    sb.append(getMessage());
    if (sqlState() != null) {
      sb.append(" [").append(sqlState()).append("]");
    }
    if (detail != null) {
      sb.append("\n  Detail: ").append(detail);
    }
    if (hint != null) {
      sb.append("\n  Hint: ").append(hint);
    }
    if (sql() != null) {
      sb.append("\n  SQL: ").append(sql());
    }
    return sb.toString();
  }
}
