package io.djb.pg;

import io.djb.DbException;
import io.djb.pg.impl.codec.BackendMessage;

/**
 * Postgres-specific exception with additional error fields.
 */
@SuppressWarnings("serial")
public class PgException extends DbException {

  private final String detail;
  private final String hint;
  private final String schema;
  private final String table;
  private final String column;
  private final String constraint;

  public PgException(
      String severity,
      String sqlState,
      String message,
      String detail,
      String hint,
      String schema,
      String table,
      String column,
      String constraint
  ) {
    super(severity, sqlState, message);
    this.detail = detail;
    this.hint = hint;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.constraint = constraint;
  }

  public PgException(
      String severity,
      String sqlState,
      String message,
      String detail,
      String hint
  ) {
    this(severity, sqlState, message, detail, hint, null, null, null, null);
  }

  public static PgException fromErrorResponse(BackendMessage.ErrorResponse err) {
    return new PgException(
        err.severity(), err.code(), err.message(),
        err.detail(), err.hint(),
        err.schema(), err.table(), err.column(), err.constraint()
    );
  }

  public String detail() {
    return detail;
  }

  public String hint() {
    return hint;
  }

  public String schema() {
    return schema;
  }

  public String table() {
    return table;
  }

  public String column() {
    return column;
  }

  public String constraint() {
    return constraint;
  }

  @Override
  public String toString() {
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
    return sb.toString();
  }
}
