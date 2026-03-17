package io.github.bpdbi.mysql;

import io.github.bpdbi.core.DbException;
import io.github.bpdbi.mysql.impl.codec.MysqlDecoder;
import org.jspecify.annotations.NonNull;

/** MySQL-specific exception. */
@SuppressWarnings("serial") // Exception subclass, serialVersionUID not needed
public class MysqlException extends DbException {

  private final int errorCode;

  public MysqlException(int errorCode, @NonNull String sqlState, @NonNull String message) {
    super("ERROR", sqlState, message);
    this.errorCode = errorCode;
  }

  public static @NonNull MysqlException fromErrPacket(MysqlDecoder.ErrPacket err) {
    return new MysqlException(err.errorCode(), err.sqlState(), err.message());
  }

  public int errorCode() {
    return errorCode;
  }

  @Override
  public @NonNull String toString() {
    var sb = new StringBuilder("MysqlException: ");
    sb.append(getMessage())
        .append(" [")
        .append(sqlState())
        .append("] (errno=")
        .append(errorCode)
        .append(")");
    if (sql() != null) {
      sb.append("\n  SQL: ").append(sql());
    }
    return sb.toString();
  }
}
