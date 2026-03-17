package io.djb.mysql;

import io.djb.DbException;
import io.djb.mysql.impl.codec.MysqlDecoder;
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
    return "MysqlException: " + getMessage() + " [" + sqlState() + "] (errno=" + errorCode + ")";
  }
}
