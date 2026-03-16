package io.djb.mysql;

import io.djb.DbException;
import io.djb.mysql.impl.codec.MysqlDecoder;

/**
 * MySQL-specific exception.
 */
@SuppressWarnings("serial")
public class MysqlException extends DbException {

  private final int errorCode;

  public MysqlException(int errorCode, String sqlState, String message) {
    super("ERROR", sqlState, message);
    this.errorCode = errorCode;
  }

  public static MysqlException fromErrPacket(MysqlDecoder.ErrPacket err) {
    return new MysqlException(err.errorCode(), err.sqlState(), err.message());
  }

  public int errorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return "MysqlException: " + getMessage() + " [" + sqlState() + "] (errno=" + errorCode + ")";
  }
}
