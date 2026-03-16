package io.djb.pool;


/**
 * Base exception for connection pool errors.
 */
@SuppressWarnings("serial")
public class PoolException extends RuntimeException {

  public PoolException(String message) {
    super(message);
  }

  public PoolException(String message, Throwable cause) {
    super(message, cause);
  }
}
