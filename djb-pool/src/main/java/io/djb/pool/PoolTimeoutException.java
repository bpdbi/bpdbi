package io.djb.pool;

/**
 * Thrown when a connection cannot be acquired within the configured timeout.
 *
 * @see PoolConfig#connectionTimeoutMillis()
 */
@SuppressWarnings("serial")
public class PoolTimeoutException extends PoolException {

  public PoolTimeoutException(long timeoutMillis) {
    super("Timeout waiting for a connection from the pool (" + timeoutMillis + "ms)");
  }
}
