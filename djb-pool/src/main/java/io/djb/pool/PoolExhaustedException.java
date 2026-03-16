package io.djb.pool;

/**
 * Thrown when the pool's wait queue is full and no connections are available.
 *
 * @see PoolConfig#maxWaitQueueSize()
 */
@SuppressWarnings("serial")
public class PoolExhaustedException extends PoolException {

  public PoolExhaustedException(int maxWaitQueueSize) {
    super("Connection pool wait queue is full (max " + maxWaitQueueSize + ")");
  }
}
