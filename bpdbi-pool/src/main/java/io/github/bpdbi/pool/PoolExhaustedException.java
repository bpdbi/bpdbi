package io.github.bpdbi.pool;

/**
 * Thrown when the pool's wait queue is full and no connections are available.
 *
 * @see PoolConfig#maxWaitQueueSize()
 */
@SuppressWarnings("serial") // Would trigger a warning otherwise (and warnings break the build).
public class PoolExhaustedException extends PoolException {

  public PoolExhaustedException(int maxWaitQueueSize) {
    super("Connection pool wait queue is full (max " + maxWaitQueueSize + ")");
  }
}
