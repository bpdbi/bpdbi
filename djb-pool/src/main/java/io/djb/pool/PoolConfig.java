package io.djb.pool;

import org.jspecify.annotations.NonNull;

/**
 * Configuration for a {@link ConnectionPool} with fluent builder API.
 *
 * <p>All settings have sensible defaults suitable for small-to-medium deployments:
 *
 * <pre>{@code
 * var config = new PoolConfig()
 *     .maxSize(20)
 *     .maxIdleTimeMillis(300_000)    // 5 minutes
 *     .connectionTimeoutMillis(5000) // 5 seconds
 *     .validateOnBorrow(true);       // ping before handing out idle connections
 * }</pre>
 *
 * @see ConnectionPool
 */
public final class PoolConfig {

  private int maxSize = 10;
  private int maxWaitQueueSize = -1; // -1 = unbounded
  private long maxIdleTimeMillis = 600_000; // 10 minutes
  private long maxLifetimeMillis = 0; // 0 = disabled
  private long connectionTimeoutMillis = 30_000; // 30 seconds
  private long poolCleanerPeriodMillis = 1_000; // 1 second
  private boolean validateOnBorrow = false;
  private long leakDetectionThresholdMillis = 0; // 0 = disabled

  public PoolConfig() {}

  /** Maximum number of connections in the pool. Default: 10. */
  public int maxSize() {
    return maxSize;
  }

  public @NonNull PoolConfig maxSize(int maxSize) {
    if (maxSize < 1) {
      throw new IllegalArgumentException("maxSize must be >= 1");
    }
    this.maxSize = maxSize;
    return this;
  }

  /**
   * Maximum number of requests waiting for a connection when the pool is exhausted. {@code -1}
   * means unbounded. Default: -1.
   */
  public int maxWaitQueueSize() {
    return maxWaitQueueSize;
  }

  public @NonNull PoolConfig maxWaitQueueSize(int maxWaitQueueSize) {
    if (maxWaitQueueSize < -1) {
      throw new IllegalArgumentException("maxWaitQueueSize must be >= -1");
    }
    this.maxWaitQueueSize = maxWaitQueueSize;
    return this;
  }

  /** Max time a connection can sit idle before being evicted. 0 = disabled. Default: 10 minutes. */
  public long maxIdleTimeMillis() {
    return maxIdleTimeMillis;
  }

  public @NonNull PoolConfig maxIdleTimeMillis(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("maxIdleTimeMillis must be >= 0");
    }
    this.maxIdleTimeMillis = millis;
    return this;
  }

  /**
   * Max lifetime of a connection regardless of activity. Connections that exceed this are evicted.
   * 0 = disabled. Default: 0 (disabled).
   */
  public long maxLifetimeMillis() {
    return maxLifetimeMillis;
  }

  public @NonNull PoolConfig maxLifetimeMillis(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("maxLifetimeMillis must be >= 0");
    }
    this.maxLifetimeMillis = millis;
    return this;
  }

  /** Max time to wait for a connection from the pool. Default: 30 seconds. */
  public long connectionTimeoutMillis() {
    return connectionTimeoutMillis;
  }

  public @NonNull PoolConfig connectionTimeoutMillis(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("connectionTimeoutMillis must be >= 0");
    }
    this.connectionTimeoutMillis = millis;
    return this;
  }

  /**
   * How often the background pool cleaner runs to evict idle/expired connections, in milliseconds.
   * Only active when {@link #maxIdleTimeMillis()} > 0 or {@link #maxLifetimeMillis()} > 0. {@code
   * <= 0} disables background eviction (connections are still checked on acquire). Default: 1000 (1
   * second).
   */
  public long poolCleanerPeriodMillis() {
    return poolCleanerPeriodMillis;
  }

  public @NonNull PoolConfig poolCleanerPeriodMillis(long millis) {
    this.poolCleanerPeriodMillis = millis;
    return this;
  }

  /**
   * Whether to validate connections with {@link io.djb.Connection#ping()} when borrowing from the
   * idle pool. When {@code true}, stale connections (e.g. after a network blip) are detected and
   * discarded before being handed to the caller. Adds one lightweight roundtrip per borrow from
   * idle. Default: false.
   */
  public boolean validateOnBorrow() {
    return validateOnBorrow;
  }

  public @NonNull PoolConfig validateOnBorrow(boolean validateOnBorrow) {
    this.validateOnBorrow = validateOnBorrow;
    return this;
  }

  /**
   * Time in milliseconds after which a connection held without being returned triggers a leak
   * warning in the logs. 0 = disabled. Default: 0 (disabled). Requires {@link
   * #poolCleanerPeriodMillis()} &gt; 0 to be effective.
   */
  public long leakDetectionThresholdMillis() {
    return leakDetectionThresholdMillis;
  }

  public @NonNull PoolConfig leakDetectionThresholdMillis(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("leakDetectionThresholdMillis must be >= 0");
    }
    this.leakDetectionThresholdMillis = millis;
    return this;
  }
}
