package io.djb.pool;

/**
 * Configuration for a {@link ConnectionPool}.
 */
public final class PoolConfig {

    private int maxSize = 10;
    private long maxIdleTimeMillis = 600_000; // 10 minutes
    private long connectionTimeoutMillis = 30_000; // 30 seconds

    public PoolConfig() {}

    /** Maximum number of connections in the pool. Default: 10. */
    public int maxSize() { return maxSize; }
    public PoolConfig maxSize(int maxSize) { this.maxSize = maxSize; return this; }

    /** Max time a connection can sit idle before being closed. Default: 10 minutes. */
    public long maxIdleTimeMillis() { return maxIdleTimeMillis; }
    public PoolConfig maxIdleTimeMillis(long millis) { this.maxIdleTimeMillis = millis; return this; }

    /** Max time to wait for a connection from the pool. Default: 30 seconds. */
    public long connectionTimeoutMillis() { return connectionTimeoutMillis; }
    public PoolConfig connectionTimeoutMillis(long millis) { this.connectionTimeoutMillis = millis; return this; }
}
