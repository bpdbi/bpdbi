package io.djb.pool;

import io.djb.Connection;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple connection pool for djb connections.
 *
 * <p>Thread-safe. Each call to {@link #acquire()} borrows a connection from the pool;
 * the caller must {@link #release(Connection)} it when done. Alternatively, use
 * {@link #withConnection(ConnectionAction)} for automatic release.
 *
 * <p>Connections are created lazily up to {@link PoolConfig#maxSize()}.
 * If all connections are in use, {@link #acquire()} blocks until one is returned
 * or the configured timeout expires.
 */
public final class ConnectionPool implements AutoCloseable {

    private final ConnectionFactory factory;
    private final PoolConfig config;
    private final BlockingQueue<PooledEntry> idle;
    private final AtomicInteger totalCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConnectionPool(ConnectionFactory factory, PoolConfig config) {
        this.factory = factory;
        this.config = config;
        this.idle = new ArrayBlockingQueue<>(config.maxSize());
    }

    public ConnectionPool(ConnectionFactory factory) {
        this(factory, new PoolConfig());
    }

    /**
     * Acquire a connection from the pool. Creates a new one if the pool is below
     * max size and no idle connections are available.
     *
     * @throws RuntimeException if the pool is closed or timeout expires
     */
    public Connection acquire() {
        if (closed.get()) throw new IllegalStateException("Pool is closed");

        // Try to get an idle connection first (non-blocking)
        PooledEntry entry = idle.poll();
        if (entry != null) {
            if (isValid(entry)) {
                return entry.connection;
            }
            // Stale connection, close it and try to create a new one
            closeQuietly(entry.connection);
            totalCount.decrementAndGet();
        }

        // Try to create a new connection if below max size
        if (totalCount.get() < config.maxSize()) {
            int newCount = totalCount.incrementAndGet();
            if (newCount <= config.maxSize()) {
                try {
                    return factory.create();
                } catch (Exception e) {
                    totalCount.decrementAndGet();
                    throw e;
                }
            }
            totalCount.decrementAndGet();
        }

        // All connections are in use — wait for one to be returned
        try {
            entry = idle.poll(config.connectionTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (entry == null) {
                throw new RuntimeException("Timeout waiting for a connection from the pool (" +
                    config.connectionTimeoutMillis() + "ms)");
            }
            if (isValid(entry)) {
                return entry.connection;
            }
            closeQuietly(entry.connection);
            totalCount.decrementAndGet();
            // Recurse to try creating a new one
            return acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a connection", e);
        }
    }

    /**
     * Return a connection to the pool. The connection must have been obtained via {@link #acquire()}.
     */
    public void release(Connection connection) {
        if (closed.get()) {
            closeQuietly(connection);
            totalCount.decrementAndGet();
            return;
        }
        if (!idle.offer(new PooledEntry(connection, System.currentTimeMillis()))) {
            // Queue full — shouldn't normally happen, but close the connection
            closeQuietly(connection);
            totalCount.decrementAndGet();
        }
    }

    /**
     * Execute an action with a pooled connection, automatically releasing it when done.
     */
    public <T> T withConnection(ConnectionFunction<T> action) {
        Connection conn = acquire();
        try {
            return action.apply(conn);
        } finally {
            release(conn);
        }
    }

    /**
     * Execute an action with a pooled connection (no return value).
     */
    public void withConnection(ConnectionAction action) {
        Connection conn = acquire();
        try {
            action.accept(conn);
        } finally {
            release(conn);
        }
    }

    /** Number of idle connections currently in the pool. */
    public int idleCount() {
        return idle.size();
    }

    /** Total number of connections (idle + in use). */
    public int totalCount() {
        return totalCount.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            PooledEntry entry;
            while ((entry = idle.poll()) != null) {
                closeQuietly(entry.connection);
                totalCount.decrementAndGet();
            }
        }
    }

    private boolean isValid(PooledEntry entry) {
        long idleTime = System.currentTimeMillis() - entry.returnedAt;
        return idleTime < config.maxIdleTimeMillis();
    }

    private static void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            // best effort
        }
    }

    private record PooledEntry(Connection connection, long returnedAt) {}

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection);
    }

    @FunctionalInterface
    public interface ConnectionAction {
        void accept(Connection connection);
    }
}
