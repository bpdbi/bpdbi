package io.github.bpdbi.pool;

import io.github.bpdbi.core.Connection;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A simple db connection pool for Djb connections.
 *
 * <p>Thread-safe. Each call to {@link #acquire()} borrows a connection from the pool; callers
 * return it by calling {@link Connection#close()} on the returned connection (which redirects to
 * the pool instead of closing). Alternatively, use {@link #withConnection(ConnectionAction)} for
 * automatic release.
 *
 * <p>Connections are created lazily up to {@link PoolConfig#maxSize()}. If all connections are in
 * use, {@link #acquire()} blocks until one is returned or the configured timeout expires.
 *
 * <p>When {@link PoolConfig#maxIdleTimeMillis()} or {@link PoolConfig#maxLifetimeMillis()} are
 * configured and {@link PoolConfig#poolCleanerPeriodMillis()} &gt; 0, a background thread
 * periodically evicts expired connections and checks for leaked connections.
 */
public final class ConnectionPool implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(ConnectionPool.class.getName());

  private final ConnectionFactory factory;
  private final PoolConfig config;
  private final BlockingQueue<PooledConnection> idle;
  private final Set<PooledConnection> active = ConcurrentHashMap.newKeySet();
  private final AtomicInteger totalCount = new AtomicInteger(0);
  private final AtomicInteger waitCount = new AtomicInteger(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final @Nullable ScheduledExecutorService cleaner;
  private final @Nullable ScheduledFuture<?> cleanerTask;

  public ConnectionPool(@NonNull ConnectionFactory factory, @NonNull PoolConfig config) {
    this.factory = factory;
    this.config = config;
    this.idle = new ArrayBlockingQueue<>(config.maxSize());

    boolean needsCleaner =
        config.poolCleanerPeriodMillis() > 0
            && (config.maxIdleTimeMillis() > 0
                || config.maxLifetimeMillis() > 0
                || config.leakDetectionThresholdMillis() > 0);
    if (needsCleaner) {
      this.cleaner =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "djb-pool-cleaner");
                t.setDaemon(true);
                return t;
              });
      this.cleanerTask =
          cleaner.scheduleAtFixedRate(
              this::maintain,
              config.poolCleanerPeriodMillis(),
              config.poolCleanerPeriodMillis(),
              TimeUnit.MILLISECONDS);
    } else {
      this.cleaner = null;
      this.cleanerTask = null;
    }
  }

  public ConnectionPool(@NonNull ConnectionFactory factory) {
    this(factory, new PoolConfig());
  }

  /**
   * Acquire a connection from the pool. Creates a new one if the pool is below max size and no idle
   * connections are available.
   *
   * <p>The returned connection's {@link Connection#close()} method returns it to the pool instead
   * of closing the underlying connection, so callers can use try-with-resources.
   *
   * @throws IllegalStateException if the pool is closed
   * @throws RuntimeException if the timeout expires or the wait queue is full
   */
  public @NonNull Connection acquire() {
    while (true) {
      if (closed.get()) {
        throw new IllegalStateException("Pool is closed");
      }

      // Try to get an idle connection first (non-blocking)
      PooledConnection pc = idle.poll();
      if (pc != null) {
        if (isValid(pc) && validateConnection(pc)) {
          return checkout(pc);
        }
        discard(pc);
        continue; // slot freed — try again
      }

      // Try to create a new connection if below max size (CAS loop — no overshoot)
      int current;
      while ((current = totalCount.get()) < config.maxSize()) {
        if (totalCount.compareAndSet(current, current + 1)) {
          try {
            Connection conn = factory.create();
            pc = new PooledConnection(conn, this, System.currentTimeMillis());
            return checkout(pc);
          } catch (Exception e) {
            totalCount.decrementAndGet();
            throw e;
          }
        }
      }

      // Check wait queue limit
      int maxWait = config.maxWaitQueueSize();
      if (maxWait >= 0) {
        int waiting = waitCount.incrementAndGet();
        if (waiting > maxWait) {
          waitCount.decrementAndGet();
          throw new PoolExhaustedException(maxWait);
        }
      }

      // All connections are in use — wait for one to be returned
      try {
        pc = idle.poll(config.connectionTimeoutMillis(), TimeUnit.MILLISECONDS);
        if (pc == null) {
          throw new PoolTimeoutException(config.connectionTimeoutMillis());
        }
        if (isValid(pc) && validateConnection(pc)) {
          return checkout(pc);
        }
        discard(pc);
        // Discarded stale connection freed a slot — loop to try creating a new one
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for a connection", e);
      } finally {
        if (maxWait >= 0) {
          waitCount.decrementAndGet();
        }
      }
    }
  }

  /** Return a connection to the pool. Called internally by {@link PooledConnection#close()}. */
  void release(Connection connection) {
    if (!(connection instanceof PooledConnection pc)) {
      // Not a pooled connection — close it directly
      closeQuietly(connection);
      return;
    }
    active.remove(pc);
    pc.returnedAt = System.currentTimeMillis();
    if (closed.get()) {
      discard(pc);
      return;
    }
    if (!idle.offer(pc)) {
      discard(pc);
    }
  }

  /** Execute an action with a pooled connection, automatically releasing it when done. */
  public <T> T withConnection(@NonNull ConnectionFunction<T> action) {
    try (Connection conn = acquire()) {
      return action.apply(conn);
    }
  }

  /** Execute an action with a pooled connection (no return value). */
  @SuppressWarnings("overloads")
  public void withConnection(@NonNull ConnectionAction action) {
    try (Connection conn = acquire()) {
      action.accept(conn);
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

  /** Number of connections currently borrowed and in use. */
  public int activeCount() {
    return active.size();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (cleanerTask != null) {
        cleanerTask.cancel(false);
      }
      if (cleaner != null) {
        cleaner.shutdown();
      }
      PooledConnection pc;
      while ((pc = idle.poll()) != null) {
        discard(pc);
      }
    }
  }

  private Connection checkout(PooledConnection pc) {
    pc.acquiredAt = System.currentTimeMillis();
    active.add(pc);
    return pc;
  }

  private void maintain() {
    evict();
    detectLeaks();
  }

  private void evict() {
    long now = System.currentTimeMillis();
    int size = idle.size();
    for (int i = 0; i < size; i++) {
      PooledConnection pc = idle.poll();
      if (pc == null) {
        break;
      }
      if (isValid(pc, now)) {
        boolean offerSuccessful = idle.offer(pc);
        if (offerSuccessful) {
          continue;
        }
      }
      discard(pc);
    }
  }

  private void detectLeaks() {
    long threshold = config.leakDetectionThresholdMillis();
    if (threshold <= 0) {
      return;
    }
    long now = System.currentTimeMillis();
    for (PooledConnection pc : active) {
      long held = now - pc.acquiredAt;
      if (held >= threshold) {
        LOG.warning(
            "Possible connection leak detected: connection held for "
                + held
                + "ms (threshold: "
                + threshold
                + "ms)");
      }
    }
  }

  private boolean isValid(PooledConnection pc) {
    return isValid(pc, System.currentTimeMillis());
  }

  private boolean isValid(PooledConnection pc, long now) {
    long maxIdle = config.maxIdleTimeMillis();
    if (maxIdle > 0) {
      if (now - pc.returnedAt >= maxIdle) {
        return false;
      }
    }
    long maxLifetime = config.maxLifetimeMillis();
    if (maxLifetime > 0) {
      return (now - pc.createdAt) < maxLifetime;
    }
    return true;
  }

  private boolean validateConnection(PooledConnection pc) {
    if (!config.validateOnBorrow()) {
      return true;
    }
    try {
      pc.delegate().ping();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void discard(PooledConnection pc) {
    totalCount.decrementAndGet();
    pc.closeDelegate();
  }

  private static void closeQuietly(Connection conn) {
    try {
      conn.close();
    } catch (Exception e) {
      // best effort
    }
  }

  @FunctionalInterface
  public interface ConnectionFunction<T> {

    T apply(@NonNull Connection connection);
  }

  @FunctionalInterface
  public interface ConnectionAction {

    void accept(@NonNull Connection connection);
  }
}
