package io.github.bpdbi.pool;

import io.github.bpdbi.core.Connection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A connection pool for bpdbi connections, optimized for minimal hot-path overhead.
 *
 * <p>Thread-safe. Each call to {@link #acquire()} borrows a connection from the pool; callers
 * return it by calling {@link Connection#close()} on the returned connection (which redirects to
 * the pool instead of closing). Alternatively, use {@link #withConnection(ConnectionAction)} for
 * automatic release.
 *
 * <p>The pool uses thread-local affinity inspired by HikariCP's ConcurrentBag: each thread
 * remembers its last-used connection and tries to reclaim it on the next acquire, avoiding shared
 * queue contention in the common borrow-use-return-borrow pattern.
 *
 * <p>Timestamps use {@link System#nanoTime()} (monotonic, not wall-clock) for all internal
 * lifecycle management (idle time, max lifetime, leak detection). This avoids wall-clock drift
 * issues and is cheaper to call on many platforms.
 */
public final class ConnectionPool implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(ConnectionPool.class.getName());

  private final ConnectionFactory factory;
  private final PoolConfig config;
  private final BlockingQueue<PooledConnection> idle;
  private final AtomicInteger totalCount = new AtomicInteger(0);
  private final AtomicInteger waitCount = new AtomicInteger(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final @Nullable ScheduledExecutorService cleaner;
  private final @Nullable ScheduledFuture<?> cleanerTask;

  // Leak detection: only tracked when leak detection is enabled. Uses a CopyOnWriteArrayList
  // since writes (acquire/release) are infrequent relative to the cleaner's read sweeps, and
  // this avoids the ConcurrentHashMap overhead on every acquire/release.
  private final @Nullable CopyOnWriteArrayList<PooledConnection> activeForLeakDetection;

  // Converted config values from millis to nanos, cached to avoid repeated conversion.
  private final long maxIdleTimeNanos;
  private final long connectionTimeoutNanos;
  private final long leakDetectionThresholdNanos;

  /**
   * Thread-local hint for the last connection used by each thread. Checked first on acquire to
   * avoid contending on the shared idle queue — a pattern borrowed from HikariCP's ConcurrentBag.
   * The connection is only used if it is still idle and valid; otherwise it is ignored and the
   * normal path is taken.
   */
  private final ThreadLocal<PooledConnection> threadLocal = new ThreadLocal<>();

  public ConnectionPool(@NonNull ConnectionFactory factory, @NonNull PoolConfig config) {
    this.factory = factory;
    this.config = config;
    this.idle = new ArrayBlockingQueue<>(config.maxSize());
    this.maxIdleTimeNanos = TimeUnit.MILLISECONDS.toNanos(config.maxIdleTimeMillis());
    this.connectionTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(config.connectionTimeoutMillis());
    this.leakDetectionThresholdNanos =
        TimeUnit.MILLISECONDS.toNanos(config.leakDetectionThresholdMillis());

    // Only allocate the active-connections list when leak detection is enabled
    this.activeForLeakDetection =
        config.leakDetectionThresholdMillis() > 0 ? new CopyOnWriteArrayList<>() : null;

    boolean needsCleaner =
        config.poolCleanerPeriodMillis() > 0
            && (config.maxIdleTimeMillis() > 0
                || config.maxLifetimeMillis() > 0
                || config.leakDetectionThresholdMillis() > 0);
    if (needsCleaner) {
      this.cleaner =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "bpdbi-pool-cleaner");
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

      // Fast path: check thread-local hint first to avoid shared queue contention.
      // The hint is only used if the connection is still in the idle queue (remove returns true).
      PooledConnection pc = threadLocal.get();
      if (pc != null) {
        threadLocal.remove();
        if (idle.remove(pc)) {
          // Successfully claimed from queue — validate like any other idle connection
          if (isValid(pc) && validateConnection(pc)) {
            return checkout(pc);
          }
          discard(pc);
          continue; // slot freed — try again
        }
        // Hint was stale (connection already evicted or claimed by another thread) — fall through
      }

      // Try to get an idle connection (non-blocking)
      pc = idle.poll();
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
          Connection conn;
          try {
            conn = factory.create();
          } catch (Exception e) {
            totalCount.decrementAndGet();
            throw e;
          }
          pc =
              new PooledConnection(
                  conn, this, System.nanoTime(), computeEffectiveMaxLifetimeNanos());
          return checkout(pc); // if hook fails, checkout calls discard (which decrements)
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
        pc = idle.poll(connectionTimeoutNanos, TimeUnit.NANOSECONDS);
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
    pc.returnedAtNanos = System.nanoTime();
    if (activeForLeakDetection != null) {
      activeForLeakDetection.remove(pc);
    }
    if (closed.get()) {
      discard(pc);
      return;
    }
    var beforeReturn = config.beforeReturn();
    if (beforeReturn != null) {
      try {
        beforeReturn.accept(pc);
      } catch (Exception e) {
        discard(pc);
        return;
      }
    }
    if (!idle.offer(pc)) {
      discard(pc);
      return;
    }
    // Set thread-local hint so this thread can reclaim the same connection on next acquire,
    // avoiding shared queue contention in the common borrow-use-return-borrow pattern.
    threadLocal.set(pc);
  }

  /** Execute an action with a pooled connection, automatically releasing it when done. */
  public <T> T withConnection(@NonNull ConnectionFunction<T> action) {
    try (Connection conn = acquire()) {
      return action.apply(conn);
    }
  }

  /** Execute an action with a pooled connection (no return value). */
  @SuppressWarnings("overloads") // intentional overload for void-returning variant
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
    return totalCount.get() - idle.size();
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
    pc.acquiredAtNanos = System.nanoTime();
    if (activeForLeakDetection != null) {
      activeForLeakDetection.add(pc);
    }
    var afterAcquire = config.afterAcquire();
    if (afterAcquire != null) {
      try {
        afterAcquire.accept(pc);
      } catch (Exception e) {
        if (activeForLeakDetection != null) {
          activeForLeakDetection.remove(pc);
        }
        discard(pc);
        throw e;
      }
    }
    return pc;
  }

  private void maintain() {
    evict();
    detectLeaks();
  }

  private void evict() {
    long now = System.nanoTime();
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
    if (activeForLeakDetection == null || leakDetectionThresholdNanos <= 0) {
      return;
    }
    long now = System.nanoTime();
    for (PooledConnection pc : activeForLeakDetection) {
      long heldNanos = now - pc.acquiredAtNanos;
      if (heldNanos >= leakDetectionThresholdNanos) {
        long heldMs = TimeUnit.NANOSECONDS.toMillis(heldNanos);
        long threshMs = config.leakDetectionThresholdMillis();
        LOG.warning(
            "Possible connection leak detected: connection held for "
                + heldMs
                + "ms (threshold: "
                + threshMs
                + "ms)");
      }
    }
  }

  private boolean isValid(PooledConnection pc) {
    return isValid(pc, System.nanoTime());
  }

  private boolean isValid(PooledConnection pc, long nowNanos) {
    if (maxIdleTimeNanos > 0) {
      if (nowNanos - pc.returnedAtNanos >= maxIdleTimeNanos) {
        return false;
      }
    }
    long effectiveLifetimeNanos = pc.effectiveMaxLifetimeNanos;
    if (effectiveLifetimeNanos > 0) {
      return (nowNanos - pc.createdAtNanos) < effectiveLifetimeNanos;
    }
    return true;
  }

  /**
   * Compute a per-connection max lifetime with random variance (up to 25%) subtracted. This
   * prevents all connections created around the same time from expiring simultaneously, which would
   * cause a burst of reconnections (thundering herd). Inspired by HikariCP.
   */
  private long computeEffectiveMaxLifetimeNanos() {
    long maxLifetime = config.maxLifetimeMillis();
    if (maxLifetime <= 0) {
      return 0;
    }
    long maxLifetimeNanos = TimeUnit.MILLISECONDS.toNanos(maxLifetime);
    // Apply up to 25% variance for lifetimes > 10 seconds; skip variance for very short lifetimes
    if (maxLifetime > 10_000) {
      long variance = ThreadLocalRandom.current().nextLong(maxLifetimeNanos / 4);
      return maxLifetimeNanos - variance;
    }
    return maxLifetimeNanos;
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
    if (activeForLeakDetection != null) {
      activeForLeakDetection.remove(pc);
    }
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
