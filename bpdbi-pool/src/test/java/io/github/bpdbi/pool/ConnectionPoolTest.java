package io.github.bpdbi.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.BinderRegistry;
import io.github.bpdbi.core.ColumnMapperRegistry;
import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.JsonMapper;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

class ConnectionPoolTest {

  /** Minimal stub connection for testing pool mechanics (no real database). */
  static class StubConnection implements Connection {

    boolean closed = false;
    final int id;

    StubConnection(int id) {
      this.id = id;
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql) {
      return null;
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql, Object... params) {
      return null;
    }

    @Override
    public int enqueue(@NonNull String sql) {
      return 0;
    }

    @Override
    public int enqueue(@NonNull String sql, Object... params) {
      return 0;
    }

    @Override
    public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
      return null;
    }

    @Override
    public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
      return 0;
    }

    @Override
    public @NonNull List<RowSet> flush() {
      return List.of();
    }

    @Override
    public @NonNull List<RowSet> executeMany(
        @NonNull String sql, @NonNull List<Object[]> paramSets) {
      return List.of();
    }

    @Override
    public @NonNull PreparedStatement prepare(@NonNull String sql) {
      return null;
    }

    @Override
    public @NonNull Cursor cursor(@NonNull String sql, Object... params) {
      return null;
    }

    @Override
    public void ping() {}

    @Override
    public void queryStream(
        @NonNull String sql, java.util.function.@NonNull Consumer<Row> consumer) {}

    @Override
    public void queryStream(
        @NonNull String sql,
        java.util.function.@NonNull Consumer<Row> consumer,
        Object... params) {}

    @Override
    public @NonNull RowStream stream(@NonNull String sql, Object... params) {
      return new RowStream(() -> null, () -> {});
    }

    @Override
    public @NonNull Map<String, String> parameters() {
      return Map.of();
    }

    @Override
    public void setBinderRegistry(@NonNull BinderRegistry registry) {}

    @Override
    public @NonNull BinderRegistry binderRegistry() {
      return BinderRegistry.defaults();
    }

    @Override
    public void setColumnMapperRegistry(@NonNull ColumnMapperRegistry registry) {}

    @Override
    public @NonNull ColumnMapperRegistry mapperRegistry() {
      return ColumnMapperRegistry.defaults();
    }

    @Override
    public void setJsonMapper(JsonMapper mapper) {}

    @Override
    public JsonMapper jsonMapper() {
      return null;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  /** Unwrap a PooledConnection to get the underlying StubConnection. */
  private static StubConnection unwrap(Connection conn) {
    return (StubConnection) ((PooledConnection) conn).delegate();
  }

  @Test
  void acquireAndRelease() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(3));

    var c1 = pool.acquire();
    assertNotNull(c1);
    assertEquals(1, pool.totalCount());
    assertEquals(0, pool.idleCount());

    c1.close(); // returns to pool
    assertEquals(1, pool.totalCount());
    assertEquals(1, pool.idleCount());

    // Reuse the same underlying connection
    var c2 = pool.acquire();
    assertSame(unwrap(c1), unwrap(c2));

    c2.close();
    pool.close();
  }

  @Test
  void createsUpToMaxSize() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(3));

    var c1 = pool.acquire();
    var c2 = pool.acquire();
    var c3 = pool.acquire();
    assertEquals(3, pool.totalCount());

    c1.close();
    c2.close();
    c3.close();
    pool.close();
  }

  @Test
  void withConnectionAutoRelease() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(2));

    pool.withConnection((ConnectionPool.ConnectionAction) conn -> assertNotNull(conn));
    assertEquals(1, pool.idleCount());

    String result = pool.withConnection(conn -> "hello");
    assertEquals("hello", result);

    pool.close();
  }

  @Test
  void closePoolClosesUnderlyingConnections() {
    var counter = new AtomicInteger();
    var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()));

    var c1 = pool.acquire();
    var c2 = pool.acquire();
    var stub1 = unwrap(c1);
    var stub2 = unwrap(c2);
    c1.close();
    c2.close();

    pool.close();

    assertTrue(stub1.closed);
    assertTrue(stub2.closed);
    assertEquals(0, pool.idleCount());
  }

  @Test
  void acquireOnClosedPoolThrows() {
    var pool = new ConnectionPool(() -> new StubConnection(0));
    pool.close();
    assertThrows(IllegalStateException.class, pool::acquire);
  }

  @Test
  void timeoutWhenPoolExhausted() {
    var pool =
        new ConnectionPool(
            () -> new StubConnection(0), new PoolConfig().maxSize(1).connectionTimeoutMillis(100));

    var c1 = pool.acquire();
    // Pool is full, acquire should timeout with specific exception
    var ex = assertThrows(PoolTimeoutException.class, pool::acquire);
    assertTrue(ex.getMessage().contains("100ms"));

    c1.close();
    pool.close();
  }

  @Test
  void concurrentAccess() throws Exception {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(5));

    int threadCount = 20;
    var latch = new CountDownLatch(threadCount);
    var errors = new AtomicInteger();

    for (int i = 0; i < threadCount; i++) {
      Thread.startVirtualThread(
          () -> {
            try {
              for (int j = 0; j < 10; j++) {
                pool.withConnection(
                    conn -> {
                      assertNotNull(conn);
                      Thread.yield();
                    });
              }
            } catch (Exception e) {
              errors.incrementAndGet();
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(0, errors.get());
    assertTrue(pool.totalCount() <= 5);

    pool.close();
  }

  @Test
  void evictsIdleConnections() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3).maxIdleTimeMillis(1)); // 1ms idle timeout

    var c1 = pool.acquire();
    var stub1 = unwrap(c1);
    c1.close();

    // Wait for the connection to become stale
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Acquire should create a new connection since the old one is stale
    var c2 = pool.acquire();
    assertNotSame(unwrap(c1), unwrap(c2));
    assertTrue(stub1.closed);

    c2.close();
    pool.close();
  }

  @Test
  void maxLifetimeEvictsOldConnections() throws Exception {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3).maxLifetimeMillis(50).maxIdleTimeMillis(0));

    var c1 = pool.acquire();
    var stub1 = unwrap(c1);
    c1.close();

    // Connection is still within lifetime
    var c2 = pool.acquire();
    assertSame(stub1, unwrap(c2));
    c2.close();

    // Wait for max lifetime to expire
    Thread.sleep(60);

    // Should get a new connection
    var c3 = pool.acquire();
    assertNotSame(stub1, unwrap(c3));
    assertTrue(stub1.closed);

    c3.close();
    pool.close();
  }

  @Test
  void backgroundEvictionRemovesExpiredConnections() throws Exception {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3).maxIdleTimeMillis(50).poolCleanerPeriodMillis(25));

    var c1 = pool.acquire();
    var c2 = pool.acquire();
    var stub1 = unwrap(c1);
    var stub2 = unwrap(c2);
    c1.close();
    c2.close();
    assertEquals(2, pool.idleCount());

    // Wait for background eviction to kick in
    Thread.sleep(150);

    assertEquals(0, pool.idleCount());
    assertTrue(stub1.closed);
    assertTrue(stub2.closed);

    pool.close();
  }

  @Test
  void maxWaitQueueSizeRejects() {
    var pool =
        new ConnectionPool(
            () -> new StubConnection(0),
            new PoolConfig().maxSize(1).maxWaitQueueSize(0).connectionTimeoutMillis(100));

    var c1 = pool.acquire();
    // Wait queue is full (max 0), should reject immediately with specific exception
    var ex = assertThrows(PoolExhaustedException.class, pool::acquire);
    assertTrue(ex.getMessage().contains("wait queue is full"));

    c1.close();
    pool.close();
  }

  @Test
  void closeOnConnectionReturnsToPool() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(1));

    var c1 = pool.acquire();
    var stub = unwrap(c1);
    assertEquals(0, pool.idleCount());

    // close() should return to pool, NOT close the underlying connection
    c1.close();
    assertFalse(stub.closed);
    assertEquals(1, pool.idleCount());
    assertEquals(1, pool.totalCount());

    // Can acquire again
    var c2 = pool.acquire();
    assertSame(stub, unwrap(c2));

    c2.close();
    pool.close();
  }

  @Test
  void activeCountTracking() {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()), new PoolConfig().maxSize(3));

    assertEquals(0, pool.activeCount());

    var c1 = pool.acquire();
    assertEquals(1, pool.activeCount());

    var c2 = pool.acquire();
    assertEquals(2, pool.activeCount());

    c1.close();
    assertEquals(1, pool.activeCount());

    c2.close();
    assertEquals(0, pool.activeCount());

    pool.close();
  }

  @Test
  void leakDetectionLogsWarning() throws Exception {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig()
                .maxSize(3)
                .leakDetectionThresholdMillis(50)
                .poolCleanerPeriodMillis(25));

    // Capture log output
    var logHandler =
        new Handler() {
          final List<String> warnings = new CopyOnWriteArrayList<>();

          @Override
          public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
              warnings.add(record.getMessage());
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    var logger = Logger.getLogger(ConnectionPool.class.getName());
    logger.addHandler(logHandler);

    try {
      var c1 = pool.acquire();
      // Hold connection past leak threshold
      Thread.sleep(150);

      assertFalse(logHandler.warnings.isEmpty());
      assertTrue(logHandler.warnings.stream().anyMatch(m -> m.contains("leak")));

      c1.close();
    } finally {
      logger.removeHandler(logHandler);
      pool.close();
    }
  }

  @Test
  void totalCountNeverExceedsMaxSize() throws Exception {
    int maxSize = 3;
    var created = new AtomicInteger();
    var peakTotal = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(created.incrementAndGet()),
            new PoolConfig().maxSize(maxSize).connectionTimeoutMillis(5000));

    int threadCount = 50;
    var barrier = new CyclicBarrier(threadCount);
    var latch = new CountDownLatch(threadCount);
    var errors = new AtomicInteger();

    for (int i = 0; i < threadCount; i++) {
      Thread.startVirtualThread(
          () -> {
            try {
              barrier.await(); // all threads start at once
              for (int j = 0; j < 20; j++) {
                Connection conn = pool.acquire();
                // Record peak totalCount while holding the connection
                peakTotal.accumulateAndGet(pool.totalCount(), Math::max);
                Thread.yield();
                conn.close();
              }
            } catch (Exception e) {
              errors.incrementAndGet();
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS));
    assertEquals(0, errors.get());
    assertTrue(
        peakTotal.get() <= maxSize,
        "totalCount peaked at " + peakTotal.get() + " but maxSize is " + maxSize);

    pool.close();
  }

  @Test
  void poolConfigValidation() {
    assertThrows(IllegalArgumentException.class, () -> new PoolConfig().maxSize(0));
    assertThrows(IllegalArgumentException.class, () -> new PoolConfig().maxSize(-1));
    assertThrows(IllegalArgumentException.class, () -> new PoolConfig().maxIdleTimeMillis(-1));
    assertThrows(IllegalArgumentException.class, () -> new PoolConfig().maxLifetimeMillis(-1));
    assertThrows(
        IllegalArgumentException.class, () -> new PoolConfig().connectionTimeoutMillis(-1));
    assertThrows(IllegalArgumentException.class, () -> new PoolConfig().maxWaitQueueSize(-2));
    assertThrows(
        IllegalArgumentException.class, () -> new PoolConfig().leakDetectionThresholdMillis(-1));

    // These should be fine
    new PoolConfig().maxIdleTimeMillis(0);
    new PoolConfig().maxLifetimeMillis(0);
    new PoolConfig().maxWaitQueueSize(-1);
    new PoolConfig().poolCleanerPeriodMillis(0);
    new PoolConfig().leakDetectionThresholdMillis(0);
  }

  // --- Edge case tests ---

  /** StubConnection whose ping() throws on the first N calls, then succeeds. */
  static class FlakeyPingConnection extends StubConnection {

    private final AtomicInteger pingFailsRemaining;

    FlakeyPingConnection(int id, int failCount) {
      super(id);
      this.pingFailsRemaining = new AtomicInteger(failCount);
    }

    @Override
    public void ping() {
      if (pingFailsRemaining.getAndDecrement() > 0) {
        throw new RuntimeException("ping failed");
      }
    }
  }

  @Test
  void validateOnBorrowRetryAfterFailure() {
    var counter = new AtomicInteger();
    // Factory always creates a normal StubConnection
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3).validateOnBorrow(true).maxIdleTimeMillis(0));

    // Acquire and return a connection
    var c1 = pool.acquire();
    var stub1 = unwrap(c1);
    c1.close();

    // Now replace the idle connection with a flakey one by:
    // 1. Acquire the good connection (empties idle)
    var c2 = pool.acquire();
    assertSame(stub1, unwrap(c2));
    c2.close(); // returns stub1 to idle

    // Swap stub1's ping to fail: we do this by using a different approach —
    // create a pool where the first connection has a bad ping
    var counter2 = new AtomicInteger();
    var badConn = new FlakeyPingConnection(0, 1); // ping fails once
    var created = new AtomicBoolean(false);
    var pool2 =
        new ConnectionPool(
            () -> {
              if (!created.getAndSet(true)) {
                return badConn;
              }
              return new StubConnection(counter2.incrementAndGet());
            },
            new PoolConfig().maxSize(3).validateOnBorrow(true).maxIdleTimeMillis(0));

    // Acquire the flakey connection (fresh create — no validation on fresh connections)
    var first = pool2.acquire();
    assertSame(badConn, unwrap(first));
    first.close(); // return to idle

    // Now acquire again — pool will validate via ping(), which fails on badConn.
    // Pool should discard it and create a new one.
    var second = pool2.acquire();
    assertNotSame(badConn, unwrap(second));
    assertTrue(badConn.closed, "Bad connection should have been closed");

    second.close();
    pool.close();
    pool2.close();
  }

  @Test
  void factoryThrowsIntermittently() {
    var callCount = new AtomicInteger();
    ConnectionFactory flakeyFactory =
        () -> {
          int n = callCount.incrementAndGet();
          if (n % 2 == 0) {
            throw new RuntimeException("Factory failed on call #" + n);
          }
          return new StubConnection(n);
        };

    var pool = new ConnectionPool(flakeyFactory, new PoolConfig().maxSize(3));

    // First call (n=1) succeeds
    var c1 = pool.acquire();
    assertNotNull(c1);
    assertEquals(1, unwrap(c1).id);
    c1.close();

    // Return c1 so it's idle; next acquire reuses it (no factory call)
    var c1Again = pool.acquire();
    assertSame(unwrap(c1), unwrap(c1Again));
    c1Again.close();

    // Hold c1 so pool must create new — n=2 will fail, should throw
    var held = pool.acquire();
    assertThrows(RuntimeException.class, pool::acquire); // n=2 fails

    // totalCount should not have increased from the failed create
    assertEquals(1, pool.totalCount());

    // Next attempt — n=3 succeeds
    var c3 = pool.acquire();
    assertNotNull(c3);
    assertEquals(3, unwrap(c3).id);
    assertEquals(2, pool.totalCount());

    held.close();
    c3.close();
    pool.close();
  }

  @Test
  void concurrentCloseAndAcquire() throws Exception {
    var counter = new AtomicInteger();
    var pool =
        new ConnectionPool(
            () -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(5).connectionTimeoutMillis(1000));

    // Pre-populate the pool
    var conns = new ArrayList<Connection>();
    for (int i = 0; i < 5; i++) {
      conns.add(pool.acquire());
    }
    for (var c : conns) {
      c.close();
    }

    int threadCount = 20;
    var barrier = new CyclicBarrier(threadCount);
    var latch = new CountDownLatch(threadCount);
    var errors = new CopyOnWriteArrayList<Throwable>();

    for (int i = 0; i < threadCount; i++) {
      final int ti = i;
      Thread.startVirtualThread(
          () -> {
            try {
              barrier.await(5, TimeUnit.SECONDS);
              for (int j = 0; j < 50; j++) {
                try {
                  var conn = pool.acquire();
                  Thread.yield();
                  conn.close();
                } catch (IllegalStateException e) {
                  // Expected if pool was closed by another thread
                } catch (PoolTimeoutException | PoolExhaustedException e) {
                  // Also acceptable under contention
                }
              }
            } catch (Exception e) {
              errors.add(e);
            } finally {
              latch.countDown();
            }
          });
    }

    // Close the pool from main thread while others are working
    Thread.sleep(10);
    pool.close();

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Threads should complete without deadlock");
    // Filter out expected BrokenBarrierExceptions (threads that didn't reach barrier before pool
    // closed)
    var unexpectedErrors =
        errors.stream()
            .filter(e -> !(e instanceof java.util.concurrent.BrokenBarrierException))
            .toList();
    assertTrue(unexpectedErrors.isEmpty(), "Unexpected errors: " + unexpectedErrors);
  }

  @Test
  void validateOnBorrowDiscardsBadConnection() {
    var badConn =
        new StubConnection(1) {
          @Override
          public void ping() {
            throw new RuntimeException("connection is dead");
          }
        };
    var counter = new AtomicInteger(1);
    var created = new AtomicBoolean(false);

    var pool =
        new ConnectionPool(
            () -> {
              if (!created.getAndSet(true)) {
                return badConn;
              }
              return new StubConnection(counter.incrementAndGet());
            },
            new PoolConfig().maxSize(3).validateOnBorrow(true).maxIdleTimeMillis(0));

    // Acquire the bad connection (fresh — no validation on create)
    var c1 = pool.acquire();
    assertSame(badConn, unwrap(c1));
    assertFalse(badConn.closed);
    c1.close(); // return to idle

    // Now acquire — validateOnBorrow should detect the bad ping and discard
    var c2 = pool.acquire();
    assertNotSame(badConn, unwrap(c2));
    assertTrue(badConn.closed, "Original bad connection should be closed via discard");
    assertEquals(2, unwrap(c2).id);

    c2.close();
    pool.close();
  }

  @Test
  void acquireAfterPoolCloseFromDifferentThread() throws Exception {
    var pool =
        new ConnectionPool(
            () -> new StubConnection(0), new PoolConfig().maxSize(1).connectionTimeoutMillis(5000));

    // Exhaust the pool so a second acquire will block
    var held = pool.acquire();

    var acquireStarted = new CountDownLatch(1);
    var result = new AtomicReference<Throwable>();

    var waiter =
        Thread.startVirtualThread(
            () -> {
              acquireStarted.countDown();
              try {
                pool.acquire(); // will block waiting for a connection
              } catch (Throwable t) {
                result.set(t);
              }
            });

    // Wait for the waiter thread to start
    assertTrue(acquireStarted.await(2, TimeUnit.SECONDS));
    // Give it a moment to actually enter the blocking poll
    Thread.sleep(50);

    // Close the pool from this thread — the held connection is returned
    // then pool is closed, which should wake up or prevent the waiter
    pool.close();
    held.close(); // returning after pool close triggers discard

    waiter.join(5000);
    assertFalse(waiter.isAlive(), "Waiter thread should have finished");

    // The waiter should have gotten an exception (timeout, ISE, or similar)
    assertNotNull(result.get(), "Blocked acquire should fail after pool is closed");
    assertTrue(
        result.get() instanceof IllegalStateException
            || result.get() instanceof PoolTimeoutException,
        "Expected IllegalStateException or PoolTimeoutException but got: "
            + result.get().getClass().getName());
  }
}
