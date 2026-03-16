package io.djb.pool;

import io.djb.*;

import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionPoolTest {

    /** Minimal stub connection for testing pool mechanics (no real database). */
    static class StubConnection implements Connection {
        boolean closed = false;
        final int id;

        StubConnection(int id) { this.id = id; }

        @Override public RowSet query(String sql) { return null; }
        @Override public RowSet query(String sql, Object... params) { return null; }
        @Override public int enqueue(String sql) { return 0; }
        @Override public int enqueue(String sql, Object... params) { return 0; }
        @Override public RowSet query(String sql, Map<String, Object> params) { return null; }
        @Override public int enqueue(String sql, Map<String, Object> params) { return 0; }
        @Override public List<RowSet> flush() { return List.of(); }
        @Override public PreparedStatement prepare(String sql) { return null; }
        @Override public Cursor cursor(String sql, Object... params) { return null; }
        @Override public Map<String, String> parameters() { return Map.of(); }
        @Override public void close() { closed = true; }
    }

    @Test
    void acquireAndRelease() {
        var counter = new AtomicInteger();
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3));

        var c1 = pool.acquire();
        assertNotNull(c1);
        assertEquals(1, pool.totalCount());
        assertEquals(0, pool.idleCount());

        pool.release(c1);
        assertEquals(1, pool.totalCount());
        assertEquals(1, pool.idleCount());

        // Reuse the same connection
        var c2 = pool.acquire();
        assertSame(c1, c2);

        pool.release(c2);
        pool.close();
    }

    @Test
    void createsUpToMaxSize() {
        var counter = new AtomicInteger();
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3));

        var c1 = pool.acquire();
        var c2 = pool.acquire();
        var c3 = pool.acquire();
        assertEquals(3, pool.totalCount());

        pool.release(c1);
        pool.release(c2);
        pool.release(c3);
        pool.close();
    }

    @Test
    void withConnectionAutoRelease() {
        var counter = new AtomicInteger();
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(2));

        pool.withConnection((ConnectionPool.ConnectionAction) conn -> assertNotNull(conn));
        assertEquals(1, pool.idleCount());

        String result = pool.withConnection(conn -> "hello");
        assertEquals("hello", result);

        pool.close();
    }

    @Test
    void closePoolClosesIdleConnections() {
        var counter = new AtomicInteger();
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()));

        var c1 = (StubConnection) pool.acquire();
        var c2 = (StubConnection) pool.acquire();
        pool.release(c1);
        pool.release(c2);

        pool.close();

        assertTrue(c1.closed);
        assertTrue(c2.closed);
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
        var pool = new ConnectionPool(() -> new StubConnection(0),
            new PoolConfig().maxSize(1).connectionTimeoutMillis(100));

        var c1 = pool.acquire();
        // Pool is full, acquire should timeout
        assertThrows(RuntimeException.class, pool::acquire);

        pool.release(c1);
        pool.close();
    }

    @Test
    void concurrentAccess() throws Exception {
        var counter = new AtomicInteger();
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(5));

        int threadCount = 20;
        var latch = new CountDownLatch(threadCount);
        var errors = new AtomicInteger();

        for (int i = 0; i < threadCount; i++) {
            Thread.startVirtualThread(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        pool.withConnection(conn -> {
                            assertNotNull(conn);
                            // Simulate some work
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
        var pool = new ConnectionPool(() -> new StubConnection(counter.incrementAndGet()),
            new PoolConfig().maxSize(3).maxIdleTimeMillis(1)); // 1ms idle timeout

        var c1 = pool.acquire();
        pool.release(c1);

        // Wait for the connection to become stale
        try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Acquire should create a new connection since the old one is stale
        var c2 = pool.acquire();
        assertNotSame(c1, c2);
        assertTrue(((StubConnection) c1).closed);

        pool.release(c2);
        pool.close();
    }
}
