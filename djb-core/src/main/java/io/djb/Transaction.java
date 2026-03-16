package io.djb;

/**
 * A database transaction with auto-rollback support.
 * Use with try-with-resources for automatic rollback on failure.
 *
 * <pre>{@code
 * try (var tx = conn.begin()) {
 *     tx.query("INSERT INTO t VALUES (1)");
 *     tx.query("INSERT INTO t VALUES (2)");
 *     tx.commit();
 * } // auto-rollback if commit() was not called
 * }</pre>
 */
public final class Transaction implements AutoCloseable {

    private final Connection conn;
    private boolean finished = false;

    public Transaction(Connection conn) {
        this.conn = conn;
        conn.query("BEGIN");
    }

    public RowSet query(String sql) {
        checkNotFinished();
        return conn.query(sql);
    }

    public RowSet query(String sql, Object... params) {
        checkNotFinished();
        return conn.query(sql, params);
    }

    public int enqueue(String sql) {
        checkNotFinished();
        return conn.enqueue(sql);
    }

    public int enqueue(String sql, Object... params) {
        checkNotFinished();
        return conn.enqueue(sql, params);
    }

    public java.util.List<RowSet> flush() {
        checkNotFinished();
        return conn.flush();
    }

    public void commit() {
        checkNotFinished();
        conn.query("COMMIT");
        finished = true;
    }

    public void rollback() {
        checkNotFinished();
        conn.query("ROLLBACK");
        finished = true;
    }

    @Override
    public void close() {
        if (!finished) {
            try {
                conn.query("ROLLBACK");
            } catch (Exception e) {
                // best effort — connection may already be broken
            }
            finished = true;
        }
    }

    private void checkNotFinished() {
        if (finished) {
            throw new IllegalStateException("Transaction already committed or rolled back");
        }
    }
}
