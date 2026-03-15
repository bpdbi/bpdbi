package io.djb;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Result of executing a SQL statement. Contains rows (if any) and metadata.
 */
public final class RowSet implements Iterable<Row> {

    private final List<Row> rows;
    private final List<ColumnDescriptor> columns;
    private final int rowsAffected;
    private final PgException error;

    public RowSet(List<Row> rows, List<ColumnDescriptor> columns, int rowsAffected) {
        this.rows = rows;
        this.columns = columns;
        this.rowsAffected = rowsAffected;
        this.error = null;
    }

    /** Create a RowSet representing a failed statement in a pipeline. */
    public RowSet(PgException error) {
        this.rows = Collections.emptyList();
        this.columns = Collections.emptyList();
        this.rowsAffected = 0;
        this.error = error;
    }

    /** Throws PgException if this result represents a failed pipeline statement. */
    private void checkError() {
        if (error != null) throw error;
    }

    public int size() {
        checkError();
        return rows.size();
    }

    public int rowsAffected() {
        checkError();
        return rowsAffected;
    }

    public List<ColumnDescriptor> columnDescriptors() {
        checkError();
        return columns;
    }

    public Row first() {
        checkError();
        if (rows.isEmpty()) {
            throw new IllegalStateException("No rows in result");
        }
        return rows.getFirst();
    }

    public Stream<Row> stream() {
        checkError();
        return rows.stream();
    }

    @Override
    public Iterator<Row> iterator() {
        checkError();
        return rows.iterator();
    }

    public boolean hasError() {
        return error != null;
    }

    public PgException getError() {
        return error;
    }
}
