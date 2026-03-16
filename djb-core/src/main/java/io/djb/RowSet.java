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
    private final DbException error;

    public RowSet(List<Row> rows, List<ColumnDescriptor> columns, int rowsAffected) {
        this.rows = rows;
        this.columns = columns;
        this.rowsAffected = rowsAffected;
        this.error = null;
    }

    /** Create a RowSet representing a failed statement in a pipeline. */
    public RowSet(DbException error) {
        this.rows = Collections.emptyList();
        this.columns = Collections.emptyList();
        this.rowsAffected = 0;
        this.error = error;
    }

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

    /**
     * Map all rows to typed objects using the given mapper.
     */
    public <T> List<T> mapTo(RowMapper<T> mapper) {
        checkError();
        return rows.stream().map(mapper::map).toList();
    }

    /**
     * Map the first row to a typed object using the given mapper.
     * @throws IllegalStateException if no rows
     */
    public <T> T mapFirst(RowMapper<T> mapper) {
        return mapper.map(first());
    }

    public boolean hasError() {
        return error != null;
    }

    public DbException getError() {
        return error;
    }
}
