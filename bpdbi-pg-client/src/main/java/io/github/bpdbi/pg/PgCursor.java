package io.github.bpdbi.pg;

import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.impl.ColumnBuffer;
import io.github.bpdbi.pg.impl.codec.BackendMessage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Postgres cursor implementation. Reads rows in batches via the portal Execute/Sync cycle.
 *
 * <p>Package-private — created by {@link PgConnection#cursor(String, Object...)}.
 */
final class PgCursor implements Cursor {

  private final PgConnection conn;
  private final String portalName;
  private final ColumnDescriptor[] columns;
  private boolean hasMore = true;
  private boolean closed = false;
  // Reusable column buffers — sized from the previous batch's observed data
  private ColumnBuffer @Nullable [] prevBuffers;

  PgCursor(PgConnection conn, String portalName, ColumnDescriptor[] columns) {
    this.conn = conn;
    this.portalName = portalName;
    this.columns = columns;
  }

  @Override
  public @NonNull RowSet read(int count) {
    if (closed) {
      throw new IllegalStateException("Cursor is closed");
    }
    if (!hasMore) {
      return new RowSet(List.of(), columns != null ? List.of(columns) : List.of(), 0);
    }
    // Adaptive fetch size: limit count so estimated batch bytes stay within budget
    int adjustedCount = count;
    int maxBytes = conn.maxResultBufferBytes();
    if (maxBytes > 0 && prevBuffers != null && prevBuffers.length > 0) {
      int avgRowSize = 0;
      for (ColumnBuffer b : prevBuffers) {
        avgRowSize += b.averageValueSize();
      }
      if (avgRowSize > 0) {
        adjustedCount = Math.max(1, Math.min(count, maxBytes / avgRowSize));
      }
    }
    try {
      var encoder = conn.encoder();
      OutputStream out = conn.out();
      encoder.writeExecute(portalName, adjustedCount);
      encoder.writeSync();
      encoder.flush(out);

      ColumnBuffer[] buffers = createOrReuseBuffers(count);
      int rowCount = 0;
      int rowsAffected = 0;
      while (true) {
        BackendMessage msg = conn.decoder().readMessage();
        switch (msg) {
          case BackendMessage.DataRow dr -> {
            PgConnection.appendToColumnBuffers(buffers, dr.values());
            rowCount++;
          }
          case BackendMessage.CommandComplete cc -> {
            rowsAffected = cc.rowsAffected();
            hasMore = false;
          }
          case BackendMessage.PortalSuspended ps -> hasMore = true;
          case BackendMessage.ErrorResponse err -> {
            conn.drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.ReadyForQuery rq -> {
            prevBuffers = buffers;
            return conn.buildCursorRowSet(columns, buffers, rowCount, rowsAffected);
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading cursor", e);
    }
  }

  private ColumnBuffer[] createOrReuseBuffers(int expectedRows) {
    if (columns == null) {
      return new ColumnBuffer[0];
    }
    ColumnBuffer[] prev = this.prevBuffers;
    if (prev != null && prev.length == columns.length) {
      // Reuse existing buffers — avoids reallocating backing arrays each batch
      for (ColumnBuffer b : prev) {
        b.reset();
      }
      return prev;
    }
    return PgConnection.createColumnBuffers(columns.length, expectedRows, 32);
  }

  @Override
  public boolean hasMore() {
    return hasMore && !closed;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      hasMore = false;
      try {
        var encoder = conn.encoder();
        encoder.writeClosePortal(portalName);
        encoder.writeSync();
        encoder.flush(conn.out());
        while (true) {
          BackendMessage msg = conn.decoder().readMessage();
          if (msg instanceof BackendMessage.ReadyForQuery) {
            break;
          }
        }
      } catch (IOException e) {
        // best effort
      }
    }
  }
}
