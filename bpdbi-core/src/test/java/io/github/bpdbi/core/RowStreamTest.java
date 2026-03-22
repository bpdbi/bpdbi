package io.github.bpdbi.core;

import static io.github.bpdbi.core.test.TestRows.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Unit tests for RowStream. No database required. */
class RowStreamTest {

  private static Row makeRow(String value) {
    ColumnDescriptor[] cols = {col("v")};
    byte[][] vals = {value.getBytes(StandardCharsets.UTF_8)};
    return new Row(cols, vals, null, null);
  }

  @Test
  void forEachConsumesAllRows() {
    var rows = new Row[] {makeRow("a"), makeRow("b"), makeRow("c")};
    var idx = new AtomicInteger(0);
    var stream =
        new RowStream(() -> idx.get() < rows.length ? rows[idx.getAndIncrement()] : null, () -> {});

    var collected = new ArrayList<String>();
    stream.forEach(row -> collected.add(row.getString(0)));
    assertEquals(3, collected.size());
    assertEquals("a", collected.get(0));
    assertEquals("c", collected.get(2));
  }

  @Test
  void iteratorWalksRows() {
    var rows = new Row[] {makeRow("x"), makeRow("y")};
    var idx = new AtomicInteger(0);
    var stream =
        new RowStream(() -> idx.get() < rows.length ? rows[idx.getAndIncrement()] : null, () -> {});

    var iter = stream.iterator();
    assertEquals("x", iter.next().getString(0));
    assertEquals("y", iter.next().getString(0));
    assertFalse(iter.hasNext());
  }

  @Test
  void iteratorNextOnExhaustedThrowsNoSuchElement() {
    var stream = new RowStream(() -> null, () -> {});
    var iter = stream.iterator();
    assertFalse(iter.hasNext());
    assertThrows(NoSuchElementException.class, iter::next);
  }

  @Test
  void streamReturnsJavaStream() {
    var rows = new Row[] {makeRow("1"), makeRow("2"), makeRow("3")};
    var idx = new AtomicInteger(0);
    var stream =
        new RowStream(() -> idx.get() < rows.length ? rows[idx.getAndIncrement()] : null, () -> {});

    assertEquals(3, stream.stream().count());
  }

  @Test
  void closeCallsOnCloseOnce() {
    var closeCount = new AtomicInteger(0);
    var stream = new RowStream(() -> null, closeCount::incrementAndGet);

    stream.close();
    assertEquals(1, closeCount.get());
  }

  @Test
  void doubleCloseCallsOnCloseOnce() {
    var closeCount = new AtomicInteger(0);
    var stream = new RowStream(() -> null, closeCount::incrementAndGet);

    stream.close();
    stream.close();
    assertEquals(1, closeCount.get());
  }

  @Test
  void iteratorAfterCloseReturnsEmpty() {
    var stream = new RowStream(() -> makeRow("should not see"), () -> {});
    stream.close();
    var iter = stream.iterator();
    assertFalse(iter.hasNext());
  }
}
