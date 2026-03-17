package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ColumnBufferTest {

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  // ===== Basic append and get =====

  @Test
  void appendAndGet() {
    var buf = new ColumnBuffer(4, 16);
    buf.append(bytes("hello"));
    buf.append(bytes("world"));
    assertEquals(2, buf.rowCount());
    assertArrayEquals(bytes("hello"), buf.get(0));
    assertArrayEquals(bytes("world"), buf.get(1));
  }

  @Test
  void appendNull() {
    var buf = new ColumnBuffer(4, 16);
    buf.append(null);
    assertEquals(1, buf.rowCount());
    assertNull(buf.get(0));
    assertTrue(buf.isNull(0));
  }

  @Test
  void mixedNullAndNonNull() {
    var buf = new ColumnBuffer(4, 16);
    buf.append(bytes("first"));
    buf.append(null);
    buf.append(bytes("third"));
    assertEquals(3, buf.rowCount());
    assertArrayEquals(bytes("first"), buf.get(0));
    assertFalse(buf.isNull(0));
    assertNull(buf.get(1));
    assertTrue(buf.isNull(1));
    assertArrayEquals(bytes("third"), buf.get(2));
    assertFalse(buf.isNull(2));
  }

  @Test
  void emptyByteArray() {
    var buf = new ColumnBuffer(4, 16);
    buf.append(new byte[0]);
    assertEquals(1, buf.rowCount());
    assertFalse(buf.isNull(0));
    assertArrayEquals(new byte[0], buf.get(0));
  }

  // ===== Growth / resizing =====

  @Test
  void growsBeyondInitialCapacity() {
    var buf = new ColumnBuffer(2, 4); // very small initial
    for (int i = 0; i < 100; i++) {
      buf.append(bytes("row" + i));
    }
    assertEquals(100, buf.rowCount());
    assertArrayEquals(bytes("row0"), buf.get(0));
    assertArrayEquals(bytes("row50"), buf.get(50));
    assertArrayEquals(bytes("row99"), buf.get(99));
  }

  @Test
  void growsDataBufferForLargeValues() {
    var buf = new ColumnBuffer(4, 2); // tiny data buffer
    byte[] largeValue = new byte[1000];
    for (int i = 0; i < largeValue.length; i++) {
      largeValue[i] = (byte) (i % 256);
    }
    buf.append(largeValue);
    assertArrayEquals(largeValue, buf.get(0));
  }

  // ===== Returned array is a copy =====

  @Test
  void getReturnsCopy() {
    var buf = new ColumnBuffer(4, 16);
    buf.append(bytes("original"));
    byte[] first = buf.get(0);
    byte[] second = buf.get(0);
    assertNotSame(first, second);
    assertArrayEquals(first, second);
    // Modifying the returned copy should not affect the buffer
    Assertions.assertNotNull(first);
    first[0] = 'X';
    assertArrayEquals(bytes("original"), buf.get(0));
  }

  // ===== Empty buffer =====

  @Test
  void emptyBuffer() {
    var buf = new ColumnBuffer(4, 16);
    assertEquals(0, buf.rowCount());
  }

  // ===== All nulls =====

  @Test
  void allNulls() {
    var buf = new ColumnBuffer(4, 16);
    for (int i = 0; i < 50; i++) {
      buf.append(null);
    }
    assertEquals(50, buf.rowCount());
    for (int i = 0; i < 50; i++) {
      assertTrue(buf.isNull(i));
      assertNull(buf.get(i));
    }
  }

  // ===== Stress: many rows =====

  @Test
  void manyRows() {
    var buf = new ColumnBuffer(8, 8);
    int count = 10_000;
    for (int i = 0; i < count; i++) {
      if (i % 7 == 0) {
        buf.append(null);
      } else {
        buf.append(bytes(String.valueOf(i)));
      }
    }
    assertEquals(count, buf.rowCount());
    // Verify a sample
    assertTrue(buf.isNull(0));
    assertArrayEquals(bytes("1"), buf.get(1));
    assertTrue(buf.isNull(7));
    assertArrayEquals(bytes("9999"), buf.get(9999));
  }

  // ===== Zero initial capacity =====

  @Test
  void zeroInitialRows() {
    var buf = new ColumnBuffer(0, 0);
    buf.append(bytes("works"));
    assertEquals(1, buf.rowCount());
    assertArrayEquals(bytes("works"), buf.get(0));
  }
}
