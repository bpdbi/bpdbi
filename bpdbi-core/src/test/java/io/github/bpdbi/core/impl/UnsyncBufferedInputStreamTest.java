package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class UnsyncBufferedInputStreamTest {

  @Test
  void singleByteReads() throws IOException {
    byte[] data = {1, 2, 3, 4, 5};
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(data), 4);
    for (byte b : data) {
      assertEquals(b & 0xFF, ubis.read());
    }
    assertEquals(-1, ubis.read());
  }

  @Test
  void bulkReadFitsInBuffer() throws IOException {
    byte[] data = {10, 20, 30, 40, 50};
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(data), 16);
    byte[] out = new byte[5];
    int read = ubis.read(out, 0, 5);
    assertEquals(5, read);
    assertArrayEquals(data, out);
  }

  @Test
  void bulkReadLargerThanBufferBypassesBuffer() throws IOException {
    byte[] data = new byte[256];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    // Buffer size 8, read 256 bytes -> should bypass buffer
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(data), 8);
    byte[] out = new byte[256];
    int totalRead = 0;
    while (totalRead < 256) {
      int n = ubis.read(out, totalRead, 256 - totalRead);
      if (n == -1) break;
      totalRead += n;
    }
    assertEquals(256, totalRead);
    assertArrayEquals(data, out);
  }

  @Test
  void peekDoesNotConsume() throws IOException {
    byte[] data = {42, 99};
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(data), 4);
    assertEquals(42, ubis.peek());
    assertEquals(42, ubis.peek()); // still 42
    assertEquals(42, ubis.read()); // now consume
    assertEquals(99, ubis.peek());
    assertEquals(99, ubis.read());
    assertEquals(-1, ubis.peek());
    assertEquals(-1, ubis.read());
  }

  @Test
  void mixedReadsPreserveOrder() throws IOException {
    byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(data), 4);
    assertEquals(1, ubis.read()); // single byte
    byte[] bulk = new byte[3];
    int n = ubis.read(bulk, 0, 3);
    assertEquals(3, n);
    assertArrayEquals(new byte[] {2, 3, 4}, bulk);
    assertEquals(5, ubis.peek());
    assertEquals(5, ubis.read());
    // Read remaining
    byte[] rest = new byte[5];
    int total = 0;
    while (total < 5) {
      int r = ubis.read(rest, total, 5 - total);
      if (r == -1) break;
      total += r;
    }
    assertEquals(5, total);
    assertArrayEquals(new byte[] {6, 7, 8, 9, 10}, rest);
  }

  @Test
  void readReturnsMinusOneAtEOF() throws IOException {
    var ubis = new UnsyncBufferedInputStream(new ByteArrayInputStream(new byte[0]), 4);
    assertEquals(-1, ubis.read());
    byte[] buf = new byte[4];
    assertEquals(-1, ubis.read(buf, 0, 4));
  }

  @Test
  void closeClosesUnderlying() throws IOException {
    var closed = new boolean[] {false};
    InputStream underlying =
        new ByteArrayInputStream(new byte[] {1}) {
          @Override
          public void close() throws IOException {
            closed[0] = true;
            super.close();
          }
        };
    var ubis = new UnsyncBufferedInputStream(underlying, 4);
    ubis.close();
    assertTrue(closed[0]);
  }
}
