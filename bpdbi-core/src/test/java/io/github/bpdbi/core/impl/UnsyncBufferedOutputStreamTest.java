package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class UnsyncBufferedOutputStreamTest {

  @Test
  void singleByteWrites() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 4);
    buf.write(1);
    buf.write(2);
    buf.write(3);
    // Not flushed yet — still in buffer
    assertEquals(0, target.size());
    buf.flush();
    assertArrayEquals(new byte[] {1, 2, 3}, target.toByteArray());
  }

  @Test
  void bufferFlushesWhenFull() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 4);
    buf.write(1);
    buf.write(2);
    buf.write(3);
    buf.write(4);
    // Buffer is full but not yet flushed to underlying
    buf.write(5); // triggers flush of first 4 bytes
    buf.flush();
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, target.toByteArray());
  }

  @Test
  void bulkWriteFitsInBuffer() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 16);
    buf.write(new byte[] {1, 2, 3, 4, 5}, 0, 5);
    assertEquals(0, target.size());
    buf.flush();
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, target.toByteArray());
  }

  @Test
  void bulkWriteLargerThanBufferBypassesBuffer() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 4);
    byte[] large = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    buf.write(large, 0, large.length);
    // Large write goes directly to underlying stream
    assertArrayEquals(large, target.toByteArray());
  }

  @Test
  void mixedWritesPreserveOrder() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 8);
    buf.write(0xAA);
    buf.write(new byte[] {0x01, 0x02, 0x03}, 0, 3);
    buf.write(0xBB);
    buf.flush();
    assertArrayEquals(
        new byte[] {(byte) 0xAA, 0x01, 0x02, 0x03, (byte) 0xBB}, target.toByteArray());
  }

  @Test
  void closeFlushesAndClosesUnderlying() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 16);
    buf.write(new byte[] {1, 2, 3}, 0, 3);
    buf.close();
    assertArrayEquals(new byte[] {1, 2, 3}, target.toByteArray());
  }

  @Test
  void bulkWriteWithOffsetAndLength() throws IOException {
    var target = new ByteArrayOutputStream();
    var buf = new UnsyncBufferedOutputStream(target, 16);
    byte[] data = new byte[] {0, 0, 1, 2, 3, 0, 0};
    buf.write(data, 2, 3);
    buf.flush();
    assertArrayEquals(new byte[] {1, 2, 3}, target.toByteArray());
  }
}
