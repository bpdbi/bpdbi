package io.djb.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ByteBufferTest {

  @Test
  void writeAndReadByte() {
    var buf = new ByteBuffer(16);
    buf.writeByte(0x42);
    buf.writeByte(0xFF);
    assertEquals(2, buf.writerIndex());

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(0x42, wrapped.readByte());
    assertEquals((byte) 0xFF, wrapped.readByte());
  }

  @Test
  void writeAndReadShort() {
    var buf = new ByteBuffer(16);
    buf.writeShort(0x1234);
    buf.writeShort(-1);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(0x1234, wrapped.readUnsignedShort());
    assertEquals(-1, wrapped.readShort());
  }

  @Test
  void writeAndReadInt() {
    var buf = new ByteBuffer(16);
    buf.writeInt(0x12345678);
    buf.writeInt(-1);
    buf.writeInt(0);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(0x12345678, wrapped.readInt());
    assertEquals(-1, wrapped.readInt());
    assertEquals(0, wrapped.readInt());
  }

  @Test
  void writeAndReadLong() {
    var buf = new ByteBuffer(16);
    buf.writeLong(0x123456789ABCDEF0L);
    buf.writeLong(-1L);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(0x123456789ABCDEF0L, wrapped.readLong());
    assertEquals(-1L, wrapped.readLong());
  }

  @Test
  void writeAndReadFloat() {
    var buf = new ByteBuffer(16);
    buf.writeFloat(3.14f);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(3.14f, wrapped.readFloat(), 0.001f);
  }

  @Test
  void writeAndReadDouble() {
    var buf = new ByteBuffer(16);
    buf.writeDouble(3.14159265358979);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(3.14159265358979, wrapped.readDouble(), 1e-15);
  }

  @Test
  void writeAndReadBytes() {
    var buf = new ByteBuffer(16);
    byte[] src = {1, 2, 3, 4, 5};
    buf.writeBytes(src);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    byte[] dst = new byte[5];
    wrapped.readBytes(dst);
    assertArrayEquals(src, dst);
  }

  @Test
  void writeCStringAndReadCString() {
    var buf = new ByteBuffer(64);
    buf.writeCString("hello");
    buf.writeCString("world");
    buf.writeCString(""); // empty string

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals("hello", wrapped.readCString());
    assertEquals("world", wrapped.readCString());
    assertEquals("", wrapped.readCString());
  }

  @Test
  void writeCStringUTF8() {
    var buf = new ByteBuffer(64);
    buf.writeCString("héllo wörld"); // non-ASCII

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals("héllo wörld", wrapped.readCString());
  }

  @Test
  void writeString() {
    var buf = new ByteBuffer(64);
    buf.writeString("hello");

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(5, wrapped.readableBytes());
    byte[] dst = new byte[5];
    wrapped.readBytes(dst);
    assertEquals("hello", new String(dst, StandardCharsets.UTF_8));
  }

  @Test
  void setInt() {
    var buf = new ByteBuffer(16);
    buf.writeInt(0); // placeholder at position 0
    buf.writeInt(42); // data at position 4
    buf.setInt(0, 99); // backpatch

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(99, wrapped.readInt());
    assertEquals(42, wrapped.readInt());
  }

  @Test
  void getIntWithoutAdvancing() {
    var buf = new ByteBuffer(16);
    buf.writeInt(0x12345678);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(0x12345678, wrapped.getInt(0));
    // readerIndex should not have advanced
    assertEquals(0, wrapped.readerIndex());
    assertEquals(0x12345678, wrapped.readInt()); // now advance
    assertEquals(4, wrapped.readerIndex());
  }

  @Test
  void readableBytes() {
    var buf = new ByteBuffer(16);
    buf.writeInt(1);
    buf.writeInt(2);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(8, wrapped.readableBytes());
    wrapped.readInt();
    assertEquals(4, wrapped.readableBytes());
    wrapped.readInt();
    assertEquals(0, wrapped.readableBytes());
  }

  @Test
  void skipBytes() {
    var buf = new ByteBuffer(16);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(3);

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    wrapped.skipBytes(4);
    assertEquals(2, wrapped.readInt());
    wrapped.skipBytes(4);
    assertEquals(0, wrapped.readableBytes());
  }

  @Test
  void growsAutomatically() {
    var buf = new ByteBuffer(4); // tiny initial capacity
    for (int i = 0; i < 100; i++) {
      buf.writeInt(i);
    }
    assertEquals(400, buf.writerIndex());

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    for (int i = 0; i < 100; i++) {
      assertEquals(i, wrapped.readInt());
    }
  }

  @Test
  void clear() {
    var buf = new ByteBuffer(16);
    buf.writeInt(42);
    assertEquals(4, buf.writerIndex());

    buf.clear();
    assertEquals(0, buf.writerIndex());
    assertEquals(0, buf.readerIndex());

    buf.writeInt(99);
    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(99, wrapped.readInt());
  }

  @Test
  void wrapWithOffsetAndLength() {
    byte[] data = {0, 0, 0, 42, 0, 0, 0, 99, 0, 0};
    var buf = ByteBuffer.wrap(data, 0, 8);
    assertEquals(8, buf.readableBytes());
    assertEquals(42, buf.readInt());
    assertEquals(99, buf.readInt());
  }

  @Test
  void readUnsignedByte() {
    var buf = new ByteBuffer(4);
    buf.writeByte(200); // > 127, unsigned

    var wrapped = ByteBuffer.wrap(buf.toByteArray());
    assertEquals(200, wrapped.readUnsignedByte());
  }
}
