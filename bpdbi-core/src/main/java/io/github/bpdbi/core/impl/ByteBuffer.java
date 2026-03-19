package io.github.bpdbi.core.impl;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.jspecify.annotations.NonNull;

/**
 * Growable byte buffer with separate read/write cursors. Used by protocol encoders/decoders across
 * all database drivers.
 */
public final class ByteBuffer {

  private byte[] data;
  private int readerIndex;
  private int writerIndex;

  public ByteBuffer(int initialCapacity) {
    this.data = new byte[initialCapacity];
  }

  private ByteBuffer(byte[] data, int readerIndex, int writerIndex) {
    this.data = data;
    this.readerIndex = readerIndex;
    this.writerIndex = writerIndex;
  }

  public static @NonNull ByteBuffer wrap(byte @NonNull [] data) {
    return new ByteBuffer(data, 0, data.length);
  }

  public static @NonNull ByteBuffer wrap(byte @NonNull [] data, int offset, int length) {
    return new ByteBuffer(data, offset, offset + length);
  }

  // --- Read operations ---

  public int readableBytes() {
    return writerIndex - readerIndex;
  }

  public int readerIndex() {
    return readerIndex;
  }

  public void readerIndex(int index) {
    this.readerIndex = index;
  }

  public int writerIndex() {
    return writerIndex;
  }

  public byte readByte() {
    return data[readerIndex++];
  }

  public int readUnsignedByte() {
    return data[readerIndex++] & 0xFF;
  }

  public short readShort() {
    short v = (short) ((data[readerIndex] & 0xFF) << 8 | (data[readerIndex + 1] & 0xFF));
    readerIndex += 2;
    return v;
  }

  public int readUnsignedShort() {
    return readShort() & 0xFFFF;
  }

  public int readInt() {
    int v =
        (data[readerIndex] & 0xFF) << 24
            | (data[readerIndex + 1] & 0xFF) << 16
            | (data[readerIndex + 2] & 0xFF) << 8
            | (data[readerIndex + 3] & 0xFF);
    readerIndex += 4;
    return v;
  }

  public long readLong() {
    long v =
        (long) (data[readerIndex] & 0xFF) << 56
            | (long) (data[readerIndex + 1] & 0xFF) << 48
            | (long) (data[readerIndex + 2] & 0xFF) << 40
            | (long) (data[readerIndex + 3] & 0xFF) << 32
            | (long) (data[readerIndex + 4] & 0xFF) << 24
            | (long) (data[readerIndex + 5] & 0xFF) << 16
            | (long) (data[readerIndex + 6] & 0xFF) << 8
            | (long) (data[readerIndex + 7] & 0xFF);
    readerIndex += 8;
    return v;
  }

  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  public void readBytes(byte @NonNull [] dst) {
    readBytes(dst, 0, dst.length);
  }

  public void readBytes(byte @NonNull [] dst, int offset, int length) {
    System.arraycopy(data, readerIndex, dst, offset, length);
    readerIndex += length;
  }

  public void skipBytes(int count) {
    readerIndex += count;
  }

  public byte getByte(int index) {
    return data[index];
  }

  public int getInt(int index) {
    return (data[index] & 0xFF) << 24
        | (data[index + 1] & 0xFF) << 16
        | (data[index + 2] & 0xFF) << 8
        | (data[index + 3] & 0xFF);
  }

  public @NonNull String readCString() {
    int start = readerIndex;
    while (data[readerIndex] != 0) {
      readerIndex++;
    }
    String s = new String(data, start, readerIndex - start, StandardCharsets.UTF_8);
    readerIndex++; // skip null terminator
    return s;
  }

  // --- Write operations ---

  private void ensureCapacity(int additional) {
    int required = writerIndex + additional;
    if (required > data.length) {
      int newCap = Math.max(data.length * 2, required);
      data = Arrays.copyOf(data, newCap);
    }
  }

  public void writeByte(int value) {
    ensureCapacity(1);
    data[writerIndex++] = (byte) value;
  }

  public void writeShort(int value) {
    ensureCapacity(2);
    data[writerIndex++] = (byte) (value >>> 8);
    data[writerIndex++] = (byte) value;
  }

  public void writeInt(int value) {
    ensureCapacity(4);
    data[writerIndex++] = (byte) (value >>> 24);
    data[writerIndex++] = (byte) (value >>> 16);
    data[writerIndex++] = (byte) (value >>> 8);
    data[writerIndex++] = (byte) value;
  }

  public void writeLong(long value) {
    ensureCapacity(8);
    data[writerIndex++] = (byte) (value >>> 56);
    data[writerIndex++] = (byte) (value >>> 48);
    data[writerIndex++] = (byte) (value >>> 40);
    data[writerIndex++] = (byte) (value >>> 32);
    data[writerIndex++] = (byte) (value >>> 24);
    data[writerIndex++] = (byte) (value >>> 16);
    data[writerIndex++] = (byte) (value >>> 8);
    data[writerIndex++] = (byte) value;
  }

  public void writeFloat(float value) {
    writeInt(Float.floatToIntBits(value));
  }

  public void writeDouble(double value) {
    writeLong(Double.doubleToLongBits(value));
  }

  public void writeBytes(byte @NonNull [] src) {
    writeBytes(src, 0, src.length);
  }

  public void writeBytes(byte @NonNull [] src, int offset, int length) {
    ensureCapacity(length);
    System.arraycopy(src, offset, data, writerIndex, length);
    writerIndex += length;
  }

  public void writeCString(@NonNull String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    ensureCapacity(bytes.length + 1);
    System.arraycopy(bytes, 0, data, writerIndex, bytes.length);
    writerIndex += bytes.length;
    data[writerIndex++] = 0;
  }

  public void writeString(@NonNull String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    writeBytes(bytes);
  }

  public void setInt(int index, int value) {
    data[index] = (byte) (value >>> 24);
    data[index + 1] = (byte) (value >>> 16);
    data[index + 2] = (byte) (value >>> 8);
    data[index + 3] = (byte) value;
  }

  /** Ensure the buffer can hold at least {@code additionalBytes} more bytes without resizing. */
  public void ensureWritable(int additionalBytes) {
    ensureCapacity(additionalBytes);
  }

  // --- Lifecycle ---

  public byte @NonNull [] toByteArray() {
    return Arrays.copyOfRange(data, 0, writerIndex);
  }

  public byte @NonNull [] array() {
    return data;
  }

  private static final int DEFAULT_CAPACITY = 1024;
  private static final int SHRINK_THRESHOLD = 64 * 1024; // 64KB

  public void clear() {
    readerIndex = 0;
    writerIndex = 0;
    // Shrink oversized buffers to avoid holding large allocations indefinitely
    if (data.length > SHRINK_THRESHOLD) {
      data = new byte[DEFAULT_CAPACITY];
    }
  }
}
