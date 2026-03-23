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

  /** Used by tests to verify encoded values. No production callers. */
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

  /** Used by tests to verify encoded values. No production callers. */
  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  /** Used by tests to verify encoded values. No production callers. */
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

  /** Used by tests to verify encoded values at specific offsets. */
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
    writeInt(Float.floatToRawIntBits(value));
  }

  public void writeDouble(double value) {
    writeLong(Double.doubleToRawLongBits(value));
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
    writeStringUtf8(s);
    writeByte(0);
  }

  /**
   * Write a pre-encoded null-terminated C string. The input must already include the trailing null
   * byte. Avoids the String → byte[] conversion of {@link #writeCString(String)}.
   */
  public void writeCStringBytes(byte @NonNull [] cstring) {
    ensureCapacity(cstring.length);
    System.arraycopy(cstring, 0, data, writerIndex, cstring.length);
    writerIndex += cstring.length;
  }

  public void writeString(@NonNull String s) {
    writeStringUtf8(s);
  }

  public void setInt(int index, int value) {
    data[index] = (byte) (value >>> 24);
    data[index + 1] = (byte) (value >>> 16);
    data[index + 2] = (byte) (value >>> 8);
    data[index + 3] = (byte) value;
  }

  public void setShort(int index, int value) {
    data[index] = (byte) (value >>> 8);
    data[index + 1] = (byte) value;
  }

  /** Advance the writer index by {@code bytes} without writing. Used to reserve space. */
  public void skip(int bytes) {
    ensureCapacity(bytes);
    writerIndex += bytes;
  }

  /** Encode a String as UTF-8 directly into the buffer. Returns the number of bytes written. */
  public int writeStringUtf8(@NonNull String s) {
    int len = s.length();
    // Fast path: ASCII-only strings (very common for SQL values)
    ensureCapacity(len * 3); // worst case UTF-8 expansion
    int start = writerIndex;
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      if (c < 0x80) {
        data[writerIndex++] = (byte) c;
      } else if (c < 0x800) {
        data[writerIndex++] = (byte) (0xC0 | (c >>> 6));
        data[writerIndex++] = (byte) (0x80 | (c & 0x3F));
      } else if (Character.isHighSurrogate(c) && i + 1 < len) {
        char low = s.charAt(++i);
        int cp = Character.toCodePoint(c, low);
        data[writerIndex++] = (byte) (0xF0 | (cp >>> 18));
        data[writerIndex++] = (byte) (0x80 | ((cp >>> 12) & 0x3F));
        data[writerIndex++] = (byte) (0x80 | ((cp >>> 6) & 0x3F));
        data[writerIndex++] = (byte) (0x80 | (cp & 0x3F));
      } else {
        data[writerIndex++] = (byte) (0xE0 | (c >>> 12));
        data[writerIndex++] = (byte) (0x80 | ((c >>> 6) & 0x3F));
        data[writerIndex++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return writerIndex - start;
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
