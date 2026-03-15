package io.djb.impl.buffer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Growable byte buffer with separate read/write cursors.
 * Replaces Netty's ByteBuf for our blocking I/O use case.
 */
public final class PgBuffer {

    private byte[] data;
    private int readerIndex;
    private int writerIndex;

    public PgBuffer(int initialCapacity) {
        this.data = new byte[initialCapacity];
    }

    private PgBuffer(byte[] data, int readerIndex, int writerIndex) {
        this.data = data;
        this.readerIndex = readerIndex;
        this.writerIndex = writerIndex;
    }

    public static PgBuffer wrap(byte[] data) {
        return new PgBuffer(data, 0, data.length);
    }

    public static PgBuffer wrap(byte[] data, int offset, int length) {
        return new PgBuffer(data, offset, offset + length);
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
        int v = (data[readerIndex] & 0xFF) << 24
              | (data[readerIndex + 1] & 0xFF) << 16
              | (data[readerIndex + 2] & 0xFF) << 8
              | (data[readerIndex + 3] & 0xFF);
        readerIndex += 4;
        return v;
    }

    public long readLong() {
        long v = (long)(data[readerIndex] & 0xFF) << 56
               | (long)(data[readerIndex + 1] & 0xFF) << 48
               | (long)(data[readerIndex + 2] & 0xFF) << 40
               | (long)(data[readerIndex + 3] & 0xFF) << 32
               | (long)(data[readerIndex + 4] & 0xFF) << 24
               | (long)(data[readerIndex + 5] & 0xFF) << 16
               | (long)(data[readerIndex + 6] & 0xFF) << 8
               | (long)(data[readerIndex + 7] & 0xFF);
        readerIndex += 8;
        return v;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public void readBytes(byte[] dst) {
        readBytes(dst, 0, dst.length);
    }

    public void readBytes(byte[] dst, int offset, int length) {
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

    /**
     * Read a null-terminated UTF-8 string.
     */
    public String readCString() {
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

    public void writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
    }

    public void writeBytes(byte[] src, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(src, offset, data, writerIndex, length);
        writerIndex += length;
    }

    /**
     * Write a null-terminated UTF-8 string.
     */
    public void writeCString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ensureCapacity(bytes.length + 1);
        System.arraycopy(bytes, 0, data, writerIndex, bytes.length);
        writerIndex += bytes.length;
        data[writerIndex++] = 0;
    }

    /**
     * Write a UTF-8 string (no null terminator).
     */
    public void writeString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeBytes(bytes);
    }

    /**
     * Backpatch: set a 4-byte int at a specific position without advancing the writer.
     */
    public void setInt(int index, int value) {
        data[index] = (byte) (value >>> 24);
        data[index + 1] = (byte) (value >>> 16);
        data[index + 2] = (byte) (value >>> 8);
        data[index + 3] = (byte) value;
    }

    // --- Lifecycle ---

    /**
     * Returns the written data as a byte array (copies only the written portion).
     */
    public byte[] toByteArray() {
        return Arrays.copyOfRange(data, 0, writerIndex);
    }

    /**
     * Returns the backing array directly (avoid copy for writing to OutputStream).
     * Use with writerIndex() to know how many bytes are valid.
     */
    public byte[] array() {
        return data;
    }

    public void clear() {
        readerIndex = 0;
        writerIndex = 0;
    }
}
