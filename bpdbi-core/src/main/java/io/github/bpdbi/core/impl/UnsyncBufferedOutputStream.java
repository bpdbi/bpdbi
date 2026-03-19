package io.github.bpdbi.core.impl;

import java.io.IOException;
import java.io.OutputStream;
import org.jspecify.annotations.NonNull;

/**
 * An unsynchronized buffered output stream. Unlike {@link java.io.BufferedOutputStream}, this does
 * not acquire a lock on every write, avoiding contention overhead. Safe because each database
 * connection is used by a single thread.
 *
 * <p>Also optimized for large writes: data larger than the buffer is written directly to the
 * underlying stream, bypassing the buffer to avoid an extra copy.
 */
public final class UnsyncBufferedOutputStream extends OutputStream {

  private final OutputStream out;
  private final byte[] buf;
  private int count;

  public UnsyncBufferedOutputStream(@NonNull OutputStream out, int size) {
    this.out = out;
    this.buf = new byte[size];
  }

  @Override
  public void write(int b) throws IOException {
    if (count >= buf.length) {
      flushBuffer();
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte @NonNull [] data, int off, int len) throws IOException {
    if (len >= buf.length) {
      // Large write: flush buffer, then write directly to underlying stream
      flushBuffer();
      out.write(data, off, len);
      return;
    }
    if (len > buf.length - count) {
      flushBuffer();
    }
    System.arraycopy(data, off, buf, count, len);
    count += len;
  }

  @Override
  public void flush() throws IOException {
    flushBuffer();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      out.close();
    }
  }

  private void flushBuffer() throws IOException {
    if (count > 0) {
      out.write(buf, 0, count);
      count = 0;
    }
  }
}
