package io.github.bpdbi.core.impl;

import java.io.IOException;
import java.io.InputStream;
import org.jspecify.annotations.NonNull;

/**
 * An unsynchronized buffered input stream. Unlike {@link java.io.BufferedInputStream}, this does
 * not acquire a lock on every read, avoiding contention overhead. Safe because each database
 * connection is used by a single thread.
 *
 * <p>Also optimized for large reads: data larger than the buffer is read directly from the
 * underlying stream, bypassing the buffer to avoid an extra copy.
 *
 * <p>Provides a {@link #peek()} method to examine the next byte without consuming it.
 */
public final class UnsyncBufferedInputStream extends InputStream {

  private final InputStream in;
  private final byte[] buf;
  private int pos;
  private int count;

  public UnsyncBufferedInputStream(@NonNull InputStream in, int size) {
    this.in = in;
    this.buf = new byte[size];
  }

  @Override
  public int read() throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    return buf[pos++] & 0xFF;
  }

  @Override
  public int read(byte @NonNull [] data, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }

    // If data is available in the buffer, serve from it
    int avail = count - pos;
    if (avail > 0) {
      int toRead = Math.min(avail, len);
      System.arraycopy(buf, pos, data, off, toRead);
      pos += toRead;
      return toRead;
    }

    // Buffer empty. Large reads bypass the buffer entirely.
    if (len >= buf.length) {
      return in.read(data, off, len);
    }

    // Refill buffer, then copy
    fill();
    avail = count - pos;
    if (avail <= 0) {
      return -1;
    }
    int toRead = Math.min(avail, len);
    System.arraycopy(buf, pos, data, off, toRead);
    pos += toRead;
    return toRead;
  }

  /**
   * Examine the next byte without consuming it. Returns -1 at end of stream.
   *
   * @return the next byte (0-255), or -1 if EOF
   */
  public int peek() throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    return buf[pos] & 0xFF;
  }

  @Override
  public int available() throws IOException {
    int avail = count - pos;
    return avail + in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  private void fill() throws IOException {
    pos = 0;
    count = 0;
    int n = in.read(buf, 0, buf.length);
    if (n > 0) {
      count = n;
    }
  }
}
