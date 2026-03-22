package io.github.bpdbi.core.impl;

import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.NonNull;

/**
 * A {@link CharSequence} view over a region of a byte array, avoiding the copy that {@code new
 * String(buf, off, len, UTF_8)} would require.
 *
 * <p>For ASCII-only content (the common case for SQL text data like numbers, dates, identifiers),
 * {@link #charAt(int)} and {@link #length()} operate directly on the raw bytes with zero
 * allocation. For content containing multi-byte UTF-8 sequences, a full {@link String} is decoded
 * lazily on first access and reused thereafter.
 *
 * <p>Callers must not modify the backing byte array while this sequence is alive.
 */
public final class ByteCharSequence implements CharSequence {

  private final byte[] buf;
  private final int off;
  private final int len;
  private final boolean ascii;
  // Lazily decoded full String for non-ASCII content. Once set, all methods use this cached value.
  private String decoded;

  public ByteCharSequence(byte[] buf, int off, int len) {
    this.buf = buf;
    this.off = off;
    this.len = len;
    this.ascii = isAscii(buf, off, len);
  }

  private static boolean isAscii(byte[] buf, int off, int len) {
    for (int i = off, end = off + len; i < end; i++) {
      if (buf[i] < 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int length() {
    if (ascii) {
      return len;
    }
    // Non-ASCII: decode to String (cached) and delegate — multi-byte UTF-8 chars mean
    // byte count != char count, so we must decode to get the correct character length.
    return toString().length();
  }

  @Override
  public char charAt(int index) {
    if (ascii) {
      if (index < 0 || index >= len) {
        throw new IndexOutOfBoundsException("index " + index + ", length " + len);
      }
      return (char) buf[off + index];
    }
    // Non-ASCII: delegate to the cached decoded String
    return toString().charAt(index);
  }

  @Override
  public @NonNull CharSequence subSequence(int start, int end) {
    if (ascii) {
      return new ByteCharSequence(buf, off + start, end - start);
    }
    // Non-ASCII: delegate to the cached decoded String
    return toString().subSequence(start, end);
  }

  @Override
  public @NonNull String toString() {
    if (decoded == null) {
      decoded = new String(buf, off, len, StandardCharsets.UTF_8);
    }
    return decoded;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    // CharSequence does not define equals, but String.contentEquals(CharSequence) is the
    // idiomatic comparison. Implement equals against String for convenience.
    if (obj instanceof String s) {
      return s.contentEquals(this);
    }
    if (obj instanceof ByteCharSequence other) {
      if (length() != other.length()) {
        return false;
      }
      if (ascii && other.ascii) {
        for (int i = 0; i < len; i++) {
          if (buf[off + i] != other.buf[other.off + i]) {
            return false;
          }
        }
        return true;
      }
      return toString().equals(other.toString());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
