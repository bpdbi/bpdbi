package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/**
 * Postgres 'bit' or 'varbit' (bit string) value. Stores a fixed number of bits backed by a byte
 * array.
 */
public record BitString(int bitCount, byte @NonNull [] bytes) {

  public BitString {
    int expectedBytes = (bitCount + 7) / 8;
    if (bytes.length != expectedBytes) {
      throw new IllegalArgumentException(
          "Expected " + expectedBytes + " bytes for " + bitCount + " bits, got " + bytes.length);
    }
  }

  /** Get the bit at the given index (0-based, MSB-first). */
  public boolean get(int index) {
    if (index < 0 || index >= bitCount) {
      throw new IndexOutOfBoundsException(
          "Bit index " + index + " out of range [0, " + bitCount + ")");
    }
    return (bytes[index / 8] & (0x80 >>> (index % 8))) != 0;
  }

  @Override
  public @NonNull String toString() {
    StringBuilder sb = new StringBuilder(bitCount);
    for (int i = 0; i < bitCount; i++) {
      sb.append(get(i) ? '1' : '0');
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof BitString b
        && bitCount == b.bitCount
        && java.util.Arrays.equals(bytes, b.bytes);
  }

  @Override
  public int hashCode() {
    return 31 * bitCount + java.util.Arrays.hashCode(bytes);
  }
}
