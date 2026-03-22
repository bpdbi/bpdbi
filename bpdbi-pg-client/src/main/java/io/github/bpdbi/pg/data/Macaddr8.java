package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** Postgres 'macaddr8' (8-byte EUI-64 MAC address). */
public record Macaddr8(byte @NonNull [] address) {

  public Macaddr8 {
    if (address.length != 8) {
      throw new IllegalArgumentException("macaddr8 must be 8 bytes, got " + address.length);
    }
  }

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  @Override
  public @NonNull String toString() {
    char[] out = new char[23]; // xx:xx:xx:xx:xx:xx:xx:xx
    for (int i = 0; i < 8; i++) {
      int v = address[i] & 0xFF;
      int pos = i * 3;
      out[pos] = HEX[v >>> 4];
      out[pos + 1] = HEX[v & 0x0F];
      if (i < 7) out[pos + 2] = ':';
    }
    return new String(out);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Macaddr8 m && java.util.Arrays.equals(address, m.address);
  }

  @Override
  public int hashCode() {
    return java.util.Arrays.hashCode(address);
  }
}
