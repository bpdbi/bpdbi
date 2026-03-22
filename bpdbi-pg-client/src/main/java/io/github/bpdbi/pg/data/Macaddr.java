package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** Postgres 'macaddr' (6-byte MAC address). */
public record Macaddr(byte @NonNull [] address) {

  public Macaddr {
    if (address.length != 6) {
      throw new IllegalArgumentException("macaddr must be 6 bytes, got " + address.length);
    }
  }

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  @Override
  public @NonNull String toString() {
    char[] out = new char[17]; // xx:xx:xx:xx:xx:xx
    for (int i = 0; i < 6; i++) {
      int v = address[i] & 0xFF;
      int pos = i * 3;
      out[pos] = HEX[v >>> 4];
      out[pos + 1] = HEX[v & 0x0F];
      if (i < 5) out[pos + 2] = ':';
    }
    return new String(out);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Macaddr m && java.util.Arrays.equals(address, m.address);
  }

  @Override
  public int hashCode() {
    return java.util.Arrays.hashCode(address);
  }
}
