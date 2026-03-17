package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres macaddr8 (8-byte EUI-64 MAC address). */
public record Macaddr8(byte @NonNull [] address) {

  public Macaddr8 {
    if (address.length != 8) {
      throw new IllegalArgumentException("macaddr8 must be 8 bytes, got " + address.length);
    }
  }

  @Override
  public @NonNull String toString() {
    return String.format(
        "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
        address[0] & 0xFF,
        address[1] & 0xFF,
        address[2] & 0xFF,
        address[3] & 0xFF,
        address[4] & 0xFF,
        address[5] & 0xFF,
        address[6] & 0xFF,
        address[7] & 0xFF);
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
