package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres macaddr (6-byte MAC address). */
public record Macaddr(byte @NonNull [] address) {

  public Macaddr {
    if (address.length != 6) {
      throw new IllegalArgumentException("macaddr must be 6 bytes, got " + address.length);
    }
  }

  @Override
  public @NonNull String toString() {
    return String.format(
        "%02x:%02x:%02x:%02x:%02x:%02x",
        address[0] & 0xFF,
        address[1] & 0xFF,
        address[2] & 0xFF,
        address[3] & 0xFF,
        address[4] & 0xFF,
        address[5] & 0xFF);
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
