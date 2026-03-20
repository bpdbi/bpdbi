package io.github.bpdbi.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;

/** A Postgres cidr network address (always has a netmask). */
public record Cidr(@NonNull InetAddress address, @NonNull Integer netmask) {

  @Override
  public @NonNull String toString() {
    return address.getHostAddress() + "/" + netmask;
  }
}
