package io.github.bpdbi.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** A Postgres inet network address. */
public record Inet(@NonNull InetAddress address, @Nullable Integer netmask) {

  @Override
  public @NonNull String toString() {
    String s = address.getHostAddress();
    return netmask != null ? s + "/" + netmask : s;
  }
}
