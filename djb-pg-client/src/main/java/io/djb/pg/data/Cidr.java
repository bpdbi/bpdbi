package io.djb.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** A Postgres cidr network address (always has a netmask). */
public record Cidr(@NonNull InetAddress address, @Nullable Integer netmask) {

  public static @NonNull Cidr parse(@NonNull String s) {
    Object[] parsed = ParserHelpers.parseInetAddress(s, "cidr");
    return new Cidr((InetAddress) parsed[0], (Integer) parsed[1]);
  }

  @Override
  public @NonNull String toString() {
    return ParserHelpers.formatInetAddress(address, netmask);
  }
}
