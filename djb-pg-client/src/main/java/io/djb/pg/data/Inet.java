package io.djb.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** A Postgres inet network address. */
public record Inet(@NonNull InetAddress address, @Nullable Integer netmask) {

  public static @NonNull Inet parse(@NonNull String s) {
    Object[] parsed = ParserHelpers.parseInetAddress(s, "inet");
    return new Inet((InetAddress) parsed[0], (Integer) parsed[1]);
  }

  @Override
  public @NonNull String toString() {
    return ParserHelpers.formatInetAddress(address, netmask);
  }
}
