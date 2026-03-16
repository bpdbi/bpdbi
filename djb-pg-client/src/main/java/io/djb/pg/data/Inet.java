package io.djb.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;

/**
 * A Postgres inet network address.
 */
public record Inet(InetAddress address, Integer netmask) {

  public static Inet parse(String s) {
    Object[] parsed = ParserHelpers.parseInetAddress(s, "inet");
    return new Inet((InetAddress) parsed[0], (Integer) parsed[1]);
  }

  @Override
  public @NonNull String toString() {
    return ParserHelpers.formatInetAddress(address, netmask);
  }
}
