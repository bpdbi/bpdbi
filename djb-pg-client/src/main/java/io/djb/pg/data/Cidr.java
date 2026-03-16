package io.djb.pg.data;

import java.net.InetAddress;
import org.jspecify.annotations.NonNull;

/**
 * A Postgres cidr network address (always has a netmask).
 */
public record Cidr(InetAddress address, Integer netmask) {

  public static Cidr parse(String s) {
    Object[] parsed = ParserHelpers.parseInetAddress(s, "cidr");
    return new Cidr((InetAddress) parsed[0], (Integer) parsed[1]);
  }

  @Override
  public @NonNull String toString() {
    return ParserHelpers.formatInetAddress(address, netmask);
  }
}
