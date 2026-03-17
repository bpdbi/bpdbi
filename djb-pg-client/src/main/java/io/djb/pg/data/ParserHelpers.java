package io.djb.pg.data;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.NonNull;

class ParserHelpers {

  static @NonNull List<Point> parsePoints(String s) {
    List<Point> points = new ArrayList<>();
    // Split on ),( to find individual points
    int i = 0;
    while (i < s.length()) {
      int start = s.indexOf('(', i);
      if (start < 0) {
        break;
      }
      int end = s.indexOf(')', start);
      if (end < 0) {
        break;
      }
      points.add(Point.parse(s.substring(start, end + 1)));
      i = end + 1;
    }
    return points;
  }

  /**
   * Parse a Postgres inet/cidr address string into an InetAddress and optional netmask.
   *
   * @return a two-element array: [InetAddress, Integer (nullable netmask)]
   */
  static Object[] parseInetAddress(String s, String typeName) {
    s = s.trim();
    try {
      int slash = s.indexOf('/');
      if (slash >= 0) {
        InetAddress addr = InetAddress.getByName(s.substring(0, slash));
        int mask = Integer.parseInt(s.substring(slash + 1));
        return new Object[] {addr, mask};
      } else {
        return new Object[] {InetAddress.getByName(s), null};
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid " + typeName + " address: " + s, e);
    }
  }

  static @NonNull String formatInetAddress(InetAddress address, Integer netmask) {
    String s = address.getHostAddress();
    if (netmask != null) {
      s += "/" + netmask;
    }
    return s;
  }
}
