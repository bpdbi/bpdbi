package io.djb.pg.data;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A PostgreSQL inet network address.
 */
public record Inet(InetAddress address, Integer netmask) {

    public static Inet parse(String s) {
        s = s.trim();
        try {
            int slash = s.indexOf('/');
            if (slash >= 0) {
                InetAddress addr = InetAddress.getByName(s.substring(0, slash));
                int mask = Integer.parseInt(s.substring(slash + 1));
                return new Inet(addr, mask);
            } else {
                return new Inet(InetAddress.getByName(s), null);
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid inet address: " + s, e);
        }
    }

    @Override
    public String toString() {
        String s = address.getHostAddress();
        if (netmask != null) {
            s += "/" + netmask;
        }
        return s;
    }
}
