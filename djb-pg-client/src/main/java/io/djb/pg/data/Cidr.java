package io.djb.pg.data;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A PostgreSQL cidr network address (always has a netmask).
 */
public record Cidr(InetAddress address, Integer netmask) {

    public static Cidr parse(String s) {
        s = s.trim();
        try {
            int slash = s.indexOf('/');
            if (slash >= 0) {
                InetAddress addr = InetAddress.getByName(s.substring(0, slash));
                int mask = Integer.parseInt(s.substring(slash + 1));
                return new Cidr(addr, mask);
            } else {
                return new Cidr(InetAddress.getByName(s), null);
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid cidr address: " + s, e);
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
