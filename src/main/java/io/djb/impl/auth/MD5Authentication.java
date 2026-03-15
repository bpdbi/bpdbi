package io.djb.impl.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * PostgreSQL MD5 password authentication.
 */
public final class MD5Authentication {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private MD5Authentication() {}

    private static String toHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hex[i * 2] = HEX[v >>> 4];
            hex[i * 2 + 1] = HEX[v & 0x0F];
        }
        return new String(hex);
    }

    public static String encode(String username, String password, byte[] salt) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(password.getBytes(UTF_8));
        md.update(username.getBytes(UTF_8));
        byte[] digest = md.digest();

        md.update(toHex(digest).getBytes(US_ASCII));
        md.update(salt);
        byte[] passDigest = md.digest();

        return "md5" + toHex(passDigest);
    }
}
