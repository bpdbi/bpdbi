package io.djb.mysql.impl.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MySQL native authentication (mysql_native_password).
 * SHA1(password) XOR SHA1(nonce + SHA1(SHA1(password)))
 */
public final class Native41Authenticator {

    private Native41Authenticator() {}

    public static byte[] encode(byte[] password, byte[] nonce) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        byte[] passwordHash1 = md.digest(password);
        md.reset();
        byte[] passwordHash2 = md.digest(passwordHash1);
        md.reset();

        md.update(nonce);
        md.update(passwordHash2);
        byte[] passwordHash3 = md.digest();

        for (int i = 0; i < passwordHash1.length; i++) {
            passwordHash1[i] ^= passwordHash3[i];
        }
        return passwordHash1;
    }
}
