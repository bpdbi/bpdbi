package io.djb.mysql.impl.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jspecify.annotations.NonNull;

/**
 * MySQL caching_sha2_password authentication. XOR(SHA256(password),
 * SHA256(SHA256(SHA256(password)), nonce))
 */
public final class CachingSha2Authenticator {

  private CachingSha2Authenticator() {}

  public static byte @NonNull [] encode(byte @NonNull [] password, byte @NonNull [] nonce) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    byte[] passwordHash1 = md.digest(password);
    md.reset();
    byte[] passwordHash2 = md.digest(passwordHash1);
    md.reset();

    md.update(passwordHash2);
    byte[] passwordDigest = md.digest(nonce);

    for (int i = 0; i < passwordHash1.length; i++) {
      passwordHash1[i] ^= passwordDigest[i];
    }
    return passwordHash1;
  }
}
