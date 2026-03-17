package io.github.bpdbi.mysql.impl.auth;

import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.Cipher;
import org.jspecify.annotations.NonNull;

/**
 * Encrypts a password with the server's RSA public key for caching_sha2_password full
 * authentication. The password is XOR'd with the nonce before encryption (MySQL protocol
 * requirement).
 *
 * <p>Ported from Vert.x SQL Client (Eclipse Public License 2.0 / Apache License 2.0).
 */
public final class RsaPublicKeyEncryptor {

  private RsaPublicKeyEncryptor() {}

  /**
   * Encrypt the NULL-terminated password with the nonce and RSA public key provided by the server.
   */
  public static byte @NonNull [] encrypt(
      byte @NonNull [] password, byte @NonNull [] nonce, @NonNull String serverRsaPublicKey) {
    try {
      RSAPublicKey rsaPublicKey = parseRsaPublicKey(serverRsaPublicKey);
      byte[] obfuscated = obfuscate(password, nonce);
      Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
      cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey);
      return cipher.doFinal(obfuscated);
    } catch (Exception e) {
      throw new RuntimeException("RSA encryption failed for caching_sha2_password full auth", e);
    }
  }

  private static RSAPublicKey parseRsaPublicKey(String serverRsaPublicKey) throws Exception {
    String content =
        serverRsaPublicKey
            .replace("-----BEGIN PUBLIC KEY-----", "")
            .replace("-----END PUBLIC KEY-----", "")
            .replaceAll("\\n", "");
    byte[] keyBytes =
        Base64.getDecoder().decode(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    return (RSAPublicKey) keyFactory.generatePublic(keySpec);
  }

  private static byte[] obfuscate(byte[] password, byte[] nonce) {
    byte[] result = new byte[password.length];
    for (int i = 0; i < password.length; i++) {
      result[i] = (byte) (password[i] ^ nonce[i % nonce.length]);
    }
    return result;
  }
}
