package io.djb.mysql.impl.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Ported from vertx-mysql-client CachingSha2AuthenticatorTest. */
class CachingSha2AuthenticatorTest {

  private static final byte[] PASSWORD = "password".getBytes();

  private static String toHex(byte[] bytes) {
    var sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xFF));
    }
    return sb.toString();
  }

  @Test
  void testEncode() {
    byte[] nonce = {
      0x36, 0x47, 0x48, 0x05, 0x03, 0x6c, 0x6e, 0x60, 0x54, 0x73, 0x50, 0x61, 0x2b, 0x1b, 0x12,
      0x04, 0x6e, 0x5a, 0x79, 0x60
    };
    assertEquals(
        "83ad20dc9b0c61f959fb93f451a42fc2d15ab811b882d667fe8f0fcd0a8acaeb",
        toHex(CachingSha2Authenticator.encode(PASSWORD, nonce)));
  }

  @Test
  void testEncode2() {
    byte[] nonce = {
      0x6f, 0x2d, 0x5c, 0x6d, 0x66, 0x04, 0x10, 0x70, 0x2b, 0x71, 0x37, 0x76, 0x63, 0x39, 0x2a,
      0x02, 0x6d, 0x4a, 0x25, 0x47
    };
    assertEquals(
        "3d58c194ffdaf1f80a6c5ef67700608b07f9a29d37c9e2307aaf690c6e5526ab",
        toHex(CachingSha2Authenticator.encode(PASSWORD, nonce)));
  }
}
