package io.djb.pg.impl.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class MD5AuthenticationTest {

  @Test
  void encode() {
    assertEquals(
        "md54cd35160716308e3e571bbba12bb7591",
        MD5Authentication.encode(
            "scott", "tiger", "salt'n'pepper".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void encodeWithEmptySalt() {
    // Verify it doesn't crash with an empty salt
    String result = MD5Authentication.encode("user", "pass", new byte[4]);
    assertEquals("md5", result.substring(0, 3));
    assertEquals(35, result.length()); // "md5" + 32 hex chars
  }
}
