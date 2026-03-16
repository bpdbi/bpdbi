package io.djb.mysql.impl.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Ported from vertx-mysql-client Native41AuthenticatorTest.
 */
class Native41AuthenticatorTest {

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
        byte[] challenge = {
            0x18, 0x38, 0x55, 0x7e, 0x3a, 0x77, 0x65, 0x35,
            0x34, 0x1f, 0x44, 0x4a, 0x36, 0x60, 0x5d, 0x79, 0x5c, 0x09, 0x6c, 0x08
        };
        assertEquals("f2671df1862aed0340be809405b30bb93d29142d",
            toHex(Native41Authenticator.encode(PASSWORD, challenge)));
    }

    @Test
    void testEncode2() {
        byte[] challenge = {
            0x42, 0x0f, 0x34, 0x68, 0x6f, 0x77, 0x67, 0x18,
            0x14, 0x57, 0x3d, 0x04, 0x39, 0x70, 0x1f, 0x46, 0x58, 0x51, 0x49, 0x31
        };
        assertEquals("cc93fb6f68af2e9446dafc0a1667d015b0a49550",
            toHex(Native41Authenticator.encode(PASSWORD, challenge)));
    }
}
