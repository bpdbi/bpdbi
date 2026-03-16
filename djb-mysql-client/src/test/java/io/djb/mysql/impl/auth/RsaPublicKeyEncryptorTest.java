package io.djb.mysql.impl.auth;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

import javax.crypto.Cipher;

import static org.junit.jupiter.api.Assertions.*;

class RsaPublicKeyEncryptorTest {

    @Test
    void encryptProducesDifferentOutputThanInput() throws Exception {
        KeyPair keyPair = generateKeyPair();
        String pemPublicKey = toPem(keyPair);

        byte[] password = "secret\0".getBytes();
        byte[] nonce = new byte[20];
        for (int i = 0; i < nonce.length; i++) nonce[i] = (byte) i;

        byte[] encrypted = RsaPublicKeyEncryptor.encrypt(password, nonce, pemPublicKey);
        assertNotNull(encrypted);
        assertTrue(encrypted.length > 0);
        // Encrypted output should be different from password
        assertNotEquals(password.length, encrypted.length);
    }

    @Test
    void encryptedDataCanBeDecrypted() throws Exception {
        KeyPair keyPair = generateKeyPair();
        String pemPublicKey = toPem(keyPair);

        byte[] password = "mypassword\0".getBytes();
        byte[] nonce = new byte[20];
        for (int i = 0; i < nonce.length; i++) nonce[i] = (byte) (i * 3);

        byte[] encrypted = RsaPublicKeyEncryptor.encrypt(password, nonce, pemPublicKey);

        // Decrypt with private key
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        byte[] decrypted = cipher.doFinal(encrypted);

        // Decrypted should be the XOR of password with nonce
        assertEquals(password.length, decrypted.length);
        for (int i = 0; i < password.length; i++) {
            assertEquals((byte) (password[i] ^ nonce[i % nonce.length]), decrypted[i]);
        }
    }

    @Test
    void encryptWithDifferentNoncesProducesDifferentXorInput() throws Exception {
        KeyPair keyPair = generateKeyPair();
        String pemPublicKey = toPem(keyPair);

        byte[] password = "test\0".getBytes();
        byte[] nonce1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        byte[] nonce2 = {20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

        // Verify different nonces produce different decrypted XOR results
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");

        byte[] enc1 = RsaPublicKeyEncryptor.encrypt(password, nonce1, pemPublicKey);
        cipher.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        byte[] dec1 = cipher.doFinal(enc1);

        byte[] enc2 = RsaPublicKeyEncryptor.encrypt(password, nonce2, pemPublicKey);
        cipher.init(Cipher.DECRYPT_MODE, keyPair.getPrivate());
        byte[] dec2 = cipher.doFinal(enc2);

        assertFalse(java.util.Arrays.equals(dec1, dec2));
    }

    @Test
    void encryptWithInvalidKeyThrows() {
        byte[] password = "test\0".getBytes();
        byte[] nonce = new byte[20];

        assertThrows(RuntimeException.class, () ->
            RsaPublicKeyEncryptor.encrypt(password, nonce, "not a valid key"));
    }

    private static KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    private static String toPem(KeyPair keyPair) {
        byte[] encoded = keyPair.getPublic().getEncoded();
        String base64 = Base64.getEncoder().encodeToString(encoded);
        return "-----BEGIN PUBLIC KEY-----\n" + base64 + "\n-----END PUBLIC KEY-----";
    }
}
