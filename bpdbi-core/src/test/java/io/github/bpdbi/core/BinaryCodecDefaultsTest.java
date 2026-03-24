package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Tests the default methods in the BinaryCodec interface ({@code decode(byte[], Class)}, {@code
 * canDecode}, {@code decodeBytes}) via a minimal stub that implements only the abstract methods.
 */
class BinaryCodecDefaultsTest {

  /**
   * Minimal BinaryCodec that implements the required abstract methods. Default methods ({@code
   * decode}, {@code canDecode}, {@code decodeBytes}) are inherited and tested.
   */
  @SuppressWarnings("NullAway") // Test stub — intentionally minimal
  private static final BinaryCodec STUB =
      new BinaryCodec() {
        @Override
        public boolean decodeBool(byte @NonNull [] buf, int offset) {
          return buf[offset] != 0;
        }

        @Override
        public short decodeInt2(byte @NonNull [] buf, int offset) {
          return (short) ((buf[offset] << 8) | (buf[offset + 1] & 0xFF));
        }

        @Override
        public int decodeInt4(byte @NonNull [] buf, int offset) {
          return (buf[offset] << 24)
              | ((buf[offset + 1] & 0xFF) << 16)
              | ((buf[offset + 2] & 0xFF) << 8)
              | (buf[offset + 3] & 0xFF);
        }

        @Override
        public long decodeInt8(byte @NonNull [] buf, int offset) {
          return ((long) decodeInt4(buf, offset) << 32)
              | (decodeInt4(buf, offset + 4) & 0xFFFFFFFFL);
        }

        @Override
        public float decodeFloat4(byte @NonNull [] buf, int offset) {
          return Float.intBitsToFloat(decodeInt4(buf, offset));
        }

        @Override
        public double decodeFloat8(byte @NonNull [] buf, int offset) {
          return Double.longBitsToDouble(decodeInt8(buf, offset));
        }

        @Override
        public @NonNull String decodeString(byte @NonNull [] buf, int offset, int length) {
          return new String(buf, offset, length, StandardCharsets.UTF_8);
        }

        @Override
        public @NonNull UUID decodeUuid(byte @NonNull [] buf, int offset, int length) {
          long msb = decodeInt8(buf, offset);
          long lsb = decodeInt8(buf, offset + 8);
          return new UUID(msb, lsb);
        }

        @Override
        public @NonNull LocalDate decodeDate(byte @NonNull [] buf, int offset, int length) {
          return LocalDate.of(2000, 1, 1).plusDays(decodeInt4(buf, offset));
        }

        @Override
        public @NonNull LocalTime decodeTime(byte @NonNull [] buf, int offset, int length) {
          return LocalTime.ofNanoOfDay(decodeInt8(buf, offset) * 1000);
        }

        @Override
        public @NonNull LocalDateTime decodeTimestamp(
            byte @NonNull [] buf, int offset, int length) {
          return LocalDateTime.of(2000, 1, 1, 0, 0).plusNanos(decodeInt8(buf, offset) * 1000);
        }

        @Override
        public @NonNull OffsetDateTime decodeTimestamptz(
            byte @NonNull [] buf, int offset, int length) {
          return decodeTimestamp(buf, offset, length).atOffset(ZoneOffset.UTC);
        }

        @Override
        public @NonNull OffsetTime decodeTimetz(byte @NonNull [] buf, int offset, int length) {
          return OffsetTime.of(LocalTime.NOON, ZoneOffset.UTC);
        }

        @Override
        public @NonNull BigDecimal decodeNumeric(byte @NonNull [] buf, int offset, int length) {
          return new BigDecimal(decodeString(buf, offset, length));
        }

        @Override
        public @NonNull String decodeJson(
            byte @NonNull [] buf, int offset, int length, int typeOID) {
          return decodeString(buf, offset, length);
        }
      };

  // ===== Offset-based defaults that copy slice and delegate =====

  @Test
  void decodeBoolOffset() {
    byte[] buf = {0, 1, 0}; // true at offset 1
    assertTrue(STUB.decodeBool(buf, 1));
    assertFalse(STUB.decodeBool(buf, 0));
  }

  @Test
  void decodeInt2Offset() {
    byte[] buf = {0, 0, 0x01, 0x00}; // 256 at offset 2
    assertEquals((short) 256, STUB.decodeInt2(buf, 2));
  }

  @Test
  void decodeInt4Offset() {
    byte[] buf = {(byte) 0xFF, 0, 0, 0, 42, 0}; // 42 at offset 1
    assertEquals(42, STUB.decodeInt4(buf, 1));
  }

  @Test
  void decodeInt8Offset() {
    byte[] buf = new byte[10];
    buf[2] = 0;
    buf[3] = 0;
    buf[4] = 0;
    buf[5] = 0;
    buf[6] = 0;
    buf[7] = 0;
    buf[8] = 0;
    buf[9] = 1; // value=1 at offset 2
    assertEquals(1L, STUB.decodeInt8(buf, 2));
  }

  @Test
  void decodeFloat4Offset() {
    float val = 3.14f;
    int bits = Float.floatToIntBits(val);
    byte[] buf = new byte[6];
    buf[1] = (byte) (bits >> 24);
    buf[2] = (byte) (bits >> 16);
    buf[3] = (byte) (bits >> 8);
    buf[4] = (byte) bits;
    assertEquals(val, STUB.decodeFloat4(buf, 1), 0.001f);
  }

  @Test
  void decodeFloat8Offset() {
    double val = 3.14159;
    long bits = Double.doubleToLongBits(val);
    byte[] buf = new byte[10];
    for (int i = 0; i < 8; i++) {
      buf[i + 1] = (byte) (bits >> (56 - i * 8));
    }
    assertEquals(val, STUB.decodeFloat8(buf, 1), 1e-10);
  }

  @Test
  void decodeStringOffset() {
    byte[] buf = "XXhelloYY".getBytes(StandardCharsets.UTF_8);
    assertEquals("hello", STUB.decodeString(buf, 2, 5));
  }

  @Test
  void decodeBytesOffset() {
    byte[] buf = {0, 1, 2, 3, 4, 5};
    byte[] result = STUB.decodeBytes(buf, 2, 3);
    assertArrayEquals(new byte[] {2, 3, 4}, result);
  }

  @Test
  void decodeNumericOffset() {
    byte[] buf = "XX42.5YY".getBytes(StandardCharsets.UTF_8);
    assertEquals(new BigDecimal("42.5"), STUB.decodeNumeric(buf, 2, 4));
  }

  @Test
  void decodeJsonOffset() {
    byte[] buf = "XX{\"key\":1}YY".getBytes(StandardCharsets.UTF_8);
    assertEquals("{\"key\":1}", STUB.decodeJson(buf, 2, 9, 114));
  }

  @Test
  void decodeDateOffset() {
    // 1 day after PG epoch → 2000-01-02
    byte[] buf = {(byte) 0xFF, 0, 0, 0, 1};
    assertEquals(LocalDate.of(2000, 1, 2), STUB.decodeDate(buf, 1, 4));
  }

  @Test
  void decodeGenericOffset() {
    byte[] buf = "XXhelloYY".getBytes(StandardCharsets.UTF_8);
    assertEquals("hello", STUB.decode(buf, 2, 5, String.class));
  }

  // ===== canDecode =====

  @Test
  void canDecodeKnownTypes() {
    assertTrue(STUB.canDecode(String.class));
    assertTrue(STUB.canDecode(Integer.class));
    assertTrue(STUB.canDecode(Long.class));
    assertTrue(STUB.canDecode(Short.class));
    assertTrue(STUB.canDecode(Float.class));
    assertTrue(STUB.canDecode(Double.class));
    assertTrue(STUB.canDecode(Boolean.class));
    assertTrue(STUB.canDecode(BigDecimal.class));
    assertTrue(STUB.canDecode(UUID.class));
    assertTrue(STUB.canDecode(LocalDate.class));
    assertTrue(STUB.canDecode(LocalTime.class));
    assertTrue(STUB.canDecode(LocalDateTime.class));
    assertTrue(STUB.canDecode(OffsetDateTime.class));
    assertTrue(STUB.canDecode(OffsetTime.class));
    assertTrue(STUB.canDecode(java.time.Instant.class));
    assertTrue(STUB.canDecode(byte[].class));
  }

  @Test
  void canDecodeUnknownType() {
    assertFalse(STUB.canDecode(BinaryCodecDefaultsTest.class));
  }

  // ===== decode dispatch =====

  @Test
  void decodeDispatchString() {
    byte[] val = "hello".getBytes(StandardCharsets.UTF_8);
    assertEquals("hello", STUB.decode(val, String.class));
  }

  @Test
  void decodeDispatchInteger() {
    byte[] val = {0, 0, 0, 42};
    assertEquals(42, STUB.decode(val, Integer.class));
  }

  @Test
  void decodeDispatchBoolean() {
    assertEquals(true, STUB.decode(new byte[] {1}, Boolean.class));
  }

  @Test
  void decodeDispatchBytes() {
    byte[] val = {1, 2, 3};
    assertArrayEquals(new byte[] {1, 2, 3}, STUB.decode(val, byte[].class));
  }

  @Test
  void decodeDispatchUnsupportedThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> STUB.decode(new byte[] {}, BinaryCodecDefaultsTest.class));
  }

  // ===== Default array methods return null =====

  @Test
  void decodeArrayElementDecoderDefaultReturnsNull() {
    assertNull(STUB.decodeArray(new byte[] {}, (buf, off, len) -> "x"));
  }
}
