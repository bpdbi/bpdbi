package io.djb.mysql.impl.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MysqlBinaryCodecTest {

  private final MysqlBinaryCodec codec = MysqlBinaryCodec.INSTANCE;

  // =====================================================================
  // Bool
  // =====================================================================

  @Test
  void decodeBoolTrue() {
    assertTrue(codec.decodeBool(new byte[]{1}));
  }

  @Test
  void decodeBoolFalse() {
    assertFalse(codec.decodeBool(new byte[]{0}));
  }

  // =====================================================================
  // Int2 (little-endian)
  // =====================================================================

  @Test
  void decodeInt2() {
    // 0x0100 LE = 1 in big-endian at offset 0 → 0x00 | (0x01 << 8) = 256
    assertEquals((short) 256, codec.decodeInt2(new byte[]{0x00, 0x01}));
  }

  @Test
  void decodeInt2Negative() {
    assertEquals((short) -1, codec.decodeInt2(new byte[]{(byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  void encodeDecodeInt2RoundTrip() {
    short value = 12345;
    assertEquals(value, codec.decodeInt2(MysqlBinaryCodec.encodeInt2LE(value)));
  }

  // =====================================================================
  // Int4 (little-endian)
  // =====================================================================

  @Test
  void decodeInt4() {
    // 1 in LE = {0x01, 0x00, 0x00, 0x00}
    assertEquals(1, codec.decodeInt4(new byte[]{0x01, 0x00, 0x00, 0x00}));
  }

  @Test
  void decodeInt4Negative() {
    assertEquals(
        -1,
        codec.decodeInt4(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF})
    );
  }

  @Test
  void encodeDecodeInt4RoundTrip() {
    int value = 123456;
    assertEquals(value, codec.decodeInt4(MysqlBinaryCodec.encodeInt4LE(value)));
  }

  // =====================================================================
  // Int8 (little-endian)
  // =====================================================================

  @Test
  void decodeInt8() {
    long value = 9876543210L;
    assertEquals(value, codec.decodeInt8(MysqlBinaryCodec.encodeInt8LE(value)));
  }

  @Test
  void decodeInt8MaxValue() {
    assertEquals(Long.MAX_VALUE, codec.decodeInt8(MysqlBinaryCodec.encodeInt8LE(Long.MAX_VALUE)));
  }

  // =====================================================================
  // Float4/8 (little-endian)
  // =====================================================================

  @Test
  void decodeFloat4() {
    float value = 3.14f;
    assertEquals(value, codec.decodeFloat4(MysqlBinaryCodec.encodeFloat4LE(value)), 0.001f);
  }

  @Test
  void decodeFloat8() {
    double value = 3.14159265358979;
    assertEquals(value, codec.decodeFloat8(MysqlBinaryCodec.encodeFloat8LE(value)), 1e-15);
  }

  // =====================================================================
  // String
  // =====================================================================

  @Test
  void decodeString() {
    assertEquals("hello", codec.decodeString("hello".getBytes()));
  }

  @Test
  void encodeString() {
    assertArrayEquals("hello".getBytes(), MysqlBinaryCodec.encodeString("hello"));
  }

  // =====================================================================
  // UUID (MySQL stores as string)
  // =====================================================================

  @Test
  void decodeUuid() {
    String uuidStr = "550e8400-e29b-41d4-a716-446655440000";
    UUID expected = UUID.fromString(uuidStr);
    assertEquals(expected, codec.decodeUuid(uuidStr.getBytes()));
  }

  // =====================================================================
  // Date
  // =====================================================================

  @Test
  void decodeDateNormal() {
    // length=4, year=2025 (LE: 0xE9, 0x07), month=6, day=15
    byte[] data = {4, (byte) 0xE9, 0x07, 6, 15};
    assertEquals(LocalDate.of(2025, 6, 15), codec.decodeDate(data));
  }

  @Test
  void decodeDateEmpty() {
    assertNull(codec.decodeDate(new byte[0]));
  }

  @Test
  void decodeDateZeroLength() {
    assertNull(codec.decodeDate(new byte[]{0}));
  }

  @Test
  void encodeDateRoundTrip() {
    LocalDate date = LocalDate.of(2025, 6, 15);
    assertEquals(date, codec.decodeDate(MysqlBinaryCodec.encodeDate(date)));
  }

  // =====================================================================
  // Time
  // =====================================================================

  @Test
  void decodeTimeEmpty() {
    assertEquals(LocalTime.MIDNIGHT, codec.decodeTime(new byte[0]));
  }

  @Test
  void decodeTimeZeroLength() {
    assertEquals(LocalTime.MIDNIGHT, codec.decodeTime(new byte[]{0}));
  }

  @Test
  void decodeTimeNormal() {
    // length=8, is_negative=0, days=0, hour=12, minute=30, second=45
    byte[] data = {8, 0, 0, 0, 0, 0, 12, 30, 45};
    assertEquals(LocalTime.of(12, 30, 45), codec.decodeTime(data));
  }

  @Test
  void decodeTimeWithMicros() {
    // length=12, is_negative=0, days=0, hour=12, min=30, sec=45, micro=123456 (LE)
    int micro = 123456;
    byte[] data = {12, 0, 0, 0, 0, 0, 12, 30, 45,
        (byte) micro, (byte) (micro >> 8), (byte) (micro >> 16), (byte) (micro >> 24)};
    LocalTime expected = LocalTime.of(12, 30, 45, 123456000);
    assertEquals(expected, codec.decodeTime(data));
  }

  @Test
  void encodeTimeRoundTrip() {
    LocalTime time = LocalTime.of(14, 30, 0);
    assertEquals(time, codec.decodeTime(MysqlBinaryCodec.encodeTime(time)));
  }

  @Test
  void encodeTimeMidnight() {
    LocalTime time = LocalTime.MIDNIGHT;
    assertEquals(time, codec.decodeTime(MysqlBinaryCodec.encodeTime(time)));
  }

  @Test
  void encodeTimeWithNanos() {
    LocalTime time = LocalTime.of(10, 20, 30, 500000000); // 500ms = 500000 micros
    assertEquals(time, codec.decodeTime(MysqlBinaryCodec.encodeTime(time)));
  }

  // =====================================================================
  // Timestamp (DATETIME)
  // =====================================================================

  @Test
  void decodeTimestampEmpty() {
    assertNull(codec.decodeTimestamp(new byte[0]));
  }

  @Test
  void decodeTimestampZeroLength() {
    assertNull(codec.decodeTimestamp(new byte[]{0}));
  }

  @Test
  void decodeTimestampDateOnly() {
    // length=4
    byte[] data = {4, (byte) 0xE9, 0x07, 6, 15};
    assertEquals(LocalDateTime.of(2025, 6, 15, 0, 0, 0), codec.decodeTimestamp(data));
  }

  @Test
  void decodeTimestampFull() {
    // length=7
    byte[] data = {7, (byte) 0xE9, 0x07, 6, 15, 12, 30, 45};
    assertEquals(LocalDateTime.of(2025, 6, 15, 12, 30, 45), codec.decodeTimestamp(data));
  }

  @Test
  void decodeTimestampWithMicros() {
    // length=11
    int micro = 500000;
    byte[] data = {11, (byte) 0xE9, 0x07, 6, 15, 12, 30, 45,
        (byte) micro, (byte) (micro >> 8), (byte) (micro >> 16), (byte) (micro >> 24)};
    assertEquals(LocalDateTime.of(2025, 6, 15, 12, 30, 45, 500000000), codec.decodeTimestamp(data));
  }

  @Test
  void encodeDatetimeRoundTripDateOnly() {
    LocalDateTime dt = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
    assertEquals(dt, codec.decodeTimestamp(MysqlBinaryCodec.encodeDatetime(dt)));
  }

  @Test
  void encodeDatetimeRoundTripFull() {
    LocalDateTime dt = LocalDateTime.of(2025, 6, 15, 12, 30, 45);
    assertEquals(dt, codec.decodeTimestamp(MysqlBinaryCodec.encodeDatetime(dt)));
  }

  @Test
  void encodeDatetimeRoundTripWithMicros() {
    LocalDateTime dt = LocalDateTime.of(2025, 6, 15, 12, 30, 45, 123000000); // 123000 micros
    assertEquals(dt, codec.decodeTimestamp(MysqlBinaryCodec.encodeDatetime(dt)));
  }

  // =====================================================================
  // Timestamptz
  // =====================================================================

  @Test
  void decodeTimestamptzNull() {
    assertNull(codec.decodeTimestamptz(new byte[0]));
  }

  @Test
  void decodeTimestamptzNormal() {
    byte[] data = {7, (byte) 0xE9, 0x07, 6, 15, 12, 30, 45};
    OffsetDateTime expected = OffsetDateTime.of(2025, 6, 15, 12, 30, 45, 0, ZoneOffset.UTC);
    assertEquals(expected, codec.decodeTimestamptz(data));
  }

  // =====================================================================
  // Bytes
  // =====================================================================

  @Test
  void decodeBytes() {
    byte[] data = {1, 2, 3};
    assertSame(data, codec.decodeBytes(data));
  }

  // =====================================================================
  // Numeric (MySQL sends as text in binary protocol)
  // =====================================================================

  @Test
  void decodeNumeric() {
    assertEquals(new BigDecimal("123.45"), codec.decodeNumeric("123.45".getBytes()));
  }

  // =====================================================================
  // JSON
  // =====================================================================

  @Test
  void decodeJson() {
    byte[] data = "{\"key\":\"value\"}".getBytes();
    assertEquals("{\"key\":\"value\"}", codec.decodeJson(data, 245));
  }

  // =====================================================================
  // Duration (MySQL TIME can be >24h and negative)
  // =====================================================================

  @Test
  void decodeDurationZeroEmpty() {
    assertEquals(Duration.ZERO, codec.decodeDuration(new byte[0]));
  }

  @Test
  void decodeDurationZeroLength() {
    assertEquals(Duration.ZERO, codec.decodeDuration(new byte[]{0}));
  }

  @Test
  void decodeDurationPositive() {
    // length=8, not_negative, days=0, hour=1, minute=30, second=0
    byte[] data = {8, 0, 0, 0, 0, 0, 1, 30, 0};
    Duration expected = Duration.ofHours(1).plusMinutes(30);
    assertEquals(expected, codec.decodeDuration(data));
  }

  @Test
  void decodeDurationNegative() {
    // length=8, is_negative=1, days=0, hour=2, minute=0, second=0
    byte[] data = {8, 1, 0, 0, 0, 0, 2, 0, 0};
    Duration expected = Duration.ofHours(-2);
    assertEquals(expected, codec.decodeDuration(data));
  }

  @Test
  void decodeDurationWithDays() {
    // length=8, not_negative, days=2, hour=3, minute=0, second=0
    byte[] data = {8, 0, 2, 0, 0, 0, 3, 0, 0};
    Duration expected = Duration.ofDays(2).plusHours(3);
    assertEquals(expected, codec.decodeDuration(data));
  }

  @Test
  void decodeDurationWithMicros() {
    // length=12, not_negative, days=0, hour=0, minute=0, second=1, micro=500000
    int micro = 500000;
    byte[] data = {12, 0, 0, 0, 0, 0, 0, 0, 1,
        (byte) micro, (byte) (micro >> 8), (byte) (micro >> 16), (byte) (micro >> 24)};
    Duration expected = Duration.ofSeconds(1).plusNanos(500000000);
    assertEquals(expected, codec.decodeDuration(data));
  }

  @Test
  void decodeDurationInvalidLength() {
    // length=3 is invalid — code will throw (either AIOOBE or IAE depending on data size)
    byte[] data = {3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}; // enough bytes to avoid AIOOBE
    assertThrows(IllegalArgumentException.class, () -> codec.decodeDuration(data));
  }

  @Test
  void encodeDurationZero() {
    byte[] encoded = MysqlBinaryCodec.encodeDuration(Duration.ZERO);
    assertEquals(Duration.ZERO, codec.decodeDuration(encoded));
  }

  @Test
  void encodeDurationPositiveRoundTrip() {
    Duration d = Duration.ofHours(25).plusMinutes(30).plusSeconds(15);
    assertEquals(d, codec.decodeDuration(MysqlBinaryCodec.encodeDuration(d)));
  }

  @Test
  void encodeDurationNegativeRoundTrip() {
    Duration d = Duration.ofHours(-3).plusMinutes(-30);
    assertEquals(d, codec.decodeDuration(MysqlBinaryCodec.encodeDuration(d)));
  }

  // =====================================================================
  // Unsigned integer types
  // =====================================================================

  @Test
  void decodeUnsignedInt1() {
    assertEquals((short) 200, codec.decodeUnsignedInt1(new byte[]{(byte) 200}));
  }

  @Test
  void decodeUnsignedInt1Zero() {
    assertEquals((short) 0, codec.decodeUnsignedInt1(new byte[]{0}));
  }

  @Test
  void decodeUnsignedInt1Max() {
    assertEquals((short) 255, codec.decodeUnsignedInt1(new byte[]{(byte) 0xFF}));
  }

  @Test
  void decodeUnsignedInt2() {
    // 1000 in LE = {0xE8, 0x03}
    assertEquals(1000, codec.decodeUnsignedInt2(new byte[]{(byte) 0xE8, 0x03}));
  }

  @Test
  void decodeUnsignedInt2Max() {
    assertEquals(65535, codec.decodeUnsignedInt2(new byte[]{(byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  void decodeUnsignedInt3() {
    // 0xFFFFFF in LE bytes (stored in 4 bytes, masked to 3)
    assertEquals(
        0xFFFFFF,
        codec.decodeUnsignedInt3(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0})
    );
  }

  @Test
  void decodeUnsignedInt4() {
    // 0xFFFFFFFF → 4294967295L
    assertEquals(
        4294967295L,
        codec.decodeUnsignedInt4(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF})
    );
  }

  @Test
  void decodeUnsignedInt4Zero() {
    assertEquals(0L, codec.decodeUnsignedInt4(new byte[]{0, 0, 0, 0}));
  }

  @Test
  void decodeUnsignedInt8() {
    // Max unsigned 64-bit: 0xFFFFFFFFFFFFFFFF = 18446744073709551615
    byte[] data = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertEquals(new BigInteger("18446744073709551615"), codec.decodeUnsignedInt8(data));
  }

  @Test
  void decodeUnsignedInt8Zero() {
    assertEquals(BigInteger.ZERO, codec.decodeUnsignedInt8(new byte[8]));
  }

  @Test
  void decodeUnsignedInt8One() {
    // 1 in LE
    byte[] data = {1, 0, 0, 0, 0, 0, 0, 0};
    assertEquals(BigInteger.ONE, codec.decodeUnsignedInt8(data));
  }

  // =====================================================================
  // BIT type
  // =====================================================================

  @Test
  void decodeBitSingleByte() {
    assertEquals(0xFF, codec.decodeBit(new byte[]{(byte) 0xFF}));
  }

  @Test
  void decodeBitMultiByte() {
    // {0x01, 0x00} → 0x0100 = 256
    assertEquals(256, codec.decodeBit(new byte[]{0x01, 0x00}));
  }

  @Test
  void decodeBitZero() {
    assertEquals(0, codec.decodeBit(new byte[]{0}));
  }

  // =====================================================================
  // Encode helpers
  // =====================================================================

  @Test
  void encodeInt1() {
    assertArrayEquals(new byte[]{42}, MysqlBinaryCodec.encodeInt1(42));
  }
}
