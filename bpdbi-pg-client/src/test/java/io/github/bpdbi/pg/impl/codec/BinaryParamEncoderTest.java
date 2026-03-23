package io.github.bpdbi.pg.impl.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BinaryParamEncoderTest {

  private static final PgBinaryCodec CODEC = PgBinaryCodec.INSTANCE;

  // --- typeOID tests ---

  @Test
  void typeOIDReturnsInt4ForInteger() {
    assertEquals(23, BinaryParamEncoder.typeOID(42));
  }

  @Test
  void typeOIDReturnsInt8ForLong() {
    assertEquals(20, BinaryParamEncoder.typeOID(100L));
  }

  @Test
  void typeOIDReturnsInt2ForShort() {
    assertEquals(21, BinaryParamEncoder.typeOID((short) 7));
  }

  @Test
  void typeOIDReturnsFloat4ForFloat() {
    assertEquals(700, BinaryParamEncoder.typeOID(3.14f));
  }

  @Test
  void typeOIDReturnsFloat8ForDouble() {
    assertEquals(701, BinaryParamEncoder.typeOID(2.718));
  }

  @Test
  void typeOIDReturnsBoolForBoolean() {
    assertEquals(16, BinaryParamEncoder.typeOID(true));
  }

  @Test
  void typeOIDReturnsByteaForByteArray() {
    assertEquals(17, BinaryParamEncoder.typeOID(new byte[] {1, 2, 3}));
  }

  @Test
  void typeOIDReturnsUuidForUUID() {
    assertEquals(2950, BinaryParamEncoder.typeOID(UUID.randomUUID()));
  }

  @Test
  void typeOIDReturnsDateForLocalDate() {
    assertEquals(1082, BinaryParamEncoder.typeOID(LocalDate.now()));
  }

  @Test
  void typeOIDReturnsTimeForLocalTime() {
    assertEquals(1083, BinaryParamEncoder.typeOID(LocalTime.now()));
  }

  @Test
  void typeOIDReturnsTimestampForLocalDateTime() {
    assertEquals(1114, BinaryParamEncoder.typeOID(LocalDateTime.now()));
  }

  @Test
  void typeOIDReturnsTimestamptzForOffsetDateTime() {
    assertEquals(1184, BinaryParamEncoder.typeOID(OffsetDateTime.now()));
  }

  @Test
  void typeOIDReturnsTimestamptzForInstant() {
    assertEquals(1184, BinaryParamEncoder.typeOID(Instant.now()));
  }

  @Test
  void typeOIDReturnsNumericForBigDecimal() {
    assertEquals(1700, BinaryParamEncoder.typeOID(BigDecimal.ONE));
  }

  @Test
  void typeOIDReturnsTimetzForOffsetTime() {
    assertEquals(1266, BinaryParamEncoder.typeOID(OffsetTime.now()));
  }

  @Test
  void typeOIDReturnsZeroForNull() {
    assertEquals(0, BinaryParamEncoder.typeOID(null));
  }

  @Test
  void typeOIDReturnsZeroForString() {
    assertEquals(0, BinaryParamEncoder.typeOID("hello"));
  }

  @Test
  void typeOIDReturnsZeroForUnsupportedType() {
    assertEquals(0, BinaryParamEncoder.typeOID(new Object()));
  }

  // --- typeOID for array types ---

  @Test
  void typeOIDReturnsInt4ArrayForIntArray() {
    assertEquals(1007, BinaryParamEncoder.typeOID(new int[] {1, 2}));
  }

  @Test
  void typeOIDReturnsInt8ArrayForLongArray() {
    assertEquals(1016, BinaryParamEncoder.typeOID(new long[] {1L}));
  }

  @Test
  void typeOIDReturnsInt2ArrayForShortArray() {
    assertEquals(1005, BinaryParamEncoder.typeOID(new short[] {1}));
  }

  @Test
  void typeOIDReturnsFloat4ArrayForFloatArray() {
    assertEquals(1021, BinaryParamEncoder.typeOID(new float[] {1.0f}));
  }

  @Test
  void typeOIDReturnsFloat8ArrayForDoubleArray() {
    assertEquals(1022, BinaryParamEncoder.typeOID(new double[] {1.0}));
  }

  @Test
  void typeOIDReturnsBoolArrayForBooleanArray() {
    assertEquals(1000, BinaryParamEncoder.typeOID(new boolean[] {true}));
  }

  @Test
  void typeOIDReturnsInt4ArrayForIntegerCollection() {
    assertEquals(1007, BinaryParamEncoder.typeOID(List.of(1, 2, 3)));
  }

  @Test
  void typeOIDReturnsTextArrayForStringCollection() {
    assertEquals(1009, BinaryParamEncoder.typeOID(List.of("a", "b")));
  }

  @Test
  void typeOIDReturnsUuidArrayForUuidCollection() {
    assertEquals(2951, BinaryParamEncoder.typeOID(List.of(UUID.randomUUID())));
  }

  @Test
  void typeOIDReturnsZeroForEmptyCollection() {
    assertEquals(0, BinaryParamEncoder.typeOID(List.of()));
  }

  // --- writeParam: basic scalar types ---

  @Test
  void writeParamNull() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(null, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(-1, reader.readInt());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamInteger() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(42, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(42, reader.readInt());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamIntegerNegative() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(-1, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(-1, reader.readInt());
  }

  @Test
  void writeParamIntegerMaxValue() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Integer.MAX_VALUE, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(Integer.MAX_VALUE, reader.readInt());
  }

  @Test
  void writeParamIntegerMinValue() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Integer.MIN_VALUE, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(Integer.MIN_VALUE, reader.readInt());
  }

  @Test
  void writeParamLong() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(123456789012345L, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(8, reader.readInt());
    assertEquals(123456789012345L, reader.readLong());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamLongMaxValue() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Long.MAX_VALUE, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(8, reader.readInt());
    assertEquals(Long.MAX_VALUE, reader.readLong());
  }

  @Test
  void writeParamShort() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam((short) 1234, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(2, reader.readInt());
    assertEquals((short) 1234, reader.readShort());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamFloat() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(3.14f, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(3.14f, reader.readFloat());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamFloatSpecialValues() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Float.NaN, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertTrue(Float.isNaN(reader.readFloat()));

    buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Float.POSITIVE_INFINITY, buf));
    reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(Float.POSITIVE_INFINITY, reader.readFloat());

    buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Float.NEGATIVE_INFINITY, buf));
    reader = readBack(buf);
    assertEquals(4, reader.readInt());
    assertEquals(Float.NEGATIVE_INFINITY, reader.readFloat());
  }

  @Test
  void writeParamDouble() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(2.718281828459045, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(8, reader.readInt());
    assertEquals(2.718281828459045, reader.readDouble());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamDoubleSpecialValues() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Double.NaN, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(8, reader.readInt());
    assertTrue(Double.isNaN(reader.readDouble()));

    buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(Double.POSITIVE_INFINITY, buf));
    reader = readBack(buf);
    assertEquals(8, reader.readInt());
    assertEquals(Double.POSITIVE_INFINITY, reader.readDouble());
  }

  @Test
  void writeParamBooleanTrue() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(true, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(1, reader.readInt());
    assertEquals(1, reader.readByte());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamBooleanFalse() {
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(false, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(1, reader.readInt());
    assertEquals(0, reader.readByte());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamByteArray() {
    byte[] data = {0x01, 0x02, 0x03, (byte) 0xFF};
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(data, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(4, reader.readInt());
    byte[] result = new byte[4];
    reader.readBytes(result);
    assertEquals(0x01, result[0]);
    assertEquals(0x02, result[1]);
    assertEquals(0x03, result[2]);
    assertEquals((byte) 0xFF, result[3]);
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamByteArrayEmpty() {
    byte[] data = {};
    ByteBuffer buf = new ByteBuffer(16);
    assertTrue(BinaryParamEncoder.writeParam(data, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(0, reader.readInt());
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamUUID() {
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    ByteBuffer buf = new ByteBuffer(32);
    assertTrue(BinaryParamEncoder.writeParam(uuid, buf));
    ByteBuffer reader = readBack(buf);
    assertEquals(16, reader.readInt());
    long msb = reader.readLong();
    long lsb = reader.readLong();
    assertEquals(uuid.getMostSignificantBits(), msb);
    assertEquals(uuid.getLeastSignificantBits(), lsb);
    assertEquals(0, reader.readableBytes());
  }

  @Test
  void writeParamUUIDRoundtrip() {
    UUID original = UUID.randomUUID();
    ByteBuffer buf = new ByteBuffer(32);
    assertTrue(BinaryParamEncoder.writeParam(original, buf));
    ByteBuffer reader = readBack(buf);
    reader.readInt(); // skip length
    UUID decoded = new UUID(reader.readLong(), reader.readLong());
    assertEquals(original, decoded);
  }

  @Test
  void writeParamReturnsFalseForString() {
    ByteBuffer buf = new ByteBuffer(16);
    assertFalse(BinaryParamEncoder.writeParam("hello", buf));
    assertEquals(0, buf.writerIndex());
  }

  // =====================================================================
  // Date/time encode→decode round-trips using PgBinaryCodec
  // =====================================================================

  @Test
  void localDateRoundTrip() {
    assertLocalDateRoundTrip(LocalDate.of(2024, 6, 15));
  }

  @Test
  void localDateBeforePgEpoch() {
    assertLocalDateRoundTrip(LocalDate.of(1970, 1, 1));
  }

  @Test
  void localDateFarPast() {
    assertLocalDateRoundTrip(LocalDate.of(1, 1, 1));
  }

  @Test
  void localDateFarFuture() {
    assertLocalDateRoundTrip(LocalDate.of(9999, 12, 31));
  }

  @Test
  void localDateInfinity() {
    assertLocalDateRoundTrip(LocalDate.MAX);
    assertLocalDateRoundTrip(LocalDate.MIN);
  }

  private void assertLocalDateRoundTrip(LocalDate original) {
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeDate(encoded, 0, encoded.length));
  }

  @Test
  void localTimeRoundTrip() {
    assertLocalTimeRoundTrip(LocalTime.of(13, 45, 30));
  }

  @Test
  void localTimeMidnight() {
    assertLocalTimeRoundTrip(LocalTime.MIDNIGHT);
  }

  @Test
  void localTimeWithMicros() {
    assertLocalTimeRoundTrip(LocalTime.of(23, 59, 59, 999_999_000));
  }

  @Test
  void localTimeNoon() {
    assertLocalTimeRoundTrip(LocalTime.NOON);
  }

  private void assertLocalTimeRoundTrip(LocalTime original) {
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTime(encoded, 0, encoded.length));
  }

  @Test
  void localDateTimeRoundTrip() {
    assertLocalDateTimeRoundTrip(LocalDateTime.of(2024, 6, 15, 13, 45, 30));
  }

  @Test
  void localDateTimeBeforePgEpoch() {
    assertLocalDateTimeRoundTrip(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
  }

  @Test
  void localDateTimeWithMicros() {
    assertLocalDateTimeRoundTrip(LocalDateTime.of(2024, 1, 1, 12, 0, 0, 123_456_000));
  }

  private void assertLocalDateTimeRoundTrip(LocalDateTime original) {
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimestamp(encoded, 0, encoded.length));
  }

  @Test
  void offsetDateTimeRoundTripUTC() {
    var original = OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.UTC);
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimestamptz(encoded, 0, encoded.length));
  }

  @Test
  void offsetDateTimeRoundTripNonUTC() {
    var original = OffsetDateTime.of(2024, 6, 15, 15, 45, 30, 0, ZoneOffset.ofHours(2));
    byte[] encoded = encodeValue(original);
    // PG normalizes to UTC: 15:45+02 = 13:45 UTC
    var expected = OffsetDateTime.of(2024, 6, 15, 13, 45, 30, 0, ZoneOffset.UTC);
    assertEquals(expected, CODEC.decodeTimestamptz(encoded, 0, encoded.length));
  }

  @Test
  void offsetDateTimeRoundTripWithMicros() {
    var original = OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 123_456_000, ZoneOffset.UTC);
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimestamptz(encoded, 0, encoded.length));
  }

  @Test
  void instantRoundTrip() {
    var original = Instant.now().truncatedTo(ChronoUnit.MICROS);
    byte[] encoded = encodeValue(original);
    var decoded = CODEC.decodeTimestamptz(encoded, 0, encoded.length).toInstant();
    assertEquals(original, decoded);
  }

  @Test
  void instantEpoch() {
    var original = Instant.EPOCH;
    byte[] encoded = encodeValue(original);
    var decoded = CODEC.decodeTimestamptz(encoded, 0, encoded.length).toInstant();
    assertEquals(original, decoded);
  }

  @Test
  void offsetTimeRoundTripUTC() {
    var original = OffsetTime.of(LocalTime.of(12, 30, 0), ZoneOffset.UTC);
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimetz(encoded, 0, encoded.length));
  }

  @Test
  void offsetTimeRoundTripPositiveOffset() {
    var original = OffsetTime.of(LocalTime.of(17, 55, 4), ZoneOffset.ofHours(2));
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimetz(encoded, 0, encoded.length));
  }

  @Test
  void offsetTimeRoundTripNegativeOffset() {
    var original = OffsetTime.of(LocalTime.of(8, 15, 30), ZoneOffset.ofHours(-5));
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimetz(encoded, 0, encoded.length));
  }

  @Test
  void offsetTimeRoundTripWithMicros() {
    var original =
        OffsetTime.of(LocalTime.of(23, 59, 59, 999_999_000), ZoneOffset.ofHoursMinutes(5, 30));
    byte[] encoded = encodeValue(original);
    assertEquals(original, CODEC.decodeTimetz(encoded, 0, encoded.length));
  }

  // =====================================================================
  // BigDecimal (numeric) encode→decode round-trips
  // =====================================================================

  @Test
  void numericZero() {
    assertNumericRoundTrip(BigDecimal.ZERO);
  }

  @Test
  void numericOne() {
    assertNumericRoundTrip(BigDecimal.ONE);
  }

  @Test
  void numericTen() {
    assertNumericRoundTrip(BigDecimal.TEN);
  }

  @Test
  void numericNegative() {
    assertNumericRoundTrip(new BigDecimal("-42.5"));
  }

  @Test
  void numericSmallFraction() {
    assertNumericRoundTrip(new BigDecimal("0.001"));
  }

  @Test
  void numericVerySmallFraction() {
    assertNumericRoundTrip(new BigDecimal("0.00000001"));
  }

  @Test
  void numericLargeInteger() {
    assertNumericRoundTrip(new BigDecimal("1000000"));
  }

  @Test
  void numericLargeWithDecimals() {
    assertNumericRoundTrip(new BigDecimal("12345.678"));
  }

  @Test
  void numericTypicalMoney() {
    assertNumericRoundTrip(new BigDecimal("123.45"));
  }

  @Test
  void numericMaxPrecision() {
    assertNumericRoundTrip(new BigDecimal("99999999999999999999.99999999999999999999"));
  }

  @Test
  void numericNegativeMaxPrecision() {
    assertNumericRoundTrip(new BigDecimal("-99999999999999999999.99999999999999999999"));
  }

  @Test
  void numericZeroWithScale() {
    var value = new BigDecimal("0.00");
    byte[] encoded = encodeValue(value);
    var decoded = CODEC.decodeNumeric(encoded, 0, encoded.length);
    assertEquals(0, value.compareTo(decoded));
  }

  @Test
  void numericNegativeScale() {
    // BigDecimal with negative scale: 1.2E+10 = 12000000000
    var value = new BigDecimal("1.2E+10");
    byte[] encoded = encodeValue(value);
    var decoded = CODEC.decodeNumeric(encoded, 0, encoded.length);
    assertEquals(0, value.compareTo(decoded));
  }

  @Test
  void numericOneDigit() {
    assertNumericRoundTrip(new BigDecimal("7"));
  }

  @Test
  void numericBoundary9999() {
    assertNumericRoundTrip(new BigDecimal("9999"));
  }

  @Test
  void numericBoundary10000() {
    assertNumericRoundTrip(new BigDecimal("10000"));
  }

  @Test
  void numericBoundary10001() {
    assertNumericRoundTrip(new BigDecimal("10001"));
  }

  @Test
  void numericVeryLargeInteger() {
    assertNumericRoundTrip(new BigDecimal("123456789012345678901234567890"));
  }

  private void assertNumericRoundTrip(BigDecimal original) {
    byte[] encoded = encodeValue(original);
    var decoded = CODEC.decodeNumeric(encoded, 0, encoded.length);
    assertEquals(0, original.compareTo(decoded), "expected " + original + " but got " + decoded);
  }

  // =====================================================================
  // Primitive array encode→decode round-trips
  // =====================================================================

  @Test
  void intArrayRoundTrip() {
    int[] original = {1, 2, 3, -42, Integer.MAX_VALUE};
    byte[] encoded = encodeValue(original);
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt4(buf, off));
    assertEquals(List.of(1, 2, 3, -42, Integer.MAX_VALUE), decoded);
  }

  @Test
  void intArrayEmpty() {
    int[] original = {};
    byte[] encoded = encodeValue(original);
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt4(buf, off));
    assertEquals(List.of(), decoded);
  }

  @Test
  void longArrayRoundTrip() {
    long[] original = {100L, -200L, Long.MAX_VALUE};
    byte[] encoded = encodeValue(original);
    List<Long> decoded = CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt8(buf, off));
    assertEquals(List.of(100L, -200L, Long.MAX_VALUE), decoded);
  }

  @Test
  void shortArrayRoundTrip() {
    short[] original = {1, 2, -3};
    byte[] encoded = encodeValue(original);
    List<Short> decoded = CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt2(buf, off));
    assertEquals(List.of((short) 1, (short) 2, (short) -3), decoded);
  }

  @Test
  void floatArrayRoundTrip() {
    float[] original = {1.5f, -2.5f, 0.0f};
    byte[] encoded = encodeValue(original);
    List<Float> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeFloat4(buf, off));
    assertEquals(List.of(1.5f, -2.5f, 0.0f), decoded);
  }

  @Test
  void doubleArrayRoundTrip() {
    double[] original = {1.5, -2.5, 0.0};
    byte[] encoded = encodeValue(original);
    List<Double> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeFloat8(buf, off));
    assertEquals(List.of(1.5, -2.5, 0.0), decoded);
  }

  @Test
  void booleanArrayRoundTrip() {
    boolean[] original = {true, false, true};
    byte[] encoded = encodeValue(original);
    List<Boolean> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeBool(buf, off));
    assertEquals(List.of(true, false, true), decoded);
  }

  // =====================================================================
  // Collection / Object[] array encode→decode round-trips
  // =====================================================================

  @Test
  void integerCollectionRoundTrip() {
    List<Integer> original = List.of(10, 20, 30);
    byte[] encoded = encodeValue(original);
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt4(buf, off));
    assertEquals(original, decoded);
  }

  @Test
  void stringCollectionRoundTrip() {
    List<String> original = List.of("hello", "world", "test");
    byte[] encoded = encodeValue(original);
    List<String> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeString(buf, off, len));
    assertEquals(original, decoded);
  }

  @Test
  void stringCollectionWithSpecialChars() {
    List<String> original = List.of("hello world", "with,comma", "with\"quote", "with\\backslash");
    byte[] encoded = encodeValue(original);
    List<String> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeString(buf, off, len));
    assertEquals(original, decoded);
  }

  @Test
  void uuidCollectionRoundTrip() {
    var u1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    var u2 = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    List<UUID> original = List.of(u1, u2);
    byte[] encoded = encodeValue(original);
    List<UUID> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeUuid(buf, off, len));
    assertEquals(original, decoded);
  }

  @Test
  void collectionWithNullElements() {
    var original = new ArrayList<Integer>();
    original.add(1);
    original.add(null);
    original.add(3);
    byte[] encoded = encodeValue(original);
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt4(buf, off));
    assertEquals(3, decoded.size());
    assertEquals(1, decoded.get(0));
    assertEquals(null, decoded.get(1));
    assertEquals(3, decoded.get(2));
  }

  @Test
  void emptyCollectionWritesEmptyArray() {
    ByteBuffer buf = new ByteBuffer(32);
    assertTrue(BinaryParamEncoder.writeParam(List.of(), buf));
    byte[] encoded = encodeValue(List.of());
    // Should decode as empty list
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf2, off, len) -> CODEC.decodeInt4(buf2, off));
    assertEquals(List.of(), decoded);
  }

  @Test
  void objectArrayRoundTrip() {
    Object[] original = {1, 2, 3};
    byte[] encoded = encodeValue(original);
    List<Integer> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeInt4(buf, off));
    assertEquals(List.of(1, 2, 3), decoded);
  }

  @Test
  void stringArrayRoundTrip() {
    String[] original = {"a", "b", "c"};
    byte[] encoded = encodeValue(original);
    List<String> decoded =
        CODEC.decodeArray(encoded, (buf, off, len) -> CODEC.decodeString(buf, off, len));
    assertEquals(List.of("a", "b", "c"), decoded);
  }

  // =====================================================================
  // Helpers
  // =====================================================================

  /** Encode a value via writeParam, skip the 4-byte length prefix, return the raw value bytes. */
  private static byte[] encodeValue(Object value) {
    ByteBuffer buf = new ByteBuffer(256);
    assertTrue(BinaryParamEncoder.writeParam(value, buf));
    ByteBuffer reader = readBack(buf);
    int len = reader.readInt();
    if (len == -1) return new byte[0];
    byte[] result = new byte[len];
    reader.readBytes(result);
    return result;
  }

  private static ByteBuffer readBack(ByteBuffer buf) {
    return ByteBuffer.wrap(buf.toByteArray());
  }
}
