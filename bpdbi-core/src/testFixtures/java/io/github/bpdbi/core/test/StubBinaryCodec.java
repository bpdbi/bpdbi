package io.github.bpdbi.core.test;

import io.github.bpdbi.core.BinaryCodec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

/**
 * Test-only BinaryCodec that parses UTF-8 text representations. All decode methods interpret the
 * raw bytes as UTF-8 text and parse accordingly. Used by mapper tests that construct rows from
 * string values without a real database.
 */
public final class StubBinaryCodec implements BinaryCodec {

  public static final StubBinaryCodec INSTANCE = new StubBinaryCodec();

  private StubBinaryCodec() {}

  private static String text(byte[] v) {
    return new String(v, StandardCharsets.UTF_8);
  }

  private static String text(byte[] buf, int offset, int length) {
    return new String(buf, offset, length, StandardCharsets.UTF_8);
  }

  /** Read from offset to end of array. Test rows always use per-value byte arrays. */
  private static String textFrom(byte[] buf, int offset) {
    return text(buf, offset, buf.length - offset);
  }

  @Override
  public boolean decodeBool(byte @NonNull [] buf, int offset) {
    byte b = buf[offset];
    return b == 't' || b == '1';
  }

  @Override
  public short decodeInt2(byte @NonNull [] buf, int offset) {
    return Short.parseShort(textFrom(buf, offset));
  }

  @Override
  public int decodeInt4(byte @NonNull [] buf, int offset) {
    return Integer.parseInt(textFrom(buf, offset));
  }

  @Override
  public long decodeInt8(byte @NonNull [] buf, int offset) {
    return Long.parseLong(textFrom(buf, offset));
  }

  @Override
  public float decodeFloat4(byte @NonNull [] buf, int offset) {
    return Float.parseFloat(textFrom(buf, offset));
  }

  @Override
  public double decodeFloat8(byte @NonNull [] buf, int offset) {
    return Double.parseDouble(textFrom(buf, offset));
  }

  @Override
  public @NonNull String decodeString(byte @NonNull [] buf, int offset, int length) {
    return text(buf, offset, length);
  }

  @Override
  public @NonNull UUID decodeUuid(byte @NonNull [] buf, int offset, int length) {
    return UUID.fromString(text(buf, offset, length));
  }

  @Override
  public @NonNull LocalDate decodeDate(byte @NonNull [] buf, int offset, int length) {
    return LocalDate.parse(text(buf, offset, length));
  }

  @Override
  public @NonNull LocalTime decodeTime(byte @NonNull [] buf, int offset, int length) {
    return LocalTime.parse(text(buf, offset, length));
  }

  @Override
  public @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] buf, int offset, int length) {
    return LocalDateTime.parse(text(buf, offset, length).replace(' ', 'T'));
  }

  @Override
  public @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] buf, int offset, int length) {
    return OffsetDateTime.parse(text(buf, offset, length).replace(' ', 'T'));
  }

  @Override
  public @NonNull OffsetTime decodeTimetz(byte @NonNull [] buf, int offset, int length) {
    return OffsetTime.parse(text(buf, offset, length));
  }

  @Override
  public @NonNull BigDecimal decodeNumeric(byte @NonNull [] buf, int offset, int length) {
    return new BigDecimal(text(buf, offset, length));
  }

  @Override
  public @NonNull String decodeJson(byte @NonNull [] buf, int offset, int length, int typeOID) {
    return text(buf, offset, length);
  }
}
