package io.github.bpdbi.core.test;

import io.github.bpdbi.core.BinaryCodec;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;
import org.jspecify.annotations.NonNull;

/**
 * Minimal BinaryCodec for tests that don't need binary decoding. All decode methods throw; only the
 * default methods (parseTextArray, canDecode, etc.) work.
 */
public final class StubBinaryCodec implements BinaryCodec {

  public static final StubBinaryCodec INSTANCE = new StubBinaryCodec();

  private StubBinaryCodec() {}

  @Override
  public boolean decodeBool(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short decodeInt2(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int decodeInt4(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long decodeInt8(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float decodeFloat4(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double decodeFloat8(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull String decodeString(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull UUID decodeUuid(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull LocalDate decodeDate(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull LocalTime decodeTime(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull OffsetTime decodeTimetz(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte @NonNull [] decodeBytes(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull BigDecimal decodeNumeric(byte @NonNull [] v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull String decodeJson(byte @NonNull [] v, int typeOID) {
    throw new UnsupportedOperationException();
  }
}
