package io.djb;

import java.math.BigDecimal;
import java.time.*;
import java.util.UUID;

/**
 * Decodes binary-format column values from raw bytes.
 * Each database driver provides its own implementation (PG=big-endian, MySQL=little-endian).
 */
public interface BinaryCodec {

    boolean decodeBool(byte[] value);
    short decodeInt2(byte[] value);
    int decodeInt4(byte[] value);
    long decodeInt8(byte[] value);
    float decodeFloat4(byte[] value);
    double decodeFloat8(byte[] value);
    String decodeString(byte[] value);
    UUID decodeUuid(byte[] value);
    LocalDate decodeDate(byte[] value);
    LocalTime decodeTime(byte[] value);
    LocalDateTime decodeTimestamp(byte[] value);
    OffsetDateTime decodeTimestamptz(byte[] value);
    byte[] decodeBytes(byte[] value);
    BigDecimal decodeNumeric(byte[] value);

    /**
     * Decode a JSON/JSONB column to a string.
     * PG JSONB has a 1-byte version prefix that needs stripping.
     */
    String decodeJson(byte[] value, int typeOID);
}
