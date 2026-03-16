package io.djb.mysql.impl.codec;

import io.djb.BinaryCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * MySQL binary format decoder. All values are little-endian. Ported from Vert.x MySQL
 * DataTypeCodec.
 */
public final class MysqlBinaryCodec implements BinaryCodec {

  public static final MysqlBinaryCodec INSTANCE = new MysqlBinaryCodec();

  private MysqlBinaryCodec() {
  }

  // =====================================================================
  // BinaryCodec interface — core scalar types
  // =====================================================================

  @Override
  public boolean decodeBool(byte[] value) {
    return value[0] != 0;
  }

  @Override
  public short decodeInt2(byte[] value) {
    return (short) ((value[0] & 0xFF) | (value[1] & 0xFF) << 8);
  }

  @Override
  public int decodeInt4(byte[] value) {
    return (value[0] & 0xFF) | (value[1] & 0xFF) << 8
        | (value[2] & 0xFF) << 16 | (value[3] & 0xFF) << 24;
  }

  @Override
  public long decodeInt8(byte[] value) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result |= ((long) (value[i] & 0xFF)) << (i * 8);
    }
    return result;
  }

  @Override
  public float decodeFloat4(byte[] value) {
    return Float.intBitsToFloat(decodeInt4(value));
  }

  @Override
  public double decodeFloat8(byte[] value) {
    return Double.longBitsToDouble(decodeInt8(value));
  }

  @Override
  public String decodeString(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  @Override
  public UUID decodeUuid(byte[] value) {
    return UUID.fromString(new String(value, StandardCharsets.UTF_8));
  }

  @Override
  public LocalDate decodeDate(byte[] value) {
    // MySQL binary date: length(1) + year(2LE) + month(1) + day(1)
    if (value.length == 0) {
      return null;
    }
    int len = value[0] & 0xFF;
    if (len == 0) {
      return null;
    }
    int year = (value[1] & 0xFF) | (value[2] & 0xFF) << 8;
    int month = value[3] & 0xFF;
    int day = value[4] & 0xFF;
    return LocalDate.of(year, month, day);
  }

  @Override
  public LocalTime decodeTime(byte[] value) {
    // MySQL binary time: length + is_negative + days + hours + minutes + seconds [+ microseconds]
    if (value.length == 0) {
      return LocalTime.MIDNIGHT;
    }
    int len = value[0] & 0xFF;
    if (len == 0) {
      return LocalTime.MIDNIGHT;
    }
    // Skip is_negative(1) + days(4)
    int hour = value[6] & 0xFF;
    int minute = value[7] & 0xFF;
    int second = value[8] & 0xFF;
    int nano = 0;
    if (len == 12) {
      int micro = (value[9] & 0xFF) | (value[10] & 0xFF) << 8
          | (value[11] & 0xFF) << 16 | (value[12] & 0xFF) << 24;
      nano = micro * 1000;
    }
    return LocalTime.of(hour, minute, second, nano);
  }

  @Override
  public LocalDateTime decodeTimestamp(byte[] value) {
    // MySQL binary datetime: length + year(2LE) + month + day [+ hour + min + sec [+ microsec]]
    if (value.length == 0) {
      return null;
    }
    int len = value[0] & 0xFF;
    if (len == 0) {
      return null;
    }
    int year = (value[1] & 0xFF) | (value[2] & 0xFF) << 8;
    int month = value[3] & 0xFF;
    int day = value[4] & 0xFF;
    if (len == 4) {
      return LocalDateTime.of(year, month, day, 0, 0, 0);
    }
    int hour = value[5] & 0xFF;
    int minute = value[6] & 0xFF;
    int second = value[7] & 0xFF;
    if (len == 7) {
      return LocalDateTime.of(year, month, day, hour, minute, second, 0);
    }
    if (len == 11) {
      int micro = (value[8] & 0xFF) | (value[9] & 0xFF) << 8
          | (value[10] & 0xFF) << 16 | (value[11] & 0xFF) << 24;
      return LocalDateTime.of(year, month, day, hour, minute, second, micro * 1000);
    }
    return LocalDateTime.of(year, month, day, hour, minute, second, 0);
  }

  @Override
  public OffsetDateTime decodeTimestamptz(byte[] value) {
    LocalDateTime ldt = decodeTimestamp(value);
    return ldt == null ? null : OffsetDateTime.of(ldt, ZoneOffset.UTC);
  }

  @Override
  public OffsetTime decodeTimetz(byte[] value) {
    // MySQL has no timetz type; TIME is decoded as LocalTime
    throw new UnsupportedOperationException("MySQL does not support timetz (OffsetTime)");
  }

  @Override
  public byte[] decodeBytes(byte[] value) {
    return value;
  }

  @Override
  public BigDecimal decodeNumeric(byte[] value) {
    return new BigDecimal(new String(value, StandardCharsets.UTF_8));
  }

  @Override
  public String decodeJson(byte[] value, int typeOID) {
    return new String(value, StandardCharsets.UTF_8);
  }

  // =====================================================================
  // Duration-based TIME decode — MySQL TIME can exceed 24h and be negative
  // Ported from Vert.x DataTypeCodec
  // =====================================================================

  public Duration decodeDuration(byte[] value) {
    if (value.length == 0) {
      return Duration.ZERO;
    }
    int len = value[0] & 0xFF;
    if (len == 0) {
      return Duration.ZERO;
    }

    boolean isNegative = (value[1] != 0);
    int days = (value[2] & 0xFF) | (value[3] & 0xFF) << 8
        | (value[4] & 0xFF) << 16 | (value[5] & 0xFF) << 24;
    int hour = value[6] & 0xFF;
    int minute = value[7] & 0xFF;
    int second = value[8] & 0xFF;

    if (isNegative) {
      days = -days;
      hour = -hour;
      minute = -minute;
      second = -second;
    }

    if (len == 8) {
      return Duration.ofDays(days).plusHours(hour).plusMinutes(minute).plusSeconds(second);
    }
    if (len == 12) {
      long microsecond = (value[9] & 0xFF) | (value[10] & 0xFF) << 8
          | (value[11] & 0xFF) << 16 | ((long) (value[12] & 0xFF)) << 24;
      if (isNegative) {
        microsecond = -microsecond;
      }
      return Duration.ofDays(days).plusHours(hour).plusMinutes(minute)
          .plusSeconds(second).plusNanos(microsecond * 1000);
    }
    throw new IllegalArgumentException("Invalid MySQL TIME length: " + len);
  }

  // =====================================================================
  // Unsigned integer decode — MySQL-specific
  // Ported from Vert.x DataTypeCodec
  // =====================================================================

  /**
   * Decode unsigned TINYINT (1 byte) → Short (widened)
   */
  public short decodeUnsignedInt1(byte[] value) {
    return (short) (value[0] & 0xFF);
  }

  /**
   * Decode unsigned SMALLINT (2 bytes LE) → Integer (widened)
   */
  public int decodeUnsignedInt2(byte[] value) {
    return (value[0] & 0xFF) | (value[1] & 0xFF) << 8;
  }

  /**
   * Decode unsigned MEDIUMINT (4 bytes LE, masked to 3) → Integer
   */
  public int decodeUnsignedInt3(byte[] value) {
    return ((value[0] & 0xFF) | (value[1] & 0xFF) << 8
        | (value[2] & 0xFF) << 16 | (value[3] & 0xFF) << 24) & 0xFFFFFF;
  }

  /**
   * Decode unsigned INT (4 bytes LE) → Long (widened)
   */
  public long decodeUnsignedInt4(byte[] value) {
    return ((long) (value[0] & 0xFF)) | ((long) (value[1] & 0xFF)) << 8
        | ((long) (value[2] & 0xFF)) << 16 | ((long) (value[3] & 0xFF)) << 24;
  }

  /**
   * Decode unsigned BIGINT (8 bytes LE) → BigInteger
   */
  public BigInteger decodeUnsignedInt8(byte[] value) {
    // Swap from little-endian to big-endian for BigInteger
    byte[] bigEndian = new byte[8];
    for (int i = 0; i < 8; i++) {
      bigEndian[i] = value[7 - i];
    }
    return new BigInteger(1, bigEndian);
  }

  // =====================================================================
  // BIT type — MySQL-specific
  // Ported from Vert.x DataTypeCodec
  // =====================================================================

  /**
   * Decode BIT field (big-endian byte sequence) → long
   */
  public long decodeBit(byte[] value) {
    long result = 0;
    for (byte b : value) {
      result = (b & 0xFF) | (result << 8);
    }
    return result;
  }

  // =====================================================================
  // Binary encode methods
  // =====================================================================

  public static byte[] encodeInt1(int value) {
    return new byte[]{(byte) value};
  }

  public static byte[] encodeInt2LE(short value) {
    return new byte[]{(byte) value, (byte) (value >> 8)};
  }

  public static byte[] encodeInt4LE(int value) {
    return new byte[]{
        (byte) value, (byte) (value >> 8),
        (byte) (value >> 16), (byte) (value >> 24)
    };
  }

  public static byte[] encodeInt8LE(long value) {
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >> (i * 8));
    }
    return bytes;
  }

  public static byte[] encodeFloat4LE(float value) {
    return encodeInt4LE(Float.floatToIntBits(value));
  }

  public static byte[] encodeFloat8LE(double value) {
    return encodeInt8LE(Double.doubleToLongBits(value));
  }

  public static byte[] encodeString(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // =====================================================================
  // Date/Time/Datetime encode — ported from Vert.x DataTypeCodec
  // =====================================================================

  /**
   * Encode LocalDate to MySQL binary DATE format.
   */
  public static byte[] encodeDate(LocalDate value) {
    return new byte[]{
        4, // length
        (byte) value.getYear(), (byte) (value.getYear() >> 8),
        (byte) value.getMonthValue(),
        (byte) value.getDayOfMonth()
    };
  }

  /**
   * Encode LocalTime to MySQL binary TIME format.
   */
  public static byte[] encodeTime(LocalTime value) {
    int hour = value.getHour();
    int minute = value.getMinute();
    int second = value.getSecond();
    int nano = value.getNano();
    if (nano == 0) {
      if (hour == 0 && minute == 0 && second == 0) {
        return new byte[]{0}; // zero-length
      }
      return new byte[]{
          8, // length
          0, // not negative
          0, 0, 0, 0, // days
          (byte) hour, (byte) minute, (byte) second
      };
    }
    int microSecond = nano / 1000;
    return new byte[]{
        12, // length
        0, // not negative
        0, 0, 0, 0, // days
        (byte) hour, (byte) minute, (byte) second,
        (byte) microSecond, (byte) (microSecond >> 8),
        (byte) (microSecond >> 16), (byte) (microSecond >> 24)
    };
  }

  /**
   * Encode Duration to MySQL binary TIME format (supports >24h and negative).
   */
  public static byte[] encodeDuration(Duration value) {
    long secondsOfDuration = value.getSeconds();
    int nanosOfDuration = value.getNano();
    if (secondsOfDuration == 0 && nanosOfDuration == 0) {
      return new byte[]{0};
    }
    byte isNegative = 0;
    if (secondsOfDuration < 0) {
      isNegative = 1;
      secondsOfDuration = -secondsOfDuration;
    }

    int days = (int) (secondsOfDuration / 86400);
    int secondsOfADay = (int) (secondsOfDuration % 86400);
    int hour = secondsOfADay / 3600;
    int minute = (secondsOfADay % 3600) / 60;
    int second = secondsOfADay % 60;

    if (nanosOfDuration == 0) {
      return new byte[]{
          8, isNegative,
          (byte) days, (byte) (days >> 8), (byte) (days >> 16), (byte) (days >> 24),
          (byte) hour, (byte) minute, (byte) second
      };
    }

    int microSecond;
    if (isNegative == 1 && nanosOfDuration > 0) {
      second = second - 1;
      microSecond = (1_000_000_000 - nanosOfDuration) / 1000;
    } else {
      microSecond = nanosOfDuration / 1000;
    }

    return new byte[]{
        12, isNegative,
        (byte) days, (byte) (days >> 8), (byte) (days >> 16), (byte) (days >> 24),
        (byte) hour, (byte) minute, (byte) second,
        (byte) microSecond, (byte) (microSecond >> 8),
        (byte) (microSecond >> 16), (byte) (microSecond >> 24)
    };
  }

  /**
   * Encode LocalDateTime to MySQL binary DATETIME format.
   */
  public static byte[] encodeDatetime(LocalDateTime value) {
    int year = value.getYear();
    int month = value.getMonthValue();
    int day = value.getDayOfMonth();
    int hour = value.getHour();
    int minute = value.getMinute();
    int second = value.getSecond();
    int microsecond = value.getNano() / 1000;

    if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
      return new byte[]{
          4, // length
          (byte) year, (byte) (year >> 8),
          (byte) month, (byte) day
      };
    } else if (microsecond == 0) {
      return new byte[]{
          7, // length
          (byte) year, (byte) (year >> 8),
          (byte) month, (byte) day,
          (byte) hour, (byte) minute, (byte) second
      };
    } else {
      return new byte[]{
          11, // length
          (byte) year, (byte) (year >> 8),
          (byte) month, (byte) day,
          (byte) hour, (byte) minute, (byte) second,
          (byte) microsecond, (byte) (microsecond >> 8),
          (byte) (microsecond >> 16), (byte) 0
      };
    }
  }
}
