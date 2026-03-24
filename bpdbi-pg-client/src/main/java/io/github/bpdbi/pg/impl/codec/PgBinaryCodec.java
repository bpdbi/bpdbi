package io.github.bpdbi.pg.impl.codec;

import io.github.bpdbi.core.BinaryCodec;
import io.github.bpdbi.core.impl.ByteBuffer;
import io.github.bpdbi.pg.data.BitString;
import io.github.bpdbi.pg.data.Box;
import io.github.bpdbi.pg.data.Cidr;
import io.github.bpdbi.pg.data.Circle;
import io.github.bpdbi.pg.data.Inet;
import io.github.bpdbi.pg.data.Interval;
import io.github.bpdbi.pg.data.Line;
import io.github.bpdbi.pg.data.LineSegment;
import io.github.bpdbi.pg.data.Macaddr;
import io.github.bpdbi.pg.data.Macaddr8;
import io.github.bpdbi.pg.data.Money;
import io.github.bpdbi.pg.data.Path;
import io.github.bpdbi.pg.data.Point;
import io.github.bpdbi.pg.data.Polygon;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Postgres binary format decoder. All values are big-endian. Ported from Vert.x DataTypeCodec. */
public final class PgBinaryCodec implements BinaryCodec {

  public static final PgBinaryCodec INSTANCE = new PgBinaryCodec();

  private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
  private static final LocalDateTime PG_EPOCH_DATETIME = PG_EPOCH_DATE.atStartOfDay();

  private static final int NUMERIC_POS = 0x0000;
  private static final int NUMERIC_NEG = 0x4000;
  private static final int NUMERIC_NAN = 0xC000;
  private static final int NUMERIC_PINF = 0xD000;
  private static final int NUMERIC_NINF = 0xF000;
  private static final int NUMERIC_DSCALE_MASK = 0x3FFF;
  private static final BigInteger BI_TEN_THOUSAND = BigInteger.valueOf(10000);
  private static final int[] INT_TEN_POWERS = {1, 10, 100, 1000, 10000};

  private PgBinaryCodec() {}

  // =====================================================================
  // Offset-based scalar decode (zero-copy path). Old methods delegate.
  // =====================================================================

  @Override
  public boolean decodeBool(byte @NonNull [] buf, int offset) {
    return buf[offset] != 0;
  }

  @Override
  public short decodeInt2(byte @NonNull [] buf, int offset) {
    return (short) decodeInt2At(buf, offset);
  }

  @Override
  public int decodeInt4(byte @NonNull [] buf, int offset) {
    return decodeInt4At(buf, offset);
  }

  @Override
  public long decodeInt8(byte @NonNull [] buf, int offset) {
    return decodeInt8At(buf, offset);
  }

  @Override
  public float decodeFloat4(byte @NonNull [] buf, int offset) {
    return Float.intBitsToFloat(decodeInt4At(buf, offset));
  }

  @Override
  public double decodeFloat8(byte @NonNull [] buf, int offset) {
    return Double.longBitsToDouble(decodeInt8At(buf, offset));
  }

  @Override
  public @NonNull String decodeString(byte @NonNull [] buf, int offset, int length) {
    return new String(buf, offset, length, StandardCharsets.UTF_8);
  }

  @Override
  public @NonNull UUID decodeUuid(byte @NonNull [] buf, int offset, int length) {
    long msb = decodeInt8At(buf, offset);
    long lsb = decodeInt8At(buf, offset + 8);
    return new UUID(msb, lsb);
  }

  @Override
  public @NonNull LocalDate decodeDate(byte @NonNull [] buf, int offset, int length) {
    int days = decodeInt4At(buf, offset);
    if (days == Integer.MAX_VALUE) {
      return LocalDate.MAX;
    }
    if (days == Integer.MIN_VALUE) {
      return LocalDate.MIN;
    }
    return PG_EPOCH_DATE.plusDays(days);
  }

  @Override
  public @NonNull LocalTime decodeTime(byte @NonNull [] buf, int offset, int length) {
    long micros = decodeInt8At(buf, offset);
    return LocalTime.ofNanoOfDay(micros * 1000);
  }

  @Override
  public @NonNull LocalDateTime decodeTimestamp(byte @NonNull [] buf, int offset, int length) {
    long micros = decodeInt8At(buf, offset);
    if (micros == Long.MAX_VALUE) {
      return LocalDateTime.MAX;
    }
    if (micros == Long.MIN_VALUE) {
      return LocalDateTime.MIN;
    }
    return PG_EPOCH_DATETIME.plus(micros, ChronoUnit.MICROS);
  }

  @Override
  public @NonNull OffsetDateTime decodeTimestamptz(byte @NonNull [] buf, int offset, int length) {
    LocalDateTime ldt = decodeTimestamp(buf, offset, length);
    if (ldt == LocalDateTime.MAX) {
      return OffsetDateTime.MAX;
    }
    if (ldt == LocalDateTime.MIN) {
      return OffsetDateTime.MIN;
    }
    return OffsetDateTime.of(ldt, ZoneOffset.UTC);
  }

  @Override
  public @NonNull BigDecimal decodeNumeric(byte @NonNull [] buf, int offset, int length) {
    int nDigits = decodeInt2At(buf, offset) & 0xFFFF;
    int weight = (short) decodeInt2At(buf, offset + 2);
    int sign = decodeInt2At(buf, offset + 4);
    int dScale = getDScale(buf, offset, sign);

    if (nDigits == 0) {
      return BigDecimal.ZERO.setScale(dScale, RoundingMode.UNNECESSARY);
    }

    int idx = offset + 8;
    short d = (short) decodeInt2At(buf, idx);

    // --- Path 1: absolute value < 1 (weight < 0) ---
    if (weight < 0) {
      int effectiveScale = dScale;
      weight++;
      if (weight < 0) {
        effectiveScale += 4 * weight;
      }

      int i = 1;
      for (; i < nDigits && d == 0; i++) {
        effectiveScale -= 4;
        idx += 2;
        d = (short) decodeInt2At(buf, idx);
      }

      if (effectiveScale >= 4) {
        effectiveScale -= 4;
      } else {
        d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
        effectiveScale = 0;
      }

      BigInteger unscaledBI = null;
      long unscaledInt = d;
      for (; i < nDigits; i++) {
        if (i == 4 && effectiveScale > 2) {
          unscaledBI = BigInteger.valueOf(unscaledInt);
        }
        idx += 2;
        d = (short) decodeInt2At(buf, idx);
        if (effectiveScale >= 4) {
          if (unscaledBI == null) {
            unscaledInt *= 10000;
          } else {
            unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
          }
          effectiveScale -= 4;
        } else {
          if (unscaledBI == null) {
            unscaledInt *= INT_TEN_POWERS[effectiveScale];
          } else {
            unscaledBI = unscaledBI.multiply(BigInteger.TEN.pow(effectiveScale));
          }
          d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
          effectiveScale = 0;
        }
        if (unscaledBI == null) {
          unscaledInt += d;
        } else if (d != 0) {
          unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
        }
      }

      if (unscaledBI == null) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      if (effectiveScale > 0) {
        unscaledBI = unscaledBI.multiply(BigInteger.TEN.pow(effectiveScale));
      }
      if (sign == NUMERIC_NEG) {
        unscaledBI = unscaledBI.negate();
      }
      return new BigDecimal(unscaledBI, dScale);
    }

    // --- Path 2: integer (scale == 0) ---
    if (dScale == 0) {
      BigInteger unscaledBI = null;
      long unscaledInt = d;
      for (int i = 1; i < nDigits; i++) {
        if (i == 4) {
          unscaledBI = BigInteger.valueOf(unscaledInt);
        }
        idx += 2;
        d = (short) decodeInt2At(buf, idx);
        if (unscaledBI == null) {
          unscaledInt = unscaledInt * 10000 + d;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
          if (d != 0) {
            unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
          }
        }
      }
      if (unscaledBI == null) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      if (sign == NUMERIC_NEG) {
        unscaledBI = unscaledBI.negate();
      }
      int bigDecScale = (nDigits - (weight + 1)) * 4;
      return bigDecScale == 0
          ? new BigDecimal(unscaledBI)
          : new BigDecimal(unscaledBI, bigDecScale).setScale(0, RoundingMode.UNNECESSARY);
    }

    // --- Path 3: general case (has both integer and fractional parts) ---
    BigInteger unscaledBI = null;
    long unscaledInt = d;
    int effectiveWeight = weight;
    int effectiveScale = dScale;
    for (int i = 1; i < nDigits; i++) {
      if (i == 4) {
        unscaledBI = BigInteger.valueOf(unscaledInt);
      }
      idx += 2;
      d = (short) decodeInt2At(buf, idx);
      if (effectiveWeight > 0) {
        effectiveWeight--;
        if (unscaledBI == null) {
          unscaledInt *= 10000;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
        }
      } else if (effectiveScale >= 4) {
        effectiveScale -= 4;
        if (unscaledBI == null) {
          unscaledInt *= 10000;
        } else {
          unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND);
        }
      } else {
        if (unscaledBI == null) {
          unscaledInt *= INT_TEN_POWERS[effectiveScale];
        } else {
          unscaledBI = unscaledBI.multiply(BigInteger.TEN.pow(effectiveScale));
        }
        d = (short) (d / INT_TEN_POWERS[4 - effectiveScale]);
        effectiveScale = 0;
      }
      if (unscaledBI == null) {
        unscaledInt += d;
      } else if (d != 0) {
        unscaledBI = unscaledBI.add(BigInteger.valueOf(d));
      }
    }

    if (unscaledBI == null) {
      unscaledBI = BigInteger.valueOf(unscaledInt);
    }
    if (effectiveWeight > 0) {
      unscaledBI = unscaledBI.multiply(BigInteger.TEN.pow(effectiveWeight * 4));
    }
    if (effectiveScale > 0) {
      unscaledBI = unscaledBI.multiply(BigInteger.TEN.pow(effectiveScale));
    }
    if (sign == NUMERIC_NEG) {
      unscaledBI = unscaledBI.negate();
    }
    return new BigDecimal(unscaledBI, dScale);
  }

  private int getDScale(byte @NonNull [] buf, int offset, int sign) {
    int dScale = decodeInt2At(buf, offset + 6);

    switch (sign) {
      case NUMERIC_POS, NUMERIC_NEG -> {}
      case NUMERIC_NAN -> throw new ArithmeticException("Postgres NUMERIC value is NaN");
      case NUMERIC_PINF ->
          throw new ArithmeticException("Postgres NUMERIC value is positive infinity");
      case NUMERIC_NINF ->
          throw new ArithmeticException("Postgres NUMERIC value is negative infinity");
      default ->
          throw new IllegalArgumentException(
              "Invalid sign in NUMERIC value: 0x" + Integer.toHexString(sign));
    }

    if ((dScale & NUMERIC_DSCALE_MASK) != dScale) {
      throw new IllegalArgumentException("Invalid scale in NUMERIC value: " + dScale);
    }
    return dScale;
  }

  @Override
  public @NonNull String decodeJson(byte @NonNull [] buf, int offset, int length, int typeOID) {
    if (typeOID == PgOIDs.JSONB && length > 0) {
      return new String(buf, offset + 1, length - 1, StandardCharsets.UTF_8);
    }
    return new String(buf, offset, length, StandardCharsets.UTF_8);
  }

  // =====================================================================
  // TIMETZ (OffsetTime) — PG-specific
  // =====================================================================

  @Override
  public @NonNull OffsetTime decodeTimetz(byte @NonNull [] buf, int offset, int length) {
    long micros = decodeInt8At(buf, offset);
    int offsetSeconds = -decodeInt4At(buf, offset + 8);
    return OffsetTime.of(
        LocalTime.ofNanoOfDay(micros * 1000), ZoneOffset.ofTotalSeconds(offsetSeconds));
  }

  /** Used by tests only (round-trip verification). */
  public static byte @NonNull [] encodeTimetz(@NonNull OffsetTime value) {
    byte[] result = new byte[12];
    long micros = value.toLocalTime().getLong(ChronoField.MICRO_OF_DAY);
    putInt8(result, 0, micros);
    putInt4(result, 8, -value.getOffset().getTotalSeconds());
    return result;
  }

  // =====================================================================
  // Geometric types — ported from Vert.x DataTypeCodec.
  // Encode methods in this section are used by tests only (round-trip verification).
  // =====================================================================

  public @NonNull Point decodePoint(byte @NonNull [] buf, int off) {
    return new Point(decodeFloat8At(buf, off), decodeFloat8At(buf, off + 8));
  }

  public @NonNull Point decodePoint(byte @NonNull [] value) {
    return decodePoint(value, 0);
  }

  public static byte @NonNull [] encodePoint(@NonNull Point point) {
    byte[] result = new byte[16];
    putFloat8(result, 0, point.x());
    putFloat8(result, 8, point.y());
    return result;
  }

  public @NonNull Line decodeLine(byte @NonNull [] buf, int off) {
    return new Line(
        decodeFloat8At(buf, off), decodeFloat8At(buf, off + 8), decodeFloat8At(buf, off + 16));
  }

  public @NonNull Line decodeLine(byte @NonNull [] value) {
    return decodeLine(value, 0);
  }

  public static byte @NonNull [] encodeLine(@NonNull Line line) {
    byte[] result = new byte[24];
    putFloat8(result, 0, line.a());
    putFloat8(result, 8, line.b());
    putFloat8(result, 16, line.c());
    return result;
  }

  public @NonNull LineSegment decodeLseg(byte @NonNull [] buf, int off) {
    Point p1 = new Point(decodeFloat8At(buf, off), decodeFloat8At(buf, off + 8));
    Point p2 = new Point(decodeFloat8At(buf, off + 16), decodeFloat8At(buf, off + 24));
    return new LineSegment(p1, p2);
  }

  public @NonNull LineSegment decodeLseg(byte @NonNull [] value) {
    return decodeLseg(value, 0);
  }

  public static byte @NonNull [] encodeLseg(@NonNull LineSegment lseg) {
    byte[] result = new byte[32];
    putFloat8(result, 0, lseg.p1().x());
    putFloat8(result, 8, lseg.p1().y());
    putFloat8(result, 16, lseg.p2().x());
    putFloat8(result, 24, lseg.p2().y());
    return result;
  }

  public @NonNull Box decodeBox(byte @NonNull [] buf, int off) {
    Point ur = new Point(decodeFloat8At(buf, off), decodeFloat8At(buf, off + 8));
    Point ll = new Point(decodeFloat8At(buf, off + 16), decodeFloat8At(buf, off + 24));
    return new Box(ur, ll);
  }

  public @NonNull Box decodeBox(byte @NonNull [] value) {
    return decodeBox(value, 0);
  }

  public static byte @NonNull [] encodeBox(@NonNull Box box) {
    byte[] result = new byte[32];
    putFloat8(result, 0, box.upperRightCorner().x());
    putFloat8(result, 8, box.upperRightCorner().y());
    putFloat8(result, 16, box.lowerLeftCorner().x());
    putFloat8(result, 24, box.lowerLeftCorner().y());
    return result;
  }

  public @NonNull Path decodePath(byte @NonNull [] buf, int off) {
    byte first = buf[off];
    boolean isOpen;
    if (first == 0) {
      isOpen = true;
    } else if (first == 1) {
      isOpen = false;
    } else {
      throw new IllegalArgumentException("Decoding Path: invalid open/closed byte: " + first);
    }
    int numberOfPoints = decodeInt4At(buf, off + 1);
    List<Point> points = decodePointList(buf, off + 5, numberOfPoints);
    return new Path(isOpen, points);
  }

  public @NonNull Path decodePath(byte @NonNull [] value) {
    return decodePath(value, 0);
  }

  public static byte @NonNull [] encodePath(@NonNull Path path) {
    List<Point> points = path.points();
    byte[] result = new byte[1 + 4 + points.size() * 16];
    result[0] = (byte) (path.isOpen() ? 0 : 1);
    putInt4(result, 1, points.size());
    encodePointList(points, result, 5);
    return result;
  }

  public @NonNull Polygon decodePolygon(byte @NonNull [] buf, int off) {
    int numberOfPoints = decodeInt4At(buf, off);
    List<Point> points = decodePointList(buf, off + 4, numberOfPoints);
    return new Polygon(points);
  }

  public @NonNull Polygon decodePolygon(byte @NonNull [] value) {
    return decodePolygon(value, 0);
  }

  public static byte @NonNull [] encodePolygon(@NonNull Polygon polygon) {
    List<Point> points = polygon.points();
    byte[] result = new byte[4 + points.size() * 16];
    putInt4(result, 0, points.size());
    encodePointList(points, result, 4);
    return result;
  }

  private List<Point> decodePointList(byte @NonNull [] value, int startIdx, int count) {
    List<Point> points = new ArrayList<>(count);
    int idx = startIdx;
    for (int i = 0; i < count; i++) {
      points.add(new Point(decodeFloat8At(value, idx), decodeFloat8At(value, idx + 8)));
      idx += 16;
    }
    return points;
  }

  private static void encodePointList(List<Point> points, byte[] result, int startIdx) {
    int idx = startIdx;
    for (Point point : points) {
      putFloat8(result, idx, point.x());
      putFloat8(result, idx + 8, point.y());
      idx += 16;
    }
  }

  public @NonNull Circle decodeCircle(byte @NonNull [] buf, int off) {
    Point center = new Point(decodeFloat8At(buf, off), decodeFloat8At(buf, off + 8));
    return new Circle(center, decodeFloat8At(buf, off + 16));
  }

  public @NonNull Circle decodeCircle(byte @NonNull [] value) {
    return decodeCircle(value, 0);
  }

  public static byte @NonNull [] encodeCircle(@NonNull Circle circle) {
    byte[] result = new byte[24];
    putFloat8(result, 0, circle.centerPoint().x());
    putFloat8(result, 8, circle.centerPoint().y());
    putFloat8(result, 16, circle.radius());
    return result;
  }

  // =====================================================================
  // Interval
  // =====================================================================

  public @NonNull Interval decodeInterval(byte @NonNull [] buf, int off) {
    long totalMicros = decodeInt8At(buf, off);
    int wireDays = decodeInt4At(buf, off + 8);
    int wireMonths = decodeInt4At(buf, off + 12);

    long seconds = totalMicros / 1_000_000;
    long micros = totalMicros - seconds * 1_000_000;
    long minutes = seconds / 60;
    seconds -= minutes * 60;
    long hours = minutes / 60;
    minutes -= hours * 60;

    int years = wireMonths / 12;
    int months = wireMonths - years * 12;

    return new Interval(
        years, months, wireDays, (int) hours, (int) minutes, (int) seconds, (int) micros);
  }

  public @NonNull Interval decodeInterval(byte @NonNull [] value) {
    return decodeInterval(value, 0);
  }

  public static byte @NonNull [] encodeInterval(@NonNull Interval interval) {
    byte[] result = new byte[16];
    int months = Math.addExact(Math.multiplyExact(interval.years(), 12), interval.months());
    int days = interval.days();
    long micros =
        interval.hours() * 3_600_000_000L
            + interval.minutes() * 60_000_000L
            + interval.seconds() * 1_000_000L
            + interval.microseconds();
    putInt8(result, 0, micros);
    putInt4(result, 8, days);
    putInt4(result, 12, months);
    return result;
  }

  // =====================================================================
  // Money
  // =====================================================================

  public @NonNull Money decodeMoney(byte @NonNull [] buf, int off) {
    long cents = decodeInt8(buf, off);
    return new Money(BigDecimal.valueOf(cents, 2));
  }

  public @NonNull Money decodeMoney(byte @NonNull [] value) {
    return decodeMoney(value, 0);
  }

  public static byte @NonNull [] encodeMoney(@NonNull Money money) {
    return encodeInt8(money.bigDecimalValue().movePointRight(2).longValue());
  }

  // =====================================================================
  // Inet / Cidr
  // =====================================================================

  public @NonNull Inet decodeInet(byte @NonNull [] buf, int off) {
    byte family = buf[off];
    byte netmask = buf[off + 1];
    InetAddress address = decodeInetAddress(buf, off, "inet");
    Integer mask =
        switch (family) {
          case 2 -> (netmask & 0xFF) == 32 ? null : Byte.toUnsignedInt(netmask);
          case 3 -> (netmask & 0xFF) == 128 ? null : Byte.toUnsignedInt(netmask);
          default -> throw new IllegalArgumentException("Invalid ip family: " + family);
        };
    return new Inet(address, mask);
  }

  public @NonNull Inet decodeInet(byte @NonNull [] value) {
    return decodeInet(value, 0);
  }

  public static byte @NonNull [] encodeInet(@NonNull Inet value) {
    return encodeInetOrCidrToBytes(value.address(), value.netmask(), true);
  }

  public @NonNull Cidr decodeCidr(byte @NonNull [] buf, int off) {
    byte family = buf[off];
    byte netmask = buf[off + 1];
    InetAddress address = decodeInetAddress(buf, off, "cidr");
    switch (family) {
      case 2, 3:
        break;
      default:
        throw new IllegalArgumentException("Invalid IP family: " + family);
    }
    return new Cidr(address, Byte.toUnsignedInt(netmask));
  }

  public @NonNull Cidr decodeCidr(byte @NonNull [] value) {
    return decodeCidr(value, 0);
  }

  public static byte @NonNull [] encodeCidr(@NonNull Cidr value) {
    return encodeInetOrCidrToBytes(value.address(), value.netmask(), false);
  }

  private static @NonNull InetAddress decodeInetAddress(
      byte @NonNull [] buf, int off, String typeName) {
    int size = buf[off + 3] & 0xFF;
    byte[] data = new byte[size];
    System.arraycopy(buf, off + 4, data, 0, size);
    try {
      return InetAddress.getByAddress(data);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid " + typeName + " address", e);
    }
  }

  private static byte @NonNull [] encodeInetOrCidrToBytes(
      InetAddress address, Integer value1, boolean isInet) {
    byte family;
    byte[] data;
    int netmask;
    if (address instanceof Inet6Address) {
      family = 3;
      data = address.getAddress();
      netmask = value1 == null ? 128 : value1;
    } else if (address instanceof Inet4Address) {
      family = 2;
      data = address.getAddress();
      netmask = value1 == null ? 32 : value1;
    } else {
      throw new IllegalArgumentException("Invalid inet address type");
    }
    byte[] result = new byte[4 + data.length];
    result[0] = family;
    result[1] = (byte) netmask;
    result[2] = (byte) (isInet ? 0 : 1);
    result[3] = (byte) data.length;
    System.arraycopy(data, 0, result, 4, data.length);
    return result;
  }

  // =====================================================================
  // Macaddr / Macaddr8
  // =====================================================================

  public @NonNull Macaddr decodeMacaddr(byte @NonNull [] buf, int off) {
    byte[] addr = new byte[6];
    System.arraycopy(buf, off, addr, 0, 6);
    return new Macaddr(addr);
  }

  public @NonNull Macaddr decodeMacaddr(byte @NonNull [] value) {
    return decodeMacaddr(value, 0);
  }

  public static byte @NonNull [] encodeMacaddr(@NonNull Macaddr value) {
    byte[] result = new byte[6];
    System.arraycopy(value.address(), 0, result, 0, 6);
    return result;
  }

  public @NonNull Macaddr8 decodeMacaddr8(byte @NonNull [] buf, int off) {
    byte[] addr = new byte[8];
    System.arraycopy(buf, off, addr, 0, 8);
    return new Macaddr8(addr);
  }

  public @NonNull Macaddr8 decodeMacaddr8(byte @NonNull [] value) {
    return decodeMacaddr8(value, 0);
  }

  public static byte @NonNull [] encodeMacaddr8(@NonNull Macaddr8 value) {
    byte[] result = new byte[8];
    System.arraycopy(value.address(), 0, result, 0, 8);
    return result;
  }

  // =====================================================================
  // BitString (bit / varbit)
  // =====================================================================

  public @NonNull BitString decodeBitString(byte @NonNull [] buf, int off) {
    int bitCount = decodeInt4At(buf, off);
    int byteCount = (bitCount + 7) / 8;
    byte[] data = new byte[byteCount];
    System.arraycopy(buf, off + 4, data, 0, byteCount);
    return new BitString(bitCount, data);
  }

  public @NonNull BitString decodeBitString(byte @NonNull [] value) {
    return decodeBitString(value, 0);
  }

  public static byte @NonNull [] encodeBitString(@NonNull BitString value) {
    byte[] data = value.bytes();
    byte[] result = new byte[4 + data.length];
    putInt4(result, 0, value.bitCount());
    System.arraycopy(data, 0, result, 4, data.length);
    return result;
  }

  // =====================================================================
  // Tsvector
  // =====================================================================

  public @NonNull String decodeTsVector(byte @NonNull [] buf, int off) {
    int idx = off;
    int numLexemes = decodeInt4At(buf, idx);
    idx += 4;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numLexemes; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      int start = idx;
      while (buf[idx] != 0) {
        idx++;
      }
      sb.append('\'')
          .append(new String(buf, start, idx - start, StandardCharsets.UTF_8))
          .append('\'');
      idx++;
      int numPositions = decodeInt2At(buf, idx);
      idx += 2;
      if (numPositions > 0) {
        sb.append(':');
        for (int j = 0; j < numPositions; j++) {
          if (j > 0) {
            sb.append(',');
          }
          int posWithWeight = decodeInt2At(buf, idx);
          idx += 2;
          sb.append(posWithWeight & 0x3FFF);
          switch (posWithWeight >>> 14) {
            case 3 -> sb.append('A');
            case 2 -> sb.append('B');
            case 1 -> sb.append('C');
            default -> {}
          }
        }
      }
    }
    return sb.toString();
  }

  public @NonNull String decodeTsVector(byte @NonNull [] value) {
    return decodeTsVector(value, 0);
  }

  // =====================================================================
  // Tsquery
  // =====================================================================

  private static final int QI_VAL = 1;
  private static final int QI_OPR = 2;
  private static final int OP_NOT = 1;
  private static final int OP_AND = 2;
  private static final int OP_OR = 3;
  private static final int OP_PHRASE = 4;

  public @NonNull String decodeTsQuery(byte @NonNull [] buf, int off) {
    int idx = off;
    int numItems = decodeInt4At(buf, idx);
    idx += 4;
    if (numItems == 0) {
      return "";
    }
    var stack = new ArrayList<String>();
    for (int i = 0; i < numItems; i++) {
      int type = buf[idx] & 0xFF;
      idx++;
      if (type == QI_VAL) {
        int weightBits = buf[idx] & 0xFF;
        idx++;
        boolean isPrefix = (buf[idx] & 0xFF) != 0;
        idx++;
        int start = idx;
        while (buf[idx] != 0) {
          idx++;
        }
        String word = new String(buf, start, idx - start, StandardCharsets.UTF_8);
        idx++;
        StringBuilder operand = new StringBuilder();
        operand.append('\'').append(word).append('\'');
        if (isPrefix) {
          operand.append(':').append('*');
        }
        if (weightBits != 0) {
          if (!isPrefix) {
            operand.append(':');
          }
          if ((weightBits & 0x08) != 0) operand.append('A');
          if ((weightBits & 0x04) != 0) operand.append('B');
          if ((weightBits & 0x02) != 0) operand.append('C');
          if ((weightBits & 0x01) != 0) operand.append('D');
        }
        stack.add(operand.toString());
      } else if (type == QI_OPR) {
        int oper = buf[idx] & 0xFF;
        idx++;
        if (oper == OP_NOT) {
          stack.add("!" + (stack.isEmpty() ? "?" : stack.removeLast()));
        } else {
          int distance = 0;
          if (oper == OP_PHRASE) {
            distance = decodeInt2At(buf, idx);
            idx += 2;
          }
          String right = stack.isEmpty() ? "?" : stack.removeLast();
          String left = stack.isEmpty() ? "?" : stack.removeLast();
          String op =
              switch (oper) {
                case OP_AND -> " & ";
                case OP_OR -> " | ";
                case OP_PHRASE -> distance == 1 ? " <-> " : " <" + distance + "> ";
                default -> " ? ";
              };
          stack.add("( " + left + op + right + " )");
        }
      }
    }
    return stack.isEmpty() ? "" : stack.getLast();
  }

  public @NonNull String decodeTsQuery(byte @NonNull [] value) {
    return decodeTsQuery(value, 0);
  }

  // =====================================================================
  // Binary array decode — PG binary array wire format
  // =====================================================================

  @Override
  public <T> @Nullable List<T> decodeArray(
      byte @NonNull [] value, BinaryCodec.@NonNull ElementDecoder<T> elementDecoder) {
    if (value.length < 12) {
      return List.of();
    }
    int nDim = decodeInt4At(value, 0);
    if (nDim == 0) {
      return List.of();
    }
    if (nDim != 1) {
      return null;
    }
    int dimension = decodeInt4At(value, 12);
    int idx = 20;
    List<T> result = new ArrayList<>(dimension);
    for (int i = 0; i < dimension; i++) {
      int elemLen = decodeInt4At(value, idx);
      idx += 4;
      if (elemLen == -1) {
        result.add(null);
        continue;
      }
      result.add(elementDecoder.decode(value, idx, elemLen));
      idx += elemLen;
    }
    return result;
  }

  // =====================================================================
  // Binary encode methods. Used by tests for round-trip verification of decoders.
  // Only encodeNumeric() is called from production code (via BinaryParamEncoder).
  // =====================================================================

  public static byte @NonNull [] encodeBool(boolean value) {
    return new byte[] {(byte) (value ? 1 : 0)};
  }

  public static byte @NonNull [] encodeInt2(short value) {
    return new byte[] {(byte) (value >> 8), (byte) value};
  }

  public static byte @NonNull [] encodeInt4(int value) {
    return new byte[] {
      (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
    };
  }

  public static byte @NonNull [] encodeInt8(long value) {
    return new byte[] {
      (byte) (value >> 56),
      (byte) (value >> 48),
      (byte) (value >> 40),
      (byte) (value >> 32),
      (byte) (value >> 24),
      (byte) (value >> 16),
      (byte) (value >> 8),
      (byte) value
    };
  }

  public static byte @NonNull [] encodeFloat4(float value) {
    return encodeInt4(Float.floatToRawIntBits(value));
  }

  public static byte @NonNull [] encodeFloat8(double value) {
    return encodeInt8(Double.doubleToRawLongBits(value));
  }

  public static void encodeNumeric(@NonNull BigDecimal value, @NonNull ByteBuffer buf) {
    int dScale = Math.max(0, value.scale());

    if (value.signum() == 0) {
      buf.writeInt(8);
      buf.writeShort(0); // nDigits
      buf.writeShort(0); // weight
      buf.writeShort(NUMERIC_POS);
      buf.writeShort(dScale);
      return;
    }

    int sign = value.signum() > 0 ? NUMERIC_POS : NUMERIC_NEG;

    // Get all significant digits as a decimal string
    BigInteger unscaled = value.unscaledValue().abs();
    String digits = unscaled.toString();
    int scale = value.scale();

    // For negative scale (e.g. 1.2E+10), expand trailing zeros
    if (scale < 0) {
      digits = digits + "0".repeat(-scale);
      scale = 0;
    }

    // Number of digits before/after the decimal point
    int intLen = digits.length() - scale;

    // If intLen <= 0, prepend zeros so all digits are fractional
    if (intLen <= 0) {
      digits = "0".repeat(-intLen) + digits;
      intLen = 0;
    }

    // Pad so integer part is a multiple of 4 (left), and the fractional part is a multiple of 4
    // (right)
    int leftPad = (4 - (intLen % 4)) % 4;
    int fracLen = digits.length() - intLen;
    int rightPad = fracLen > 0 ? (4 - (fracLen % 4)) % 4 : 0;
    String padded = "0".repeat(leftPad) + digits + "0".repeat(rightPad);

    // Split into base-10000 groups
    int nDigits = padded.length() / 4;
    int weight = (intLen + leftPad) / 4 - 1;

    // Strip trailing zero groups (they're implied by dScale)
    int actualNDigits = nDigits;
    while (actualNDigits > 0) {
      int groupStart = (actualNDigits - 1) * 4;
      if (parseGroup(padded, groupStart) != 0) break;
      actualNDigits--;
    }

    // header: 4 shorts = 8 bytes, plus 2 bytes per digit group
    buf.writeInt(8 + actualNDigits * 2);
    buf.writeShort(actualNDigits);
    buf.writeShort(weight);
    buf.writeShort(sign);
    buf.writeShort(dScale);
    for (int i = 0; i < actualNDigits; i++) {
      buf.writeShort(parseGroup(padded, i * 4));
    }
  }

  private static int parseGroup(@NonNull String s, int offset) {
    return (s.charAt(offset) - '0') * 1000
        + (s.charAt(offset + 1) - '0') * 100
        + (s.charAt(offset + 2) - '0') * 10
        + (s.charAt(offset + 3) - '0');
  }

  public static byte @NonNull [] encodeUuid(@NonNull UUID uuid) {
    byte[] bytes = new byte[16];
    putInt8(bytes, 0, uuid.getMostSignificantBits());
    putInt8(bytes, 8, uuid.getLeastSignificantBits());
    return bytes;
  }

  public static byte @NonNull [] encodeDate(@NonNull LocalDate date) {
    if (date == LocalDate.MAX) {
      return encodeInt4(Integer.MAX_VALUE);
    }
    if (date == LocalDate.MIN) {
      return encodeInt4(Integer.MIN_VALUE);
    }
    return encodeInt4((int) ChronoUnit.DAYS.between(PG_EPOCH_DATE, date));
  }

  public static byte @NonNull [] encodeTime(@NonNull LocalTime time) {
    return encodeInt8(time.getLong(ChronoField.MICRO_OF_DAY));
  }

  public static byte @NonNull [] encodeTimestamp(@NonNull LocalDateTime ts) {
    return encodeInt8(ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, ts));
  }

  public static byte @NonNull [] encodeTimestamptz(@NonNull OffsetDateTime ts) {
    LocalDateTime utc =
        ts.getOffset().equals(ZoneOffset.UTC)
            ? ts.toLocalDateTime()
            : ts.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
    return encodeTimestamp(utc);
  }

  public static byte @NonNull [] encodeString(@NonNull String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // =====================================================================
  // canDecode / decode — PG-specific type dispatch
  // =====================================================================

  @Override
  public boolean canDecode(@NonNull Class<?> type) {
    return BinaryCodec.super.canDecode(type)
        || type == Point.class
        || type == Line.class
        || type == LineSegment.class
        || type == Box.class
        || type == Path.class
        || type == Polygon.class
        || type == Circle.class
        || type == Interval.class
        || type == Money.class
        || type == Inet.class
        || type == Cidr.class
        || type == Macaddr.class
        || type == Macaddr8.class
        || type == BitString.class;
  }

  @SuppressWarnings("unchecked") // safe: each branch returns the exact type requested
  @Override
  public <T> @NonNull T decode(
      byte @NonNull [] buf, int offset, int length, @NonNull Class<T> type) {
    Object result;
    if (type == String.class) {
      result = decodeString(buf, offset, length);
    } else if (type == Integer.class) {
      result = decodeInt4(buf, offset);
    } else if (type == Long.class) {
      result = decodeInt8(buf, offset);
    } else if (type == Short.class) {
      result = decodeInt2(buf, offset);
    } else if (type == Float.class) {
      result = decodeFloat4(buf, offset);
    } else if (type == Double.class) {
      result = decodeFloat8(buf, offset);
    } else if (type == Boolean.class) {
      result = decodeBool(buf, offset);
    } else if (type == BigDecimal.class) {
      result = decodeNumeric(buf, offset, length);
    } else if (type == UUID.class) {
      result = decodeUuid(buf, offset, length);
    } else if (type == LocalDate.class) {
      result = decodeDate(buf, offset, length);
    } else if (type == LocalTime.class) {
      result = decodeTime(buf, offset, length);
    } else if (type == LocalDateTime.class) {
      result = decodeTimestamp(buf, offset, length);
    } else if (type == OffsetDateTime.class) {
      result = decodeTimestamptz(buf, offset, length);
    } else if (type == OffsetTime.class) {
      result = decodeTimetz(buf, offset, length);
    } else if (type == Instant.class) {
      result = decodeTimestamptz(buf, offset, length).toInstant();
    } else if (type == byte[].class) {
      result = decodeBytes(buf, offset, length);
    } else if (type == Point.class) {
      result = decodePoint(buf, offset);
    } else if (type == Line.class) {
      result = decodeLine(buf, offset);
    } else if (type == LineSegment.class) {
      result = decodeLseg(buf, offset);
    } else if (type == Box.class) {
      result = decodeBox(buf, offset);
    } else if (type == Path.class) {
      result = decodePath(buf, offset);
    } else if (type == Polygon.class) {
      result = decodePolygon(buf, offset);
    } else if (type == Circle.class) {
      result = decodeCircle(buf, offset);
    } else if (type == Interval.class) {
      result = decodeInterval(buf, offset);
    } else if (type == Money.class) {
      result = decodeMoney(buf, offset);
    } else if (type == Inet.class) {
      result = decodeInet(buf, offset);
    } else if (type == Cidr.class) {
      result = decodeCidr(buf, offset);
    } else if (type == Macaddr.class) {
      result = decodeMacaddr(buf, offset);
    } else if (type == Macaddr8.class) {
      result = decodeMacaddr8(buf, offset);
    } else if (type == BitString.class) {
      result = decodeBitString(buf, offset);
    } else {
      return BinaryCodec.super.decode(buf, offset, length, type);
    }
    return (T) result;
  }

  @SuppressWarnings("unchecked") // safe: each branch returns the exact type requested
  @Override
  public <T> @NonNull T decode(byte @NonNull [] value, @NonNull Class<T> type) {
    Object result;
    if (type == Point.class) {
      result = decodePoint(value);
    } else if (type == Line.class) {
      result = decodeLine(value);
    } else if (type == LineSegment.class) {
      result = decodeLseg(value);
    } else if (type == Box.class) {
      result = decodeBox(value);
    } else if (type == Path.class) {
      result = decodePath(value);
    } else if (type == Polygon.class) {
      result = decodePolygon(value);
    } else if (type == Circle.class) {
      result = decodeCircle(value);
    } else if (type == Interval.class) {
      result = decodeInterval(value);
    } else if (type == Money.class) {
      result = decodeMoney(value);
    } else if (type == Inet.class) {
      result = decodeInet(value);
    } else if (type == Cidr.class) {
      result = decodeCidr(value);
    } else if (type == Macaddr.class) {
      result = decodeMacaddr(value);
    } else if (type == Macaddr8.class) {
      result = decodeMacaddr8(value);
    } else if (type == BitString.class) {
      result = decodeBitString(value);
    } else {
      return BinaryCodec.super.decode(value, type);
    }
    return (T) result;
  }

  // =====================================================================
  // Helpers — read/write at offset
  // =====================================================================

  private long decodeInt8At(byte[] value, int offset) {
    return ((long) (value[offset] & 0xFF) << 56)
        | ((long) (value[offset + 1] & 0xFF) << 48)
        | ((long) (value[offset + 2] & 0xFF) << 40)
        | ((long) (value[offset + 3] & 0xFF) << 32)
        | ((long) (value[offset + 4] & 0xFF) << 24)
        | ((long) (value[offset + 5] & 0xFF) << 16)
        | ((long) (value[offset + 6] & 0xFF) << 8)
        | ((long) (value[offset + 7] & 0xFF));
  }

  private int decodeInt4At(byte[] value, int offset) {
    return (value[offset] & 0xFF) << 24
        | (value[offset + 1] & 0xFF) << 16
        | (value[offset + 2] & 0xFF) << 8
        | (value[offset + 3] & 0xFF);
  }

  private int decodeInt2At(byte[] value, int offset) {
    return (value[offset] & 0xFF) << 8 | (value[offset + 1] & 0xFF);
  }

  private double decodeFloat8At(byte[] value, int offset) {
    return Double.longBitsToDouble(decodeInt8At(value, offset));
  }

  private static void putInt8(byte[] buf, int offset, long value) {
    buf[offset] = (byte) (value >> 56);
    buf[offset + 1] = (byte) (value >> 48);
    buf[offset + 2] = (byte) (value >> 40);
    buf[offset + 3] = (byte) (value >> 32);
    buf[offset + 4] = (byte) (value >> 24);
    buf[offset + 5] = (byte) (value >> 16);
    buf[offset + 6] = (byte) (value >> 8);
    buf[offset + 7] = (byte) value;
  }

  private static void putInt4(byte[] buf, int offset, int value) {
    buf[offset] = (byte) (value >> 24);
    buf[offset + 1] = (byte) (value >> 16);
    buf[offset + 2] = (byte) (value >> 8);
    buf[offset + 3] = (byte) value;
  }

  private static void putFloat8(byte[] buf, int offset, double value) {
    putInt8(buf, offset, Double.doubleToLongBits(value));
  }
}
