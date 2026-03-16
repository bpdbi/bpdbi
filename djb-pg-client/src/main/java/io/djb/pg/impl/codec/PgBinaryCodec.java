package io.djb.pg.impl.codec;

import io.djb.BinaryCodec;
import io.djb.pg.data.*;

import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL binary format decoder. All values are big-endian.
 * Ported from Vert.x DataTypeCodec.
 */
public final class PgBinaryCodec implements BinaryCodec {

    public static final PgBinaryCodec INSTANCE = new PgBinaryCodec();

    private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime PG_EPOCH_DATETIME = PG_EPOCH_DATE.atStartOfDay();

    // PG OIDs for JSON types
    private static final int OID_JSONB = 3802;

    // Numeric sign constants
    private static final int NUMERIC_POS = 0x0000;
    private static final int NUMERIC_NEG = 0x4000;
    private static final int NUMERIC_NAN = 0xC000;

    private PgBinaryCodec() {}

    // =====================================================================
    // BinaryCodec interface — core scalar types
    // =====================================================================

    @Override
    public boolean decodeBool(byte[] value) {
        return value[0] != 0;
    }

    @Override
    public short decodeInt2(byte[] value) {
        return (short) ((value[0] & 0xFF) << 8 | (value[1] & 0xFF));
    }

    @Override
    public int decodeInt4(byte[] value) {
        return (value[0] & 0xFF) << 24
             | (value[1] & 0xFF) << 16
             | (value[2] & 0xFF) << 8
             | (value[3] & 0xFF);
    }

    @Override
    public long decodeInt8(byte[] value) {
        return ((long)(value[0] & 0xFF) << 56)
             | ((long)(value[1] & 0xFF) << 48)
             | ((long)(value[2] & 0xFF) << 40)
             | ((long)(value[3] & 0xFF) << 32)
             | ((long)(value[4] & 0xFF) << 24)
             | ((long)(value[5] & 0xFF) << 16)
             | ((long)(value[6] & 0xFF) << 8)
             | ((long)(value[7] & 0xFF));
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
        long msb = decodeInt8At(value, 0);
        long lsb = decodeInt8At(value, 8);
        return new UUID(msb, lsb);
    }

    @Override
    public LocalDate decodeDate(byte[] value) {
        int days = decodeInt4(value);
        if (days == Integer.MAX_VALUE) return LocalDate.MAX;
        if (days == Integer.MIN_VALUE) return LocalDate.MIN;
        return PG_EPOCH_DATE.plusDays(days);
    }

    @Override
    public LocalTime decodeTime(byte[] value) {
        long micros = decodeInt8(value);
        return LocalTime.ofNanoOfDay(micros * 1000);
    }

    @Override
    public LocalDateTime decodeTimestamp(byte[] value) {
        long micros = decodeInt8(value);
        if (micros == Long.MAX_VALUE) return LocalDateTime.MAX;
        if (micros == Long.MIN_VALUE) return LocalDateTime.MIN;
        return PG_EPOCH_DATETIME.plus(micros, ChronoUnit.MICROS);
    }

    @Override
    public OffsetDateTime decodeTimestamptz(byte[] value) {
        LocalDateTime ldt = decodeTimestamp(value);
        if (ldt == LocalDateTime.MAX) return OffsetDateTime.MAX;
        if (ldt == LocalDateTime.MIN) return OffsetDateTime.MIN;
        return OffsetDateTime.of(ldt, ZoneOffset.UTC);
    }

    @Override
    public byte[] decodeBytes(byte[] value) {
        return value; // Binary bytea is raw bytes
    }

    @Override
    public BigDecimal decodeNumeric(byte[] value) {
        // PostgreSQL binary NUMERIC format: base-10000 BCD
        // Header: ndigits(2) weight(2) sign(2) dscale(2)
        // Followed by ndigits base-10000 digit groups (each 2 bytes)
        int ndigits = decodeInt2At(value, 0);
        int weight = (short) decodeInt2At(value, 2); // signed
        int sign = decodeInt2At(value, 4);
        int dscale = decodeInt2At(value, 6);

        if (sign == NUMERIC_NAN) {
            throw new ArithmeticException("PostgreSQL NUMERIC value is NaN");
        }

        if (ndigits == 0) {
            return BigDecimal.ZERO.setScale(dscale);
        }

        // Build the decimal string from base-10000 digit groups
        StringBuilder sb = new StringBuilder();
        if (sign == NUMERIC_NEG) {
            sb.append('-');
        }

        // Integer part: groups 0..weight (weight can be negative meaning no integer part)
        if (weight < 0) {
            sb.append('0');
        } else {
            for (int i = 0; i <= weight; i++) {
                int d = (i < ndigits) ? decodeInt2At(value, 8 + i * 2) : 0;
                if (i == 0) {
                    sb.append(d); // first group: no leading zero padding
                } else {
                    sb.append(String.format("%04d", d));
                }
            }
        }

        // Fractional part
        if (dscale > 0) {
            sb.append('.');
            int fracDigitsWritten = 0;
            // Leading zero groups when weight < 0
            for (int i = 0; i < -(weight + 1) && fracDigitsWritten < dscale; i++) {
                for (int j = 0; j < 4 && fracDigitsWritten < dscale; j++) {
                    sb.append('0');
                    fracDigitsWritten++;
                }
            }
            // Actual fractional digit groups
            int startGroup = Math.max(0, weight + 1);
            for (int i = startGroup; i < ndigits && fracDigitsWritten < dscale; i++) {
                int d = decodeInt2At(value, 8 + i * 2);
                String s = String.format("%04d", d);
                for (int j = 0; j < 4 && fracDigitsWritten < dscale; j++) {
                    sb.append(s.charAt(j));
                    fracDigitsWritten++;
                }
            }
            // Pad remaining with zeros
            while (fracDigitsWritten < dscale) {
                sb.append('0');
                fracDigitsWritten++;
            }
        }

        return new BigDecimal(sb.toString());
    }

    @Override
    public String decodeJson(byte[] value, int typeOID) {
        if (typeOID == OID_JSONB && value.length > 0) {
            // JSONB has a 1-byte version prefix (always 1)
            return new String(value, 1, value.length - 1, StandardCharsets.UTF_8);
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    // =====================================================================
    // TIMETZ (OffsetTime) — PG-specific
    // =====================================================================

    public OffsetTime decodeTimetz(byte[] value) {
        long micros = decodeInt8At(value, 0);
        // Zone offset in seconds (negated in PG wire format)
        int offsetSeconds = -(int) decodeInt4At(value, 8);
        return OffsetTime.of(LocalTime.ofNanoOfDay(micros * 1000), ZoneOffset.ofTotalSeconds(offsetSeconds));
    }

    public static byte[] encodeTimetz(OffsetTime value) {
        byte[] result = new byte[12];
        long micros = value.toLocalTime().getLong(ChronoField.MICRO_OF_DAY);
        putInt8(result, 0, micros);
        putInt4(result, 8, -value.getOffset().getTotalSeconds());
        return result;
    }

    // =====================================================================
    // Geometric types — ported from Vert.x DataTypeCodec
    // =====================================================================

    public Point decodePoint(byte[] value) {
        double x = decodeFloat8At(value, 0);
        double y = decodeFloat8At(value, 8);
        return new Point(x, y);
    }

    public static byte[] encodePoint(Point point) {
        byte[] result = new byte[16];
        putFloat8(result, 0, point.x());
        putFloat8(result, 8, point.y());
        return result;
    }

    public Line decodeLine(byte[] value) {
        double a = decodeFloat8At(value, 0);
        double b = decodeFloat8At(value, 8);
        double c = decodeFloat8At(value, 16);
        return new Line(a, b, c);
    }

    public static byte[] encodeLine(Line line) {
        byte[] result = new byte[24];
        putFloat8(result, 0, line.a());
        putFloat8(result, 8, line.b());
        putFloat8(result, 16, line.c());
        return result;
    }

    public LineSegment decodeLseg(byte[] value) {
        Point p1 = new Point(decodeFloat8At(value, 0), decodeFloat8At(value, 8));
        Point p2 = new Point(decodeFloat8At(value, 16), decodeFloat8At(value, 24));
        return new LineSegment(p1, p2);
    }

    public static byte[] encodeLseg(LineSegment lseg) {
        byte[] result = new byte[32];
        putFloat8(result, 0, lseg.p1().x());
        putFloat8(result, 8, lseg.p1().y());
        putFloat8(result, 16, lseg.p2().x());
        putFloat8(result, 24, lseg.p2().y());
        return result;
    }

    public Box decodeBox(byte[] value) {
        Point upperRightCorner = new Point(decodeFloat8At(value, 0), decodeFloat8At(value, 8));
        Point lowerLeftCorner = new Point(decodeFloat8At(value, 16), decodeFloat8At(value, 24));
        return new Box(upperRightCorner, lowerLeftCorner);
    }

    public static byte[] encodeBox(Box box) {
        byte[] result = new byte[32];
        putFloat8(result, 0, box.upperRightCorner().x());
        putFloat8(result, 8, box.upperRightCorner().y());
        putFloat8(result, 16, box.lowerLeftCorner().x());
        putFloat8(result, 24, box.lowerLeftCorner().y());
        return result;
    }

    public Path decodePath(byte[] value) {
        byte first = value[0];
        boolean isOpen;
        if (first == 0) {
            isOpen = true;
        } else if (first == 1) {
            isOpen = false;
        } else {
            throw new IllegalArgumentException("Decoding Path: invalid open/closed byte: " + first);
        }
        int idx = 1;
        int numberOfPoints = decodeInt4At(value, idx);
        idx += 4;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numberOfPoints; i++) {
            double x = decodeFloat8At(value, idx);
            double y = decodeFloat8At(value, idx + 8);
            points.add(new Point(x, y));
            idx += 16;
        }
        return new Path(isOpen, points);
    }

    public static byte[] encodePath(Path path) {
        List<Point> points = path.points();
        byte[] result = new byte[1 + 4 + points.size() * 16];
        result[0] = (byte) (path.isOpen() ? 0 : 1);
        putInt4(result, 1, points.size());
        int idx = 5;
        for (Point point : points) {
            putFloat8(result, idx, point.x());
            putFloat8(result, idx + 8, point.y());
            idx += 16;
        }
        return result;
    }

    public Polygon decodePolygon(byte[] value) {
        int idx = 0;
        int numberOfPoints = decodeInt4At(value, idx);
        idx += 4;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numberOfPoints; i++) {
            double x = decodeFloat8At(value, idx);
            double y = decodeFloat8At(value, idx + 8);
            points.add(new Point(x, y));
            idx += 16;
        }
        return new Polygon(points);
    }

    public static byte[] encodePolygon(Polygon polygon) {
        List<Point> points = polygon.points();
        byte[] result = new byte[4 + points.size() * 16];
        putInt4(result, 0, points.size());
        int idx = 4;
        for (Point point : points) {
            putFloat8(result, idx, point.x());
            putFloat8(result, idx + 8, point.y());
            idx += 16;
        }
        return result;
    }

    public Circle decodeCircle(byte[] value) {
        Point center = new Point(decodeFloat8At(value, 0), decodeFloat8At(value, 8));
        double radius = decodeFloat8At(value, 16);
        return new Circle(center, radius);
    }

    public static byte[] encodeCircle(Circle circle) {
        byte[] result = new byte[24];
        putFloat8(result, 0, circle.centerPoint().x());
        putFloat8(result, 8, circle.centerPoint().y());
        putFloat8(result, 16, circle.radius());
        return result;
    }

    // =====================================================================
    // Interval — ported from Vert.x DataTypeCodec
    // =====================================================================

    public Interval decodeInterval(byte[] value) {
        // Wire format: INT8 microseconds + INT4 days + INT4 months
        long micros = decodeInt8At(value, 0);
        long seconds = micros / 1000000;
        micros -= seconds * 1000000;
        long minutes = seconds / 60;
        seconds -= minutes * 60;
        long hours = minutes / 60;
        minutes -= hours * 60;
        long days = hours / 24;
        hours -= days * 24;
        days += decodeInt4At(value, 8);
        long months = days / 30;
        days -= months * 30;
        months += decodeInt4At(value, 12);
        long years = months / 12;
        months -= years * 12;
        return new Interval((int) years, (int) months, (int) days,
            (int) hours, (int) minutes, (int) seconds, (int) micros);
    }

    public static byte[] encodeInterval(Interval interval) {
        byte[] result = new byte[16];
        int monthsPart = Math.addExact(Math.multiplyExact(interval.years(), 12), interval.months());
        long secondsPart = interval.days() * 24 * 3600L
            + interval.hours() * 3600L
            + interval.minutes() * 60L
            + interval.seconds()
            + interval.microseconds() / 1000000;
        int microsPart = interval.microseconds() % 1000000;

        int months = Math.addExact(monthsPart, Math.toIntExact(secondsPart / 2592000));
        int days = (int) secondsPart % 2592000 / 86400;
        long micros = microsPart + secondsPart % 2592000 % 86400 * 1000000;

        putInt8(result, 0, micros);
        putInt4(result, 8, days);
        putInt4(result, 12, months);
        return result;
    }

    // =====================================================================
    // Money — ported from Vert.x DataTypeCodec
    // =====================================================================

    public Money decodeMoney(byte[] value) {
        long cents = decodeInt8(value);
        return new Money(BigDecimal.valueOf(cents, 2));
    }

    public static byte[] encodeMoney(Money money) {
        return encodeInt8(money.bigDecimalValue().movePointRight(2).longValue());
    }

    // =====================================================================
    // Inet / Cidr — ported from Vert.x DataTypeCodec
    // =====================================================================

    public Inet decodeInet(byte[] value) {
        byte family = value[0];
        byte netmask = value[1];
        // value[2] = is_cidr flag (0 for inet)
        int size = value[3] & 0xFF;
        byte[] data = new byte[size];
        System.arraycopy(value, 4, data, 0, size);
        InetAddress address;
        Integer mask;
        try {
            address = InetAddress.getByAddress(data);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid inet address", e);
        }
        switch (family) {
            case 2: // IPv4
                mask = (netmask & 0xFF) == 32 ? null : Byte.toUnsignedInt(netmask);
                break;
            case 3: // IPv6
                mask = (netmask & 0xFF) == 128 ? null : Byte.toUnsignedInt(netmask);
                break;
            default:
                throw new IllegalArgumentException("Invalid ip family: " + family);
        }
        return new Inet(address, mask);
    }

    public static byte[] encodeInet(Inet value) {
        InetAddress address = value.address();
        byte family;
        byte[] data;
        int netmask;
        if (address instanceof Inet6Address) {
            family = 3;
            data = address.getAddress();
            netmask = value.netmask() == null ? 128 : value.netmask();
        } else if (address instanceof Inet4Address) {
            family = 2;
            data = address.getAddress();
            netmask = value.netmask() == null ? 32 : value.netmask();
        } else {
            throw new IllegalArgumentException("Invalid inet address type");
        }
        byte[] result = new byte[4 + data.length];
        result[0] = family;
        result[1] = (byte) netmask;
        result[2] = 0; // INET
        result[3] = (byte) data.length;
        System.arraycopy(data, 0, result, 4, data.length);
        return result;
    }

    public Cidr decodeCidr(byte[] value) {
        byte family = value[0];
        byte netmask = value[1];
        int size = value[3] & 0xFF;
        byte[] data = new byte[size];
        System.arraycopy(value, 4, data, 0, size);
        InetAddress address;
        try {
            address = InetAddress.getByAddress(data);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid cidr address", e);
        }
        switch (family) {
            case 2, 3: break;
            default: throw new IllegalArgumentException("Invalid IP family: " + family);
        }
        return new Cidr(address, Byte.toUnsignedInt(netmask));
    }

    public static byte[] encodeCidr(Cidr value) {
        InetAddress address = value.address();
        byte family;
        byte[] data;
        int netmask;
        if (address instanceof Inet6Address) {
            family = 3;
            data = address.getAddress();
            netmask = value.netmask() == null ? 128 : value.netmask();
        } else if (address instanceof Inet4Address) {
            family = 2;
            data = address.getAddress();
            netmask = value.netmask() == null ? 32 : value.netmask();
        } else {
            throw new IllegalArgumentException("Invalid inet address type");
        }
        byte[] result = new byte[4 + data.length];
        result[0] = family;
        result[1] = (byte) netmask;
        result[2] = 1; // CIDR
        result[3] = (byte) data.length;
        System.arraycopy(data, 0, result, 4, data.length);
        return result;
    }

    // =====================================================================
    // Binary encode methods (for parameters)
    // =====================================================================

    public static byte[] encodeBool(boolean value) {
        return new byte[]{(byte) (value ? 1 : 0)};
    }

    public static byte[] encodeInt2(short value) {
        return new byte[]{(byte) (value >> 8), (byte) value};
    }

    public static byte[] encodeInt4(int value) {
        return new byte[]{
            (byte) (value >> 24), (byte) (value >> 16),
            (byte) (value >> 8), (byte) value
        };
    }

    public static byte[] encodeInt8(long value) {
        return new byte[]{
            (byte) (value >> 56), (byte) (value >> 48),
            (byte) (value >> 40), (byte) (value >> 32),
            (byte) (value >> 24), (byte) (value >> 16),
            (byte) (value >> 8), (byte) value
        };
    }

    public static byte[] encodeFloat4(float value) {
        return encodeInt4(Float.floatToIntBits(value));
    }

    public static byte[] encodeFloat8(double value) {
        return encodeInt8(Double.doubleToLongBits(value));
    }

    public static byte[] encodeUuid(UUID uuid) {
        byte[] bytes = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (msb >> (56 - i * 8));
            bytes[8 + i] = (byte) (lsb >> (56 - i * 8));
        }
        return bytes;
    }

    public static byte[] encodeDate(LocalDate date) {
        if (date == LocalDate.MAX) return encodeInt4(Integer.MAX_VALUE);
        if (date == LocalDate.MIN) return encodeInt4(Integer.MIN_VALUE);
        return encodeInt4((int) ChronoUnit.DAYS.between(PG_EPOCH_DATE, date));
    }

    public static byte[] encodeTime(LocalTime time) {
        return encodeInt8(time.getLong(ChronoField.MICRO_OF_DAY));
    }

    public static byte[] encodeTimestamp(LocalDateTime ts) {
        long micros = ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, ts);
        return encodeInt8(micros);
    }

    public static byte[] encodeTimestamptz(OffsetDateTime ts) {
        LocalDateTime utc = ts.getOffset().equals(ZoneOffset.UTC)
            ? ts.toLocalDateTime()
            : ts.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
        return encodeTimestamp(utc);
    }

    public static byte[] encodeString(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // =====================================================================
    // Helpers — read at offset
    // =====================================================================

    private long decodeInt8At(byte[] value, int offset) {
        return ((long)(value[offset] & 0xFF) << 56)
             | ((long)(value[offset + 1] & 0xFF) << 48)
             | ((long)(value[offset + 2] & 0xFF) << 40)
             | ((long)(value[offset + 3] & 0xFF) << 32)
             | ((long)(value[offset + 4] & 0xFF) << 24)
             | ((long)(value[offset + 5] & 0xFF) << 16)
             | ((long)(value[offset + 6] & 0xFF) << 8)
             | ((long)(value[offset + 7] & 0xFF));
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

    // Helpers — write at offset

    private static void putInt8(byte[] buf, int offset, long value) {
        buf[offset]     = (byte) (value >> 56);
        buf[offset + 1] = (byte) (value >> 48);
        buf[offset + 2] = (byte) (value >> 40);
        buf[offset + 3] = (byte) (value >> 32);
        buf[offset + 4] = (byte) (value >> 24);
        buf[offset + 5] = (byte) (value >> 16);
        buf[offset + 6] = (byte) (value >> 8);
        buf[offset + 7] = (byte) value;
    }

    private static void putInt4(byte[] buf, int offset, int value) {
        buf[offset]     = (byte) (value >> 24);
        buf[offset + 1] = (byte) (value >> 16);
        buf[offset + 2] = (byte) (value >> 8);
        buf[offset + 3] = (byte) value;
    }

    private static void putFloat8(byte[] buf, int offset, double value) {
        putInt8(buf, offset, Double.doubleToLongBits(value));
    }
}
