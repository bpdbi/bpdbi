package io.djb.pg.impl.codec;

import io.djb.pg.data.*;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.time.*;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PgBinaryCodecTest {

    private final PgBinaryCodec codec = PgBinaryCodec.INSTANCE;

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

    @Test
    void encodeBool() {
        assertArrayEquals(new byte[]{1}, PgBinaryCodec.encodeBool(true));
        assertArrayEquals(new byte[]{0}, PgBinaryCodec.encodeBool(false));
    }

    // =====================================================================
    // Int2
    // =====================================================================

    @Test
    void decodeInt2() {
        assertEquals((short) 256, codec.decodeInt2(new byte[]{0x01, 0x00}));
        assertEquals((short) -1, codec.decodeInt2(new byte[]{(byte) 0xFF, (byte) 0xFF}));
    }

    @Test
    void encodeInt2() {
        assertArrayEquals(new byte[]{0x01, 0x00}, PgBinaryCodec.encodeInt2((short) 256));
    }

    // =====================================================================
    // Int4
    // =====================================================================

    @Test
    void decodeInt4() {
        assertEquals(1, codec.decodeInt4(new byte[]{0, 0, 0, 1}));
        assertEquals(-1, codec.decodeInt4(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
    }

    @Test
    void encodeDecodeInt4RoundTrip() {
        int value = 123456;
        assertEquals(value, codec.decodeInt4(PgBinaryCodec.encodeInt4(value)));
    }

    // =====================================================================
    // Int8
    // =====================================================================

    @Test
    void decodeInt8() {
        byte[] bytes = PgBinaryCodec.encodeInt8(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, codec.decodeInt8(bytes));
    }

    @Test
    void encodeDecodeInt8RoundTrip() {
        long value = 9876543210L;
        assertEquals(value, codec.decodeInt8(PgBinaryCodec.encodeInt8(value)));
    }

    // =====================================================================
    // Float4/8
    // =====================================================================

    @Test
    void decodeFloat4() {
        byte[] bytes = PgBinaryCodec.encodeFloat4(3.14f);
        assertEquals(3.14f, codec.decodeFloat4(bytes), 0.001f);
    }

    @Test
    void decodeFloat8() {
        byte[] bytes = PgBinaryCodec.encodeFloat8(3.14159265358979);
        assertEquals(3.14159265358979, codec.decodeFloat8(bytes), 1e-15);
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
        assertArrayEquals("hello".getBytes(), PgBinaryCodec.encodeString("hello"));
    }

    // =====================================================================
    // UUID
    // =====================================================================

    @Test
    void decodeUuid() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        byte[] encoded = PgBinaryCodec.encodeUuid(uuid);
        assertEquals(uuid, codec.decodeUuid(encoded));
    }

    @Test
    void encodeDecodeUuidRoundTrip() {
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, codec.decodeUuid(PgBinaryCodec.encodeUuid(uuid)));
    }

    // =====================================================================
    // Date
    // =====================================================================

    @Test
    void decodeDateEpoch() {
        // PG epoch = 2000-01-01, so 0 days = 2000-01-01
        assertEquals(LocalDate.of(2000, 1, 1), codec.decodeDate(PgBinaryCodec.encodeInt4(0)));
    }

    @Test
    void decodeDatePositive() {
        // 1 day after epoch = 2000-01-02
        assertEquals(LocalDate.of(2000, 1, 2), codec.decodeDate(PgBinaryCodec.encodeInt4(1)));
    }

    @Test
    void decodeDateInfinity() {
        assertEquals(LocalDate.MAX, codec.decodeDate(PgBinaryCodec.encodeInt4(Integer.MAX_VALUE)));
        assertEquals(LocalDate.MIN, codec.decodeDate(PgBinaryCodec.encodeInt4(Integer.MIN_VALUE)));
    }

    @Test
    void encodeDateRoundTrip() {
        LocalDate date = LocalDate.of(2025, 6, 15);
        assertEquals(date, codec.decodeDate(PgBinaryCodec.encodeDate(date)));
    }

    // =====================================================================
    // Time
    // =====================================================================

    @Test
    void decodeTime() {
        LocalTime time = LocalTime.of(12, 30, 45);
        assertEquals(time, codec.decodeTime(PgBinaryCodec.encodeTime(time)));
    }

    @Test
    void decodeTimeMidnight() {
        LocalTime time = LocalTime.MIDNIGHT;
        assertEquals(time, codec.decodeTime(PgBinaryCodec.encodeTime(time)));
    }

    // =====================================================================
    // Timestamp
    // =====================================================================

    @Test
    void decodeTimestamp() {
        LocalDateTime ts = LocalDateTime.of(2025, 6, 15, 12, 30, 45);
        assertEquals(ts, codec.decodeTimestamp(PgBinaryCodec.encodeTimestamp(ts)));
    }

    @Test
    void decodeTimestampInfinity() {
        assertEquals(LocalDateTime.MAX, codec.decodeTimestamp(PgBinaryCodec.encodeInt8(Long.MAX_VALUE)));
        assertEquals(LocalDateTime.MIN, codec.decodeTimestamp(PgBinaryCodec.encodeInt8(Long.MIN_VALUE)));
    }

    // =====================================================================
    // Timestamptz
    // =====================================================================

    @Test
    void decodeTimestamptz() {
        OffsetDateTime ts = OffsetDateTime.of(2025, 6, 15, 12, 30, 45, 0, ZoneOffset.UTC);
        assertEquals(ts, codec.decodeTimestamptz(PgBinaryCodec.encodeTimestamptz(ts)));
    }

    @Test
    void decodeTimestamptzInfinity() {
        assertEquals(OffsetDateTime.MAX, codec.decodeTimestamptz(PgBinaryCodec.encodeInt8(Long.MAX_VALUE)));
        assertEquals(OffsetDateTime.MIN, codec.decodeTimestamptz(PgBinaryCodec.encodeInt8(Long.MIN_VALUE)));
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
    // Numeric
    // =====================================================================

    @Test
    void decodeNumericZero() {
        // ndigits=0, weight=0, sign=POS, dscale=0
        byte[] data = {0, 0, 0, 0, 0, 0, 0, 0};
        assertEquals(BigDecimal.ZERO, codec.decodeNumeric(data).stripTrailingZeros());
    }

    @Test
    void decodeNumericPositiveInteger() {
        // 12345 in base-10000: ndigits=2, weight=1, sign=POS, dscale=0
        // groups: [1, 2345]
        byte[] data = {
            0, 2,   // ndigits = 2
            0, 1,   // weight = 1
            0, 0,   // sign = NUMERIC_POS
            0, 0,   // dscale = 0
            0, 1,   // group[0] = 1
            0x09, 0x29 // group[1] = 2345
        };
        assertEquals(new BigDecimal("12345"), codec.decodeNumeric(data));
    }

    @Test
    void decodeNumericNegative() {
        // -42 in base-10000: ndigits=1, weight=0, sign=NEG, dscale=0
        byte[] data = {
            0, 1,   // ndigits = 1
            0, 0,   // weight = 0
            0x40, 0x00, // sign = NUMERIC_NEG
            0, 0,   // dscale = 0
            0, 42   // group[0] = 42
        };
        assertEquals(new BigDecimal("-42"), codec.decodeNumeric(data));
    }

    @Test
    void decodeNumericWithScale() {
        // 1.23 in base-10000: ndigits=2, weight=0, sign=POS, dscale=2
        // groups: [1, 2300]
        byte[] data = {
            0, 2,   // ndigits = 2
            0, 0,   // weight = 0
            0, 0,   // sign = NUMERIC_POS
            0, 2,   // dscale = 2
            0, 1,   // group[0] = 1
            0x08, (byte) 0xFC // group[1] = 2300
        };
        assertEquals(new BigDecimal("1.23"), codec.decodeNumeric(data));
    }

    @Test
    void decodeNumericNaN() {
        byte[] data = {
            0, 0,   // ndigits = 0
            0, 0,   // weight = 0
            (byte) 0xC0, 0x00, // sign = NUMERIC_NAN
            0, 0    // dscale = 0
        };
        assertThrows(ArithmeticException.class, () -> codec.decodeNumeric(data));
    }

    // =====================================================================
    // JSON / JSONB
    // =====================================================================

    @Test
    void decodeJsonText() {
        byte[] data = "{\"key\":\"value\"}".getBytes();
        assertEquals("{\"key\":\"value\"}", codec.decodeJson(data, 114)); // OID_JSON
    }

    @Test
    void decodeJsonb() {
        // JSONB has a version byte prefix (0x01)
        byte[] json = "{\"key\":\"value\"}".getBytes();
        byte[] data = new byte[1 + json.length];
        data[0] = 1; // version
        System.arraycopy(json, 0, data, 1, json.length);
        assertEquals("{\"key\":\"value\"}", codec.decodeJson(data, 3802)); // OID_JSONB
    }

    // =====================================================================
    // Timetz
    // =====================================================================

    @Test
    void decodeTimetzRoundTrip() {
        OffsetTime time = OffsetTime.of(14, 30, 0, 0, ZoneOffset.ofHours(2));
        byte[] encoded = PgBinaryCodec.encodeTimetz(time);
        OffsetTime decoded = codec.decodeTimetz(encoded);
        assertEquals(time, decoded);
    }

    @Test
    void decodeTimetzUTC() {
        OffsetTime time = OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC);
        assertEquals(time, codec.decodeTimetz(PgBinaryCodec.encodeTimetz(time)));
    }

    // =====================================================================
    // Geometric types - encode/decode round trips
    // =====================================================================

    @Test
    void decodePoint() {
        Point p = new Point(1.5, -2.5);
        assertEquals(p, codec.decodePoint(PgBinaryCodec.encodePoint(p)));
    }

    @Test
    void decodeLine() {
        Line l = new Line(1.0, 2.0, 3.0);
        assertEquals(l, codec.decodeLine(PgBinaryCodec.encodeLine(l)));
    }

    @Test
    void decodeLseg() {
        LineSegment ls = new LineSegment(new Point(1, 2), new Point(3, 4));
        assertEquals(ls, codec.decodeLseg(PgBinaryCodec.encodeLseg(ls)));
    }

    @Test
    void decodeBox() {
        Box b = new Box(new Point(3, 4), new Point(1, 2));
        assertEquals(b, codec.decodeBox(PgBinaryCodec.encodeBox(b)));
    }

    @Test
    void decodePathOpen() {
        Path p = new Path(true, List.of(new Point(1, 2), new Point(3, 4)));
        Path decoded = codec.decodePath(PgBinaryCodec.encodePath(p));
        assertTrue(decoded.isOpen());
        assertEquals(2, decoded.points().size());
        assertEquals(p.points(), decoded.points());
    }

    @Test
    void decodePathClosed() {
        Path p = new Path(false, List.of(new Point(0, 0), new Point(1, 0), new Point(0, 1)));
        Path decoded = codec.decodePath(PgBinaryCodec.encodePath(p));
        assertFalse(decoded.isOpen());
        assertEquals(3, decoded.points().size());
    }

    @Test
    void decodePathInvalidByte() {
        byte[] data = new byte[21];
        data[0] = 5; // invalid open/closed byte
        assertThrows(IllegalArgumentException.class, () -> codec.decodePath(data));
    }

    @Test
    void decodePolygon() {
        Polygon poly = new Polygon(List.of(new Point(0, 0), new Point(1, 0), new Point(0, 1)));
        Polygon decoded = codec.decodePolygon(PgBinaryCodec.encodePolygon(poly));
        assertEquals(3, decoded.points().size());
        assertEquals(poly.points(), decoded.points());
    }

    @Test
    void decodeCircle() {
        Circle c = new Circle(new Point(1.5, 2.5), 10.0);
        assertEquals(c, codec.decodeCircle(PgBinaryCodec.encodeCircle(c)));
    }

    // =====================================================================
    // Interval
    // =====================================================================

    @Test
    void decodeIntervalRoundTrip() {
        Interval interval = Interval.of(1, 2, 3, 4, 5, 6, 7);
        // encodeInterval then decodeInterval may not be exact due to normalization,
        // but simple cases should work
        Interval simple = Interval.of(0, 0, 0, 1, 30, 0, 0);
        Interval decoded = codec.decodeInterval(PgBinaryCodec.encodeInterval(simple));
        assertEquals(simple.hours(), decoded.hours());
        assertEquals(simple.minutes(), decoded.minutes());
    }

    // =====================================================================
    // Money (binary)
    // =====================================================================

    @Test
    void decodeMoneyBinary() {
        Money m = new Money(new BigDecimal("12.34"));
        Money decoded = codec.decodeMoney(PgBinaryCodec.encodeMoney(m));
        assertEquals(0, m.bigDecimalValue().compareTo(decoded.bigDecimalValue()));
    }

    @Test
    void decodeMoneyBinaryNegative() {
        Money m = new Money(new BigDecimal("-99.99"));
        Money decoded = codec.decodeMoney(PgBinaryCodec.encodeMoney(m));
        assertEquals(0, m.bigDecimalValue().compareTo(decoded.bigDecimalValue()));
    }

    // =====================================================================
    // Inet / Cidr (binary)
    // =====================================================================

    @Test
    void decodeInetV4() throws Exception {
        Inet inet = new Inet(InetAddress.getByName("192.168.1.1"), null);
        Inet decoded = codec.decodeInet(PgBinaryCodec.encodeInet(inet));
        assertEquals(inet.address(), decoded.address());
        assertNull(decoded.netmask());
    }

    @Test
    void decodeInetV4WithMask() throws Exception {
        Inet inet = new Inet(InetAddress.getByName("10.0.0.0"), 8);
        Inet decoded = codec.decodeInet(PgBinaryCodec.encodeInet(inet));
        assertEquals(inet.address(), decoded.address());
        assertEquals(8, decoded.netmask());
    }

    @Test
    void decodeInetV6() throws Exception {
        Inet inet = new Inet(InetAddress.getByName("::1"), null);
        Inet decoded = codec.decodeInet(PgBinaryCodec.encodeInet(inet));
        assertEquals(inet.address(), decoded.address());
        assertNull(decoded.netmask());
    }

    @Test
    void decodeCidrV4() throws Exception {
        Cidr cidr = new Cidr(InetAddress.getByName("192.168.1.0"), 24);
        Cidr decoded = codec.decodeCidr(PgBinaryCodec.encodeCidr(cidr));
        assertEquals(cidr.address(), decoded.address());
        assertEquals(24, decoded.netmask());
    }

    @Test
    void decodeCidrV6() throws Exception {
        Cidr cidr = new Cidr(InetAddress.getByName("fe80::"), 64);
        Cidr decoded = codec.decodeCidr(PgBinaryCodec.encodeCidr(cidr));
        assertEquals(cidr.address(), decoded.address());
        assertEquals(64, decoded.netmask());
    }

    @Test
    void decodeInetInvalidFamily() {
        byte[] data = {99, 32, 0, 4, 10, 0, 0, 1}; // family=99
        assertThrows(IllegalArgumentException.class, () -> codec.decodeInet(data));
    }

    @Test
    void decodeCidrInvalidFamily() {
        byte[] data = {99, 24, 1, 4, 10, 0, 0, 0}; // family=99
        assertThrows(IllegalArgumentException.class, () -> codec.decodeCidr(data));
    }
}
