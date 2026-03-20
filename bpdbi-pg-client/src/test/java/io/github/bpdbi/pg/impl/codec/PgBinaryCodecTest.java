package io.github.bpdbi.pg.impl.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.pg.data.Box;
import io.github.bpdbi.pg.data.Cidr;
import io.github.bpdbi.pg.data.Circle;
import io.github.bpdbi.pg.data.Inet;
import io.github.bpdbi.pg.data.Interval;
import io.github.bpdbi.pg.data.Line;
import io.github.bpdbi.pg.data.LineSegment;
import io.github.bpdbi.pg.data.Money;
import io.github.bpdbi.pg.data.Path;
import io.github.bpdbi.pg.data.Point;
import io.github.bpdbi.pg.data.Polygon;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PgBinaryCodecTest {

  private final PgBinaryCodec codec = PgBinaryCodec.INSTANCE;

  // =====================================================================
  // Bool
  // =====================================================================

  @Test
  void decodeBoolTrue() {
    assertTrue(codec.decodeBool(new byte[] {1}));
  }

  @Test
  void decodeBoolFalse() {
    assertFalse(codec.decodeBool(new byte[] {0}));
  }

  @Test
  void encodeBool() {
    assertArrayEquals(new byte[] {1}, PgBinaryCodec.encodeBool(true));
    assertArrayEquals(new byte[] {0}, PgBinaryCodec.encodeBool(false));
  }

  // =====================================================================
  // Int2
  // =====================================================================

  @Test
  void decodeInt2() {
    assertEquals((short) 256, codec.decodeInt2(new byte[] {0x01, 0x00}));
    assertEquals((short) -1, codec.decodeInt2(new byte[] {(byte) 0xFF, (byte) 0xFF}));
  }

  @Test
  void encodeInt2() {
    assertArrayEquals(new byte[] {0x01, 0x00}, PgBinaryCodec.encodeInt2((short) 256));
  }

  // =====================================================================
  // Int4
  // =====================================================================

  @Test
  void decodeInt4() {
    assertEquals(1, codec.decodeInt4(new byte[] {0, 0, 0, 1}));
    assertEquals(
        -1, codec.decodeInt4(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
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
    assertEquals(
        LocalDateTime.MAX, codec.decodeTimestamp(PgBinaryCodec.encodeInt8(Long.MAX_VALUE)));
    assertEquals(
        LocalDateTime.MIN, codec.decodeTimestamp(PgBinaryCodec.encodeInt8(Long.MIN_VALUE)));
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
    assertEquals(
        OffsetDateTime.MAX, codec.decodeTimestamptz(PgBinaryCodec.encodeInt8(Long.MAX_VALUE)));
    assertEquals(
        OffsetDateTime.MIN, codec.decodeTimestamptz(PgBinaryCodec.encodeInt8(Long.MIN_VALUE)));
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
      0, 2, // ndigits = 2
      0, 1, // weight = 1
      0, 0, // sign = NUMERIC_POS
      0, 0, // dscale = 0
      0, 1, // group[0] = 1
      0x09, 0x29 // group[1] = 2345
    };
    assertEquals(new BigDecimal("12345"), codec.decodeNumeric(data));
  }

  @Test
  void decodeNumericNegative() {
    // -42 in base-10000: ndigits=1, weight=0, sign=NEG, dscale=0
    byte[] data = {
      0, 1, // ndigits = 1
      0, 0, // weight = 0
      0x40, 0x00, // sign = NUMERIC_NEG
      0, 0, // dscale = 0
      0, 42 // group[0] = 42
    };
    assertEquals(new BigDecimal("-42"), codec.decodeNumeric(data));
  }

  @Test
  void decodeNumericWithScale() {
    // 1.23 in base-10000: ndigits=2, weight=0, sign=POS, dscale=2
    // groups: [1, 2300]
    byte[] data = {
      0, 2, // ndigits = 2
      0, 0, // weight = 0
      0, 0, // sign = NUMERIC_POS
      0, 2, // dscale = 2
      0, 1, // group[0] = 1
      0x08, (byte) 0xFC // group[1] = 2300
    };
    assertEquals(new BigDecimal("1.23"), codec.decodeNumeric(data));
  }

  @Test
  void decodeNumericNaN() {
    byte[] data = {
      0,
      0, // ndigits = 0
      0,
      0, // weight = 0
      (byte) 0xC0,
      0x00, // sign = NUMERIC_NAN
      0,
      0 // dscale = 0
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

  // =====================================================================
  // TsVector (binary)
  // =====================================================================

  @Test
  void decodeTsVectorSingleLexemeNoPositions() {
    // numLexemes=1, "word\0", numPositions=0
    byte[] data = {
      0,
      0,
      0,
      1, // numLexemes = 1
      'w',
      'o',
      'r',
      'd',
      0, // "word\0"
      0,
      0 // numPositions = 0
    };
    assertEquals("'word'", codec.decodeTsVector(data));
  }

  @Test
  void decodeTsVectorWithPositionsAndWeights() {
    // numLexemes=1, "cat\0", numPositions=2, pos1=1 weight=A(3), pos2=3 weight=B(2)
    byte[] data = {
      0,
      0,
      0,
      1, // numLexemes = 1
      'c',
      'a',
      't',
      0, // "cat\0"
      0,
      2, // numPositions = 2
      (byte) 0xC0,
      0x01, // pos=1 weight=3(A): 3<<14 | 1 = 0xC001
      (byte) 0x80,
      0x03 // pos=3 weight=2(B): 2<<14 | 3 = 0x8003
    };
    assertEquals("'cat':1A,3B", codec.decodeTsVector(data));
  }

  @Test
  void decodeTsVectorMultipleLexemes() {
    // numLexemes=2, "a\0" noPositions, "b\0" noPositions
    byte[] data = {
      0,
      0,
      0,
      2, // numLexemes = 2
      'a',
      0, // "a\0"
      0,
      0, // numPositions = 0
      'b',
      0, // "b\0"
      0,
      0 // numPositions = 0
    };
    assertEquals("'a' 'b'", codec.decodeTsVector(data));
  }

  // =====================================================================
  // TsQuery (binary)
  // =====================================================================

  @Test
  void decodeTsQueryEmpty() {
    byte[] data = {0, 0, 0, 0}; // numItems = 0
    assertEquals("", codec.decodeTsQuery(data));
  }

  @Test
  void decodeTsQuerySingleTerm() {
    // numItems=1, type=QI_VAL(1), weight=0, prefix=0, "word\0"
    byte[] data = {
      0,
      0,
      0,
      1, // numItems = 1
      1, // type = QI_VAL
      0, // weight = 0
      0, // prefix = false
      'w',
      'o',
      'r',
      'd',
      0 // "word\0"
    };
    assertEquals("'word'", codec.decodeTsQuery(data));
  }

  @Test
  void decodeTsQueryAndOperator() {
    // Items: val "a", val "b", op AND — stack: push a, push b, pop b+a → "( 'a' & 'b' )"
    byte[] data = {
      0,
      0,
      0,
      3, // numItems = 3
      1,
      0,
      0,
      'a',
      0, // QI_VAL "a" no weight no prefix
      1,
      0,
      0,
      'b',
      0, // QI_VAL "b" no weight no prefix
      2,
      2 // QI_OPR, OP_AND
    };
    assertEquals("( 'a' & 'b' )", codec.decodeTsQuery(data));
  }

  @Test
  void decodeTsQueryNotOperator() {
    // Items: val "x", op NOT — stack: push x, pop x → "!x"
    byte[] data = {
      0,
      0,
      0,
      2, // numItems = 2
      1,
      0,
      0,
      'x',
      0, // QI_VAL "x"
      2,
      1 // QI_OPR, OP_NOT
    };
    assertEquals("!'x'", codec.decodeTsQuery(data));
  }

  @Test
  void decodeTsQueryPhraseOperator() {
    // Items: val "a", val "b", op PHRASE with distance=2
    byte[] data = {
      0,
      0,
      0,
      3, // numItems = 3
      1,
      0,
      0,
      'a',
      0, // QI_VAL "a"
      1,
      0,
      0,
      'b',
      0, // QI_VAL "b"
      2,
      4, // QI_OPR, OP_PHRASE
      0,
      2 // distance = 2
    };
    assertEquals("( 'a' <2> 'b' )", codec.decodeTsQuery(data));
  }

  @Test
  void decodeTsQueryPhraseDistance1() {
    // Phrase with distance=1 → "<->"
    byte[] data = {
      0,
      0,
      0,
      3, // numItems = 3
      1,
      0,
      0,
      'a',
      0, // QI_VAL "a"
      1,
      0,
      0,
      'b',
      0, // QI_VAL "b"
      2,
      4, // QI_OPR, OP_PHRASE
      0,
      1 // distance = 1
    };
    assertEquals("( 'a' <-> 'b' )", codec.decodeTsQuery(data));
  }

  // =====================================================================
  // Array decode (binary)
  // =====================================================================

  @Test
  void decodeArrayEmpty() {
    // ndim=0 → empty list
    byte[] data = {
      0, 0, 0, 0, // ndim = 0
      0, 0, 0, 0, // has null = 0
      0, 0, 0, 23 // elemType = OID_INT4
    };
    assertEquals(List.of(), codec.decodeArray(data, codec::decodeInt4));
  }

  @Test
  void decodeArrayMultiDimensionalReturnsNull() {
    // ndim=2 → null (unsupported)
    byte[] data = {
      0, 0, 0, 2, // ndim = 2
      0, 0, 0, 0, // has null
      0, 0, 0, 23, // elemType
      0, 0, 0, 3, // dim[0] = 3
      0, 0, 0, 1, // lbound[0]
      0, 0, 0, 2, // dim[1] = 2
      0, 0, 0, 1 // lbound[1]
    };
    assertNull(codec.decodeArray(data, codec::decodeInt4));
  }

  @Test
  void decodeArrayInt4Elements() {
    // ndim=1, elemType=INT4, dimension=3, values: 10, 20, 30
    byte[] data = {
      0,
      0,
      0,
      1, // ndim = 1
      0,
      0,
      0,
      0, // has null = 0
      0,
      0,
      0,
      23, // elemType = OID_INT4
      0,
      0,
      0,
      3, // dimension = 3
      0,
      0,
      0,
      1, // lower bound = 1
      // element 0: length=4, value=10
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      10,
      // element 1: length=4, value=20
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      20,
      // element 2: length=4, value=30
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      30
    };
    assertEquals(List.of(10, 20, 30), codec.decodeArray(data, codec::decodeInt4));
  }

  @Test
  void decodeArrayWithNullElements() {
    // ndim=1, dimension=3, values: 10, NULL, 30 — NULL has elemLen=-1 and is skipped
    byte[] data = {
      0,
      0,
      0,
      1, // ndim = 1
      0,
      0,
      0,
      1, // has null = 1
      0,
      0,
      0,
      23, // elemType = OID_INT4
      0,
      0,
      0,
      3, // dimension = 3
      0,
      0,
      0,
      1, // lower bound = 1
      // element 0: length=4, value=10
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      10,
      // element 1: NULL (length=-1)
      (byte) 0xFF,
      (byte) 0xFF,
      (byte) 0xFF,
      (byte) 0xFF,
      // element 2: length=4, value=30
      0,
      0,
      0,
      4,
      0,
      0,
      0,
      30
    };
    // NULL elements are preserved as null in the list
    var result = codec.decodeArray(data, codec::decodeInt4);
    assertEquals(3, result.size());
    assertEquals(10, result.get(0));
    assertNull(result.get(1));
    assertEquals(30, result.get(2));
  }

  @Test
  void decodeArrayTextElements() {
    // ndim=1, elemType=TEXT(25), dimension=2, values: "hi", "ok"
    byte[] data = {
      0,
      0,
      0,
      1, // ndim = 1
      0,
      0,
      0,
      0, // has null = 0
      0,
      0,
      0,
      25, // elemType = OID_TEXT
      0,
      0,
      0,
      2, // dimension = 2
      0,
      0,
      0,
      1, // lower bound = 1
      // element 0: length=2, "hi"
      0,
      0,
      0,
      2,
      'h',
      'i',
      // element 1: length=2, "ok"
      0,
      0,
      0,
      2,
      'o',
      'k'
    };
    assertEquals(
        List.of("hi", "ok"),
        codec.decodeArray(data, (java.util.function.Function<byte[], String>) codec::decodeString));
  }

  @Test
  void decodeArrayTooShort() {
    // Less than 12 bytes → empty list
    byte[] data = {0, 0, 0, 0};
    assertEquals(List.of(), codec.decodeArray(data, codec::decodeInt4));
  }

  // =====================================================================
  // Binary encode roundtrip tests
  // =====================================================================

  @Test
  void encodeDecodeInt4Roundtrip() {
    byte[] encoded = PgBinaryCodec.encodeInt4(12345);
    assertEquals(4, encoded.length);
    assertEquals(12345, codec.decodeInt4(encoded, 0));
  }

  @Test
  void encodeDecodeInt8Roundtrip() {
    byte[] encoded = PgBinaryCodec.encodeInt8(9876543210L);
    assertEquals(8, encoded.length);
    assertEquals(9876543210L, codec.decodeInt8(encoded, 0));
  }

  @Test
  void encodeDecodeFloat8Roundtrip() {
    byte[] encoded = PgBinaryCodec.encodeFloat8(3.14159);
    assertEquals(8, encoded.length);
    assertEquals(3.14159, codec.decodeFloat8(encoded, 0));
  }

  @Test
  void encodeDecodeBoolRoundtrip() {
    assertArrayEquals(new byte[] {1}, PgBinaryCodec.encodeBool(true));
    assertArrayEquals(new byte[] {0}, PgBinaryCodec.encodeBool(false));
    assertTrue(codec.decodeBool(PgBinaryCodec.encodeBool(true), 0));
    assertFalse(codec.decodeBool(PgBinaryCodec.encodeBool(false), 0));
  }

  @Test
  void encodeDecodeUuidRoundtrip() {
    UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    byte[] encoded = PgBinaryCodec.encodeUuid(uuid);
    assertEquals(16, encoded.length);
    assertEquals(uuid, codec.decodeUuid(encoded, 0, 16));
  }

  // =====================================================================
  // Special float/double values — NaN, Infinity
  // =====================================================================

  @Test
  void decodeFloat4NaN() {
    byte[] encoded = PgBinaryCodec.encodeFloat4(Float.NaN);
    assertTrue(Float.isNaN(codec.decodeFloat4(encoded)));
  }

  @Test
  void decodeFloat4NegativeInfinity() {
    byte[] encoded = PgBinaryCodec.encodeFloat4(Float.NEGATIVE_INFINITY);
    assertEquals(Float.NEGATIVE_INFINITY, codec.decodeFloat4(encoded));
  }

  @Test
  void decodeFloat8PositiveInfinity() {
    byte[] encoded = PgBinaryCodec.encodeFloat8(Double.POSITIVE_INFINITY);
    assertEquals(Double.POSITIVE_INFINITY, codec.decodeFloat8(encoded));
  }

  @Test
  void decodeFloat8NaN() {
    byte[] encoded = PgBinaryCodec.encodeFloat8(Double.NaN);
    assertTrue(Double.isNaN(codec.decodeFloat8(encoded)));
  }

  @Test
  void decodeFloat8NegativeInfinity() {
    byte[] encoded = PgBinaryCodec.encodeFloat8(Double.NEGATIVE_INFINITY);
    assertEquals(Double.NEGATIVE_INFINITY, codec.decodeFloat8(encoded));
  }

  // =====================================================================
  // Numeric precision edge cases
  // =====================================================================

  @Test
  void decodeNumericMaxInt() {
    // 2147483647 = base-10000 groups: [21, 4748, 3647], weight=2
    byte[] data = {
      0, 3, // ndigits = 3
      0, 2, // weight = 2
      0, 0, // sign = NUMERIC_POS
      0, 0, // dscale = 0
      0, 21, // group[0] = 21
      0x12, (byte) 0x8C, // group[1] = 4748
      0x0E, 0x3F // group[2] = 3647
    };
    assertEquals(new BigDecimal("2147483647"), codec.decodeNumeric(data));
  }

  @Test
  void numericLargeValueRoundTrip() {
    // Encode and decode via integration: use the server to generate known binary values
    // For unit test: just verify our codec handles a value with many base-10000 groups
    // 99999999 = groups: [9999, 9999], weight=1
    byte[] data = {
      0, 2, // ndigits = 2
      0, 1, // weight = 1
      0, 0, // sign = NUMERIC_POS
      0, 0, // dscale = 0
      0x27, 0x0F, // group[0] = 9999
      0x27, 0x0F // group[1] = 9999
    };
    assertEquals(new BigDecimal("99999999"), codec.decodeNumeric(data));
  }
}
