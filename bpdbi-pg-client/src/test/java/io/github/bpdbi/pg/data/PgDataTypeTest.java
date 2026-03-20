package io.github.bpdbi.pg.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class PgDataTypeTest {

  // =====================================================================
  // Point
  // =====================================================================

  @Test
  void pointToString() {
    assertEquals("(1.5,2.5)", new Point(1.5, 2.5).toString());
  }

  // =====================================================================
  // Line
  // =====================================================================

  @Test
  void lineToString() {
    assertEquals("{1.0,2.0,3.0}", new Line(1.0, 2.0, 3.0).toString());
  }

  // =====================================================================
  // LineSegment
  // =====================================================================

  @Test
  void lineSegmentToString() {
    var ls = new LineSegment(new Point(1.0, 2.0), new Point(3.0, 4.0));
    assertEquals("[(1.0,2.0),(3.0,4.0)]", ls.toString());
  }

  // =====================================================================
  // Box
  // =====================================================================

  @Test
  void boxToString() {
    var b = new Box(new Point(3.0, 4.0), new Point(1.0, 2.0));
    assertEquals("(3.0,4.0),(1.0,2.0)", b.toString());
  }

  // =====================================================================
  // Circle
  // =====================================================================

  @Test
  void circleToString() {
    assertEquals("<(1.0,2.0),3.0>", new Circle(new Point(1.0, 2.0), 3.0).toString());
  }

  // =====================================================================
  // Money
  // =====================================================================

  @Test
  void moneyToString() {
    assertEquals("12.34", new Money(new BigDecimal("12.34")).toString());
  }

  // =====================================================================
  // Cidr
  // =====================================================================

  @Test
  void cidrToStringWithMask() throws Exception {
    var c = new Cidr(InetAddress.getByName("192.168.1.0"), 24);
    assertEquals("192.168.1.0/24", c.toString());
  }

  @Test
  void cidrAlwaysHasMask() throws Exception {
    // Postgres cidr type always has a netmask, unlike inet
    var c = new Cidr(InetAddress.getByName("10.0.0.0"), 8);
    assertEquals("10.0.0.0/8", c.toString());
  }

  // =====================================================================
  // Inet
  // =====================================================================

  @Test
  void inetToStringWithMask() throws Exception {
    var i = new Inet(InetAddress.getByName("10.0.0.1"), 16);
    assertEquals("10.0.0.1/16", i.toString());
  }

  @Test
  void inetToStringWithoutMask() throws Exception {
    var i = new Inet(InetAddress.getByName("10.0.0.1"), null);
    assertEquals("10.0.0.1", i.toString());
  }

  // =====================================================================
  // Interval
  // =====================================================================

  @Test
  void intervalToDuration() {
    var i = Interval.of(0, 0, 0, 1, 30, 0);
    var d = i.toDuration();
    assertEquals(5400, d.getSeconds());
  }

  @Test
  void intervalToDurationWithDays() {
    var i = Interval.of(0, 0, 2, 3, 0, 0);
    var d = i.toDuration();
    assertEquals(2 * 24 * 3600 + 3 * 3600, d.getSeconds());
  }

  @Test
  void intervalToDurationWithMicroseconds() {
    var i = Interval.of(0, 0, 0, 0, 0, 0, 500_000);
    var d = i.toDuration();
    assertEquals(500_000_000, d.getNano());
  }

  @Test
  void intervalFactory() {
    var i = Interval.of(1, 2, 3, 4, 5, 6, 7);
    assertEquals(1, i.years());
    assertEquals(2, i.months());
    assertEquals(3, i.days());
    assertEquals(4, i.hours());
    assertEquals(5, i.minutes());
    assertEquals(6, i.seconds());
    assertEquals(7, i.microseconds());
  }

  // =====================================================================
  // BitString
  // =====================================================================

  @Test
  void bitStringGetBits() {
    // 10110001 = 0xB1
    var bs = new BitString(8, new byte[] {(byte) 0xB1});
    assertTrue(bs.get(0)); // 1
    assertFalse(bs.get(1)); // 0
    assertTrue(bs.get(2)); // 1
    assertTrue(bs.get(3)); // 1
    assertFalse(bs.get(4)); // 0
    assertFalse(bs.get(5)); // 0
    assertFalse(bs.get(6)); // 0
    assertTrue(bs.get(7)); // 1
  }

  @Test
  void bitStringGetOutOfBoundsThrows() {
    var bs = new BitString(4, new byte[] {(byte) 0xF0});
    assertThrows(IndexOutOfBoundsException.class, () -> bs.get(4));
    assertThrows(IndexOutOfBoundsException.class, () -> bs.get(-1));
  }

  @Test
  void bitStringConstructorValidation() {
    assertThrows(IllegalArgumentException.class, () -> new BitString(8, new byte[] {1, 2}));
  }

  @Test
  void bitStringToString() {
    var bs = new BitString(4, new byte[] {(byte) 0xA0}); // 1010
    assertEquals("1010", bs.toString());
  }

  @Test
  void bitStringEquality() {
    var a = new BitString(8, new byte[] {(byte) 0xFF});
    var b = new BitString(8, new byte[] {(byte) 0xFF});
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  // =====================================================================
  // Macaddr
  // =====================================================================

  @Test
  void macaddrToString() {
    var m = new Macaddr(new byte[] {0x08, 0x00, 0x2b, 0x01, 0x02, 0x03});
    assertEquals("08:00:2b:01:02:03", m.toString());
  }

  @Test
  void macaddrConstructorValidation() {
    assertThrows(IllegalArgumentException.class, () -> new Macaddr(new byte[] {1, 2, 3}));
  }

  // =====================================================================
  // Macaddr8
  // =====================================================================

  @Test
  void macaddr8ToString() {
    var m = new Macaddr8(new byte[] {0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05});
    assertEquals("08:00:2b:01:02:03:04:05", m.toString());
  }

  @Test
  void macaddr8ConstructorValidation() {
    assertThrows(IllegalArgumentException.class, () -> new Macaddr8(new byte[] {1, 2, 3}));
  }
}
