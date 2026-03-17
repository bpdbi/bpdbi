package io.github.bpdbi.pg.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  void cidrToStringWithoutMask() throws Exception {
    var c = new Cidr(InetAddress.getByName("192.168.1.0"), null);
    assertEquals("192.168.1.0", c.toString());
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
}
