package io.djb.pg.data;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PgDataTypeTest {

    // =====================================================================
    // Point
    // =====================================================================

    @Test
    void parsePoint() {
        var p = Point.parse("(1.5,2.5)");
        assertEquals(1.5, p.x());
        assertEquals(2.5, p.y());
    }

    @Test
    void parsePointWithSpaces() {
        var p = Point.parse("  ( 1.0 , 2.0 )  ");
        assertEquals(1.0, p.x());
        assertEquals(2.0, p.y());
    }

    @Test
    void parsePointWithoutParens() {
        var p = Point.parse("3.0,4.0");
        assertEquals(3.0, p.x());
        assertEquals(4.0, p.y());
    }

    @Test
    void parsePointNegativeCoordinates() {
        var p = Point.parse("(-1.5,-2.5)");
        assertEquals(-1.5, p.x());
        assertEquals(-2.5, p.y());
    }

    @Test
    void pointToString() {
        assertEquals("(1.5,2.5)", new Point(1.5, 2.5).toString());
    }

    // =====================================================================
    // Line
    // =====================================================================

    @Test
    void parseLine() {
        var l = Line.parse("{1.0,2.0,3.0}");
        assertEquals(1.0, l.a());
        assertEquals(2.0, l.b());
        assertEquals(3.0, l.c());
    }

    @Test
    void parseLineWithSpaces() {
        var l = Line.parse("  { 1.0 , -2.0 , 3.5 }  ");
        assertEquals(1.0, l.a());
        assertEquals(-2.0, l.b());
        assertEquals(3.5, l.c());
    }

    @Test
    void parseLineWithoutBraces() {
        var l = Line.parse("1.0,2.0,3.0");
        assertEquals(1.0, l.a());
        assertEquals(2.0, l.b());
        assertEquals(3.0, l.c());
    }

    @Test
    void lineToString() {
        assertEquals("{1.0,2.0,3.0}", new Line(1.0, 2.0, 3.0).toString());
    }

    // =====================================================================
    // LineSegment
    // =====================================================================

    @Test
    void parseLineSegment() {
        var ls = LineSegment.parse("[(1.0,2.0),(3.0,4.0)]");
        assertEquals(1.0, ls.p1().x());
        assertEquals(2.0, ls.p1().y());
        assertEquals(3.0, ls.p2().x());
        assertEquals(4.0, ls.p2().y());
    }

    @Test
    void parseLineSegmentWithoutBrackets() {
        var ls = LineSegment.parse("(1.0,2.0),(3.0,4.0)");
        assertEquals(1.0, ls.p1().x());
        assertEquals(2.0, ls.p1().y());
        assertEquals(3.0, ls.p2().x());
        assertEquals(4.0, ls.p2().y());
    }

    @Test
    void lineSegmentToString() {
        var ls = new LineSegment(new Point(1.0, 2.0), new Point(3.0, 4.0));
        assertEquals("[(1.0,2.0),(3.0,4.0)]", ls.toString());
    }

    // =====================================================================
    // Box
    // =====================================================================

    @Test
    void parseBox() {
        var b = Box.parse("(3.0,4.0),(1.0,2.0)");
        assertEquals(3.0, b.upperRightCorner().x());
        assertEquals(4.0, b.upperRightCorner().y());
        assertEquals(1.0, b.lowerLeftCorner().x());
        assertEquals(2.0, b.lowerLeftCorner().y());
    }

    @Test
    void boxToString() {
        var b = new Box(new Point(3.0, 4.0), new Point(1.0, 2.0));
        assertEquals("(3.0,4.0),(1.0,2.0)", b.toString());
    }

    // =====================================================================
    // Circle
    // =====================================================================

    @Test
    void parseCircle() {
        var c = Circle.parse("<(1.0,2.0),3.0>");
        assertEquals(1.0, c.centerPoint().x());
        assertEquals(2.0, c.centerPoint().y());
        assertEquals(3.0, c.radius());
    }

    @Test
    void parseCircleWithoutAngleBrackets() {
        var c = Circle.parse("(1.0,2.0),3.0");
        assertEquals(1.0, c.centerPoint().x());
        assertEquals(2.0, c.centerPoint().y());
        assertEquals(3.0, c.radius());
    }

    @Test
    void circleToString() {
        assertEquals("<(1.0,2.0),3.0>", new Circle(new Point(1.0, 2.0), 3.0).toString());
    }

    // =====================================================================
    // Path
    // =====================================================================

    @Test
    void parseClosedPath() {
        var p = Path.parse("((1.0,2.0),(3.0,4.0),(5.0,6.0))");
        assertFalse(p.isOpen());
        assertEquals(3, p.points().size());
        assertEquals(1.0, p.points().get(0).x());
        assertEquals(6.0, p.points().get(2).y());
    }

    @Test
    void parseOpenPath() {
        var p = Path.parse("[(1.0,2.0),(3.0,4.0)]");
        assertTrue(p.isOpen());
        assertEquals(2, p.points().size());
    }

    @Test
    void parseSinglePointPath() {
        var p = Path.parse("[(1.0,2.0)]");
        assertTrue(p.isOpen());
        assertEquals(1, p.points().size());
        assertEquals(1.0, p.points().get(0).x());
        assertEquals(2.0, p.points().get(0).y());
    }

    // =====================================================================
    // Polygon
    // =====================================================================

    @Test
    void parsePolygon() {
        var p = Polygon.parse("((1.0,2.0),(3.0,4.0),(5.0,6.0))");
        assertEquals(3, p.points().size());
        assertEquals(1.0, p.points().get(0).x());
        assertEquals(2.0, p.points().get(0).y());
        assertEquals(5.0, p.points().get(2).x());
        assertEquals(6.0, p.points().get(2).y());
    }

    @Test
    void parsePolygonSinglePoint() {
        var p = Polygon.parse("((0.0,0.0))");
        assertEquals(1, p.points().size());
    }

    // =====================================================================
    // Money
    // =====================================================================

    @Test
    void parseMoney() {
        assertEquals(new BigDecimal("12.34"), Money.parse("$12.34").bigDecimalValue());
    }

    @Test
    void parseMoneyWithGroupingSeparators() {
        assertEquals(new BigDecimal("1234.56"), Money.parse("$1,234.56").bigDecimalValue());
    }

    @Test
    void parseMoneyNegative() {
        assertEquals(new BigDecimal("-99.99"), Money.parse("-$99.99").bigDecimalValue());
    }

    @Test
    void parseMoneyPlain() {
        assertEquals(new BigDecimal("100.00"), Money.parse("100.00").bigDecimalValue());
    }

    @Test
    void moneyToString() {
        assertEquals("12.34", new Money(new BigDecimal("12.34")).toString());
    }

    // =====================================================================
    // Cidr
    // =====================================================================

    @Test
    void parseCidrV4() throws Exception {
        var c = Cidr.parse("192.168.1.0/24");
        assertEquals(InetAddress.getByName("192.168.1.0"), c.address());
        assertEquals(24, c.netmask());
    }

    @Test
    void parseCidrV6() throws Exception {
        var c = Cidr.parse("::1/128");
        assertEquals(InetAddress.getByName("::1"), c.address());
        assertEquals(128, c.netmask());
    }

    @Test
    void parseCidrWithoutMask() throws Exception {
        var c = Cidr.parse("10.0.0.1");
        assertEquals(InetAddress.getByName("10.0.0.1"), c.address());
        assertNull(c.netmask());
    }

    @Test
    void parseCidrInvalid() {
        assertThrows(IllegalArgumentException.class, () -> Cidr.parse("not-an-address/32"));
    }

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
    void parseInetV4() throws Exception {
        var i = Inet.parse("192.168.1.1/24");
        assertEquals(InetAddress.getByName("192.168.1.1"), i.address());
        assertEquals(24, i.netmask());
    }

    @Test
    void parseInetV4NoMask() throws Exception {
        var i = Inet.parse("192.168.1.1");
        assertEquals(InetAddress.getByName("192.168.1.1"), i.address());
        assertNull(i.netmask());
    }

    @Test
    void parseInetV6() throws Exception {
        var i = Inet.parse("::1/128");
        assertEquals(InetAddress.getByName("::1"), i.address());
        assertEquals(128, i.netmask());
    }

    @Test
    void parseInetInvalid() {
        assertThrows(IllegalArgumentException.class, () -> Inet.parse("not-an-address"));
    }

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
    void parseIntervalFull() {
        var i = Interval.parse("1 year 2 mons 3 days 04:05:06.000007");
        assertEquals(1, i.years());
        assertEquals(2, i.months());
        assertEquals(3, i.days());
        assertEquals(4, i.hours());
        assertEquals(5, i.minutes());
        assertEquals(6, i.seconds());
        assertEquals(7, i.microseconds());
    }

    @Test
    void parseIntervalDaysOnly() {
        var i = Interval.parse("30 days");
        assertEquals(0, i.years());
        assertEquals(0, i.months());
        assertEquals(30, i.days());
        assertEquals(0, i.hours());
    }

    @Test
    void parseIntervalTimeOnly() {
        var i = Interval.parse("01:30:00");
        assertEquals(0, i.days());
        assertEquals(1, i.hours());
        assertEquals(30, i.minutes());
        assertEquals(0, i.seconds());
    }

    @Test
    void parseIntervalNegativeTime() {
        var i = Interval.parse("-04:05:06");
        assertEquals(-4, i.hours());
        assertEquals(-5, i.minutes());
        assertEquals(-6, i.seconds());
    }

    @Test
    void parseIntervalNegativeDays() {
        var i = Interval.parse("-3 days");
        assertEquals(-3, i.days());
    }

    @Test
    void intervalToDuration() {
        var i = Interval.of(0, 0, 0, 1, 30, 0);
        var d = i.toDuration();
        assertEquals(5400, d.getSeconds());
    }
}
