package io.djb.pg.data;

/**
 * A PostgreSQL line segment: [(x1,y1),(x2,y2)].
 */
public record LineSegment(Point p1, Point p2) {

    public static LineSegment parse(String s) {
        s = s.trim();
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length() - 1);
        }
        // Format: (x1,y1),(x2,y2)
        int mid = s.indexOf("),(");
        String s1 = s.substring(0, mid + 1);
        String s2 = s.substring(mid + 2);
        return new LineSegment(Point.parse(s1), Point.parse(s2));
    }

    @Override
    public String toString() {
        return "[" + p1 + "," + p2 + "]";
    }
}
