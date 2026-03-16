package io.djb.pg.data;

/**
 * A PostgreSQL box: (upper-right),(lower-left).
 */
public record Box(Point upperRightCorner, Point lowerLeftCorner) {

    public static Box parse(String s) {
        s = s.trim();
        int mid = s.indexOf("),(");
        String s1 = s.substring(0, mid + 1);
        String s2 = s.substring(mid + 2);
        return new Box(Point.parse(s1), Point.parse(s2));
    }

    @Override
    public String toString() {
        return upperRightCorner + "," + lowerLeftCorner;
    }
}
