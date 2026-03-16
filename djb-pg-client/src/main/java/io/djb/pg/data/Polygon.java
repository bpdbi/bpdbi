package io.djb.pg.data;

import java.util.ArrayList;
import java.util.List;

/**
 * A PostgreSQL polygon: ((x1,y1),(x2,y2),...).
 */
public record Polygon(List<Point> points) {

    public static Polygon parse(String s) {
        s = s.trim();
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1);
        }
        List<Point> points = new ArrayList<>();
        // Split on ),( to find individual points
        int i = 0;
        while (i < s.length()) {
            int start = s.indexOf('(', i);
            if (start < 0) break;
            int end = s.indexOf(')', start);
            if (end < 0) break;
            points.add(Point.parse(s.substring(start, end + 1)));
            i = end + 1;
        }
        return new Polygon(points);
    }
}
