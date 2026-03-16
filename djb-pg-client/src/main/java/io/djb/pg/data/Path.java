package io.djb.pg.data;

import java.util.ArrayList;
import java.util.List;

/**
 * A PostgreSQL path. Can be open [(x1,y1),...] or closed ((x1,y1),...).
 */
public record Path(List<Point> points, boolean closed) {

    public static Path parse(String s) {
        s = s.trim();
        boolean closed;
        if (s.startsWith("(")) {
            closed = true;
            s = s.substring(1, s.length() - 1);
        } else if (s.startsWith("[")) {
            closed = false;
            s = s.substring(1, s.length() - 1);
        } else {
            closed = false;
        }
        List<Point> points = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            int start = s.indexOf('(', i);
            if (start < 0) break;
            int end = s.indexOf(')', start);
            if (end < 0) break;
            points.add(Point.parse(s.substring(start, end + 1)));
            i = end + 1;
        }
        return new Path(points, closed);
    }
}
