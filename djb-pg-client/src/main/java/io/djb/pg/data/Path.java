package io.djb.pg.data;

import java.util.ArrayList;
import java.util.List;

/**
 * A PostgreSQL path. Can be open [(x1,y1),...] or closed ((x1,y1),...).
 */
public record Path(boolean isOpen, List<Point> points) {

    public static Path parse(String s) {
        s = s.trim();
        boolean isOpen;
        if (s.startsWith("(")) {
            isOpen = false;
            s = s.substring(1, s.length() - 1);
        } else if (s.startsWith("[")) {
            isOpen = true;
            s = s.substring(1, s.length() - 1);
        } else {
            isOpen = true;
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
        return new Path(isOpen, points);
    }
}
