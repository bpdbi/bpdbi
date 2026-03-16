package io.djb.pg.data;

import java.util.Objects;

/**
 * A PostgreSQL point: (x, y).
 */
public record Point(double x, double y) {

    /** Parse from PG text format: "(x,y)" */
    public static Point parse(String s) {
        s = s.trim();
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1);
        }
        String[] parts = s.split(",");
        return new Point(Double.parseDouble(parts[0].trim()), Double.parseDouble(parts[1].trim()));
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
