package io.djb.pg.data;

/**
 * A PostgreSQL circle: &lt;(x,y),r&gt;.
 */
public record Circle(Point center, double radius) {

    public static Circle parse(String s) {
        s = s.trim();
        if (s.startsWith("<") && s.endsWith(">")) {
            s = s.substring(1, s.length() - 1);
        }
        // Format: (x,y),r
        int lastComma = s.lastIndexOf(",");
        String pointStr = s.substring(0, lastComma);
        double r = Double.parseDouble(s.substring(lastComma + 1).trim());
        return new Circle(Point.parse(pointStr), r);
    }

    @Override
    public String toString() {
        return "<" + center + "," + radius + ">";
    }
}
