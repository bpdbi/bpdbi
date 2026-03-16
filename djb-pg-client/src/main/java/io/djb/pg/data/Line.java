package io.djb.pg.data;

/**
 * A PostgreSQL line: {A,B,C} representing Ax + By + C = 0.
 */
public record Line(double a, double b, double c) {

    public static Line parse(String s) {
        s = s.trim();
        if (s.startsWith("{") && s.endsWith("}")) {
            s = s.substring(1, s.length() - 1);
        }
        String[] parts = s.split(",");
        return new Line(
            Double.parseDouble(parts[0].trim()),
            Double.parseDouble(parts[1].trim()),
            Double.parseDouble(parts[2].trim()));
    }

    @Override
    public String toString() {
        return "{" + a + "," + b + "," + c + "}";
    }
}
