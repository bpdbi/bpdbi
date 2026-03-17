package io.djb.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres point: (x, y). */
public record Point(double x, double y) {

  /** Parse from PG text format: "(x,y)" */
  public static @NonNull Point parse(@NonNull String s) {
    s = s.trim();
    if (s.startsWith("(") && s.endsWith(")")) {
      s = s.substring(1, s.length() - 1);
    }
    String[] parts = s.split(",");
    return new Point(Double.parseDouble(parts[0].trim()), Double.parseDouble(parts[1].trim()));
  }

  @Override
  public @NonNull String toString() {
    return "(" + x + "," + y + ")";
  }
}
