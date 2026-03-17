package io.djb.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres circle: &lt;(x,y),r&gt;. */
public record Circle(@NonNull Point centerPoint, double radius) {

  public static @NonNull Circle parse(@NonNull String s) {
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
  public @NonNull String toString() {
    return "<" + centerPoint + "," + radius + ">";
  }
}
