package io.djb.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres box: (upper-right),(lower-left). */
public record Box(@NonNull Point upperRightCorner, @NonNull Point lowerLeftCorner) {

  public static @NonNull Box parse(@NonNull String s) {
    s = s.trim();
    int mid = s.indexOf("),(");
    String s1 = s.substring(0, mid + 1);
    String s2 = s.substring(mid + 2);
    return new Box(Point.parse(s1), Point.parse(s2));
  }

  @Override
  public @NonNull String toString() {
    return upperRightCorner + "," + lowerLeftCorner;
  }
}
