package io.djb.pg.data;

import static io.djb.pg.data.ParserHelpers.parsePoints;

import java.util.List;
import org.jspecify.annotations.NonNull;

/** A Postgres path. Can be open [(x1,y1),...] or closed ((x1,y1),...). */
public record Path(boolean isOpen, @NonNull List<Point> points) {

  public static @NonNull Path parse(@NonNull String s) {
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
    return new Path(isOpen, parsePoints(s));
  }
}
