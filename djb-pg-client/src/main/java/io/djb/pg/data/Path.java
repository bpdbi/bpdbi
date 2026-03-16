package io.djb.pg.data;

import static io.djb.pg.data.ParserHelpers.parsePoints;

import java.util.ArrayList;
import java.util.List;

/**
 * A Postgres path. Can be open [(x1,y1),...] or closed ((x1,y1),...).
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
    return new Path(isOpen, parsePoints(s));
  }
}
