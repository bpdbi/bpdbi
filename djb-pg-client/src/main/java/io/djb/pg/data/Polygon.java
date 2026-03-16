package io.djb.pg.data;

import static io.djb.pg.data.ParserHelpers.parsePoints;

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * A Postgres polygon: ((x1,y1),(x2,y2),...).
 */
public record Polygon(List<Point> points) {

  public static Polygon parse(String s) {
    s = s.trim();
    if (s.startsWith("(") && s.endsWith(")")) {
      s = s.substring(1, s.length() - 1);
    }
    return new Polygon(parsePoints(s));
  }
}
