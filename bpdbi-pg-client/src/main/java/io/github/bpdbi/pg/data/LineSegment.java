package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres line segment: [(x1,y1),(x2,y2)]. */
public record LineSegment(@NonNull Point p1, @NonNull Point p2) {

  @Override
  public @NonNull String toString() {
    return "[" + p1 + "," + p2 + "]";
  }
}
