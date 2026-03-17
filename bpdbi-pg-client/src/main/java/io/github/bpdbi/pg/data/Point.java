package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres point: (x, y). */
public record Point(double x, double y) {

  @Override
  public @NonNull String toString() {
    return "(" + x + "," + y + ")";
  }
}
