package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** Postgres 'line': {A,B,C} representing Ax + By + C = 0. */
public record Line(double a, double b, double c) {

  @Override
  public @NonNull String toString() {
    return "{" + a + "," + b + "," + c + "}";
  }
}
