package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** Postgres 'circle': &lt;(x,y),r&gt;. */
public record Circle(@NonNull Point centerPoint, double radius) {

  @Override
  public @NonNull String toString() {
    return "<" + centerPoint + "," + radius + ">";
  }
}
