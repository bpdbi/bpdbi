package io.github.bpdbi.pg.data;

import org.jspecify.annotations.NonNull;

/** A Postgres box: (upper-right),(lower-left). */
public record Box(@NonNull Point upperRightCorner, @NonNull Point lowerLeftCorner) {

  @Override
  public @NonNull String toString() {
    return upperRightCorner + "," + lowerLeftCorner;
  }
}
