package io.github.bpdbi.pg.data;

import java.math.BigDecimal;
import org.jspecify.annotations.NonNull;

/** A Postgres money value. Stored as cents internally. */
public record Money(@NonNull BigDecimal bigDecimalValue) {

  @Override
  public @NonNull String toString() {
    return bigDecimalValue.toPlainString();
  }
}
