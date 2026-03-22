package io.github.bpdbi.pg.data;

import java.math.BigDecimal;
import org.jspecify.annotations.NonNull;

/**
 * Postgres 'money' value. Stored as cents internally.
 *
 * <p>Usually not a good idea to use this type for money values.
 */
public record Money(@NonNull BigDecimal bigDecimalValue) {

  @Override
  public @NonNull String toString() {
    return bigDecimalValue.toPlainString();
  }
}
