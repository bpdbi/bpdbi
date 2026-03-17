package io.github.bpdbi.pool;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Base exception for connection pool errors. */
@SuppressWarnings("serial")
public class PoolException extends RuntimeException {

  public PoolException(@NonNull String message) {
    super(message);
  }

  public PoolException(@NonNull String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
