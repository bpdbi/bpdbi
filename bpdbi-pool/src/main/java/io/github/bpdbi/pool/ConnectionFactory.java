package io.github.bpdbi.pool;

import io.github.bpdbi.core.Connection;
import org.jspecify.annotations.NonNull;

/** Factory for creating new database connections. */
@FunctionalInterface
public interface ConnectionFactory {

  /** Create a new database connection. */
  @NonNull Connection create();
}
