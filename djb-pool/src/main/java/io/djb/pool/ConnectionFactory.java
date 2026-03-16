package io.djb.pool;

import io.djb.Connection;

/**
 * Factory for creating new database connections.
 */
@FunctionalInterface
public interface ConnectionFactory {

  /**
   * Create a new database connection.
   */
  Connection create();
}
