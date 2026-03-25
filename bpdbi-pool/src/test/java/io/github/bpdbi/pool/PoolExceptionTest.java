package io.github.bpdbi.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PoolExceptionTest {

  @Test
  void poolExceptionMessageOnly() {
    var ex = new PoolException("pool error");
    assertEquals("pool error", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void poolExceptionMessageAndCause() {
    var cause = new RuntimeException("root cause");
    var ex = new PoolException("pool error", cause);
    assertEquals("pool error", ex.getMessage());
    assertSame(cause, ex.getCause());
  }

  @Test
  void poolTimeoutException() {
    var ex = new PoolTimeoutException(5000);
    assertTrue(ex.getMessage().contains("5000"));
    assertTrue(ex instanceof PoolException);
  }

  @Test
  void poolExhaustedException() {
    var ex = new PoolExhaustedException(100);
    assertTrue(ex.getMessage().contains("100"));
    assertTrue(ex instanceof PoolException);
  }
}
