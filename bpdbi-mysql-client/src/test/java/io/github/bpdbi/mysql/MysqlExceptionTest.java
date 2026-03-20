package io.github.bpdbi.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for MysqlException formatting. */
class MysqlExceptionTest {

  @Test
  void toStringWithCodeAndMessage() {
    var ex = new MysqlException(1045, "28000", "Access denied for user 'root'@'localhost'");
    String s = ex.toString();
    assertTrue(s.contains("MysqlException"));
    assertTrue(s.contains("Access denied"));
    assertTrue(s.contains("28000"));
    assertTrue(s.contains("1045"));
  }

  @Test
  void toStringWithSql() {
    var ex = new MysqlException(1146, "42S02", "Table 'test.foo' doesn't exist");
    ex.setSql("SELECT * FROM foo");
    String s = ex.toString();
    assertTrue(s.contains("SELECT * FROM foo"));
    assertTrue(s.contains("SQL:"));
  }

  @Test
  void errorCodeAccessor() {
    var ex = new MysqlException(1062, "23000", "Duplicate entry");
    assertEquals(1062, ex.errorCode());
    assertEquals("23000", ex.sqlState());
    assertEquals("ERROR", ex.severity());
  }
}
