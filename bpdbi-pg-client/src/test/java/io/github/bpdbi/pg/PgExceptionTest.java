package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for PgException accessors and toString formatting. */
class PgExceptionTest {

  @Test
  void accessorsFull() {
    var ex =
        new PgException(
            "ERROR",
            "23505",
            "duplicate key",
            "Key (id)=(1) already exists.",
            "fix it",
            "public",
            "users",
            "id",
            "users_pkey");
    assertEquals("ERROR", ex.severity());
    assertEquals("23505", ex.sqlState());
    assertEquals("duplicate key", ex.getMessage());
    assertEquals("Key (id)=(1) already exists.", ex.detail());
    assertEquals("fix it", ex.hint());
    assertEquals("public", ex.schema());
    assertEquals("users", ex.table());
    assertEquals("id", ex.column());
    assertEquals("users_pkey", ex.constraint());
  }

  @Test
  void accessorsShortConstructor() {
    var ex = new PgException("WARNING", "01000", "warning message", "some detail", "try again");
    assertEquals("some detail", ex.detail());
    assertEquals("try again", ex.hint());
    assertNull(ex.schema());
    assertNull(ex.table());
    assertNull(ex.column());
    assertNull(ex.constraint());
  }

  @Test
  void toStringWithAllFields() {
    var ex =
        new PgException(
            "ERROR",
            "23505",
            "duplicate key",
            "Key exists",
            "fix it",
            "public",
            "users",
            "id",
            "users_pkey");
    ex.setSql("INSERT INTO users VALUES (1)");
    String s = ex.toString();
    assertTrue(s.contains("PgException"));
    assertTrue(s.contains("ERROR"));
    assertTrue(s.contains("23505"));
    assertTrue(s.contains("duplicate key"));
    assertTrue(s.contains("Key exists"));
    assertTrue(s.contains("fix it"));
    assertTrue(s.contains("INSERT INTO users VALUES (1)"));
  }

  @Test
  void toStringMinimal() {
    var ex = new PgException(null, null, "oops", null, null);
    String s = ex.toString();
    assertTrue(s.contains("oops"));
  }

  @Test
  void toStringWithDetailAndHintOnly() {
    var ex =
        new PgException("ERROR", "42P01", "relation does not exist", "The table was dropped", null);
    String s = ex.toString();
    assertTrue(s.contains("Detail: The table was dropped"));
  }
}
