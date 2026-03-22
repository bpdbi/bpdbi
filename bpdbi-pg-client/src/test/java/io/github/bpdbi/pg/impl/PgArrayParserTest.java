package io.github.bpdbi.pg.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for PgArrayParser edge cases. */
class PgArrayParserTest {

  @Test
  void emptyArray() {
    assertEquals(List.of(), PgArrayParser.parse("{}"));
  }

  @Test
  void simpleUnquoted() {
    assertEquals(List.of("1", "2", "3"), PgArrayParser.parse("{1,2,3}"));
  }

  @Test
  void quotedElements() {
    assertEquals(List.of("hello world", "foo"), PgArrayParser.parse("{\"hello world\",\"foo\"}"));
  }

  @Test
  void escapedQuotesInsideQuotedElement() {
    // PG format: {"with\"quote"} → element is: with"quote
    assertEquals(List.of("with\"quote"), PgArrayParser.parse("{\"with\\\"quote\"}"));
  }

  @Test
  void escapedBackslashInsideQuotedElement() {
    // PG format: {"back\\slash"} → element is: back\slash
    assertEquals(List.of("back\\slash"), PgArrayParser.parse("{\"back\\\\slash\"}"));
  }

  @Test
  void nullElementsFiltered() {
    assertEquals(List.of("a", "c"), PgArrayParser.parse("{a,NULL,c}"));
  }

  @Test
  void allNullElements() {
    assertEquals(List.of(), PgArrayParser.parse("{NULL,NULL}"));
  }

  @Test
  void mixedQuotedAndUnquoted() {
    assertEquals(List.of("plain", "has space"), PgArrayParser.parse("{plain,\"has space\"}"));
  }

  @Test
  void emptyQuotedString() {
    // {"",other} → element 0 is empty string, element 1 is "other"
    assertEquals(List.of("", "other"), PgArrayParser.parse("{\"\",other}"));
  }

  @Test
  void quotedNullIsNotFiltered() {
    // {"NULL"} is a quoted element with value "NULL", not a NULL marker
    assertEquals(List.of("NULL"), PgArrayParser.parse("{\"NULL\"}"));
  }

  @Test
  void specialCharactersInQuotedElement() {
    assertEquals(List.of("{nested}", "a,b"), PgArrayParser.parse("{\"{nested}\",\"a,b\"}"));
  }

  @Test
  void invalidInputThrows() {
    assertThrows(IllegalArgumentException.class, () -> PgArrayParser.parse("not an array"));
    assertThrows(IllegalArgumentException.class, () -> PgArrayParser.parse(""));
  }

  @Test
  void singleElement() {
    assertEquals(List.of("42"), PgArrayParser.parse("{42}"));
  }

  @Test
  void singleQuotedElement() {
    assertEquals(List.of("hello"), PgArrayParser.parse("{\"hello\"}"));
  }

  @Test
  void numericElements() {
    var result = PgArrayParser.parse("{3.14,2.71,1.41}");
    assertEquals(3, result.size());
    assertEquals("3.14", result.get(0));
  }

  @Test
  void booleanElements() {
    assertEquals(List.of("t", "f", "t"), PgArrayParser.parse("{t,f,t}"));
  }

  @Test
  void nullAtStartAndEnd() {
    assertEquals(List.of("middle"), PgArrayParser.parse("{NULL,middle,NULL}"));
  }

  @Test
  void longValues() {
    String longVal = "a".repeat(1000);
    assertEquals(List.of(longVal), PgArrayParser.parse("{" + longVal + "}"));
  }
}
