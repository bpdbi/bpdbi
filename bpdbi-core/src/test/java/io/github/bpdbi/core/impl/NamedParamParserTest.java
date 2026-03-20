package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.BinderRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NamedParamParserTest {

  private final BinderRegistry registry = BinderRegistry.defaults();

  @Test
  void singleParam() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM users WHERE id = :id", Map.of("id", 42), "$", registry);
    assertEquals("SELECT * FROM users WHERE id = $1", result.sql());
    assertArrayEquals(new String[] {"42"}, result.params());
  }

  @Test
  void multipleParams() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM users WHERE name = :name AND age > :age",
            Map.of("name", "Alice", "age", 21),
            "$",
            registry);
    assertEquals("SELECT * FROM users WHERE name = $1 AND age > $2", result.sql());
    assertEquals(2, result.params().length);
  }

  @Test
  void mysqlPlaceholders() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM users WHERE id = :id", Map.of("id", 42), "?", registry);
    assertEquals("SELECT * FROM users WHERE id = ?", result.sql());
    assertArrayEquals(new String[] {"42"}, result.params());
  }

  @Test
  void paramInsideQuotesNotReplaced() {
    var result =
        NamedParamParser.parse(
            "SELECT ':not_a_param' AS val WHERE id = :id", Map.of("id", 1), "$", registry);
    assertEquals("SELECT ':not_a_param' AS val WHERE id = $1", result.sql());
    assertEquals(1, result.params().length);
  }

  @Test
  void paramInsideDoubleQuotesNotReplaced() {
    var result =
        NamedParamParser.parse(
            "SELECT \":not_a_param\" AS val WHERE id = :id", Map.of("id", 1), "$", registry);
    assertEquals("SELECT \":not_a_param\" AS val WHERE id = $1", result.sql());
  }

  @Test
  void nullParam() {
    var result =
        NamedParamParser.parse("INSERT INTO t VALUES (:val)", Map.of("val", "test"), "$", registry);
    assertEquals("INSERT INTO t VALUES ($1)", result.sql());
  }

  @Test
  void underscoreInParamName() {
    var result =
        NamedParamParser.parse(
            "SELECT :first_name, :last_name",
            Map.of("first_name", "Alice", "last_name", "Smith"),
            "$",
            registry);
    assertEquals("SELECT $1, $2", result.sql());
  }

  @Test
  void missingParamThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> NamedParamParser.parse("SELECT :missing", Map.of(), "$", registry));
  }

  @Test
  void noParams() {
    var result = NamedParamParser.parse("SELECT 1", Map.of(), "$", registry);
    assertEquals("SELECT 1", result.sql());
    assertEquals(0, result.params().length);
  }

  @Test
  void colonInCast() {
    // ::type is a PG cast, not a named param — should be preserved
    var result = NamedParamParser.parse("SELECT :val::int", Map.of("val", 42), "$", registry);
    assertEquals("SELECT $1::int", result.sql());
    assertArrayEquals(new String[] {"42"}, result.params());
  }

  // --- Collection/array expansion tests ---

  @Test
  void listExpansionPg() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM users WHERE id IN (:ids)",
            Map.of("ids", List.of(1, 2, 3)),
            "$",
            registry);
    assertEquals("SELECT * FROM users WHERE id IN ($1, $2, $3)", result.sql());
    assertArrayEquals(new String[] {"1", "2", "3"}, result.params());
  }

  @Test
  void listExpansionMysql() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM users WHERE id IN (:ids)",
            Map.of("ids", List.of(1, 2, 3)),
            "?",
            registry);
    assertEquals("SELECT * FROM users WHERE id IN (?, ?, ?)", result.sql());
    assertArrayEquals(new String[] {"1", "2", "3"}, result.params());
  }

  @Test
  void listExpansionSingleElement() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM t WHERE id IN (:ids)", Map.of("ids", List.of(42)), "$", registry);
    assertEquals("SELECT * FROM t WHERE id IN ($1)", result.sql());
    assertArrayEquals(new String[] {"42"}, result.params());
  }

  @Test
  void listExpansionEmpty() {
    var params = new HashMap<String, Object>();
    params.put("ids", List.of());
    var result =
        NamedParamParser.parse("SELECT * FROM t WHERE id IN (:ids)", params, "$", registry);
    assertEquals("SELECT * FROM t WHERE id IN (NULL)", result.sql());
    assertEquals(0, result.params().length);
  }

  @Test
  void listExpansionMixedWithScalar() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM t WHERE status = :status AND id IN (:ids)",
            Map.of("status", "active", "ids", List.of(1, 2)),
            "$",
            registry);
    assertEquals("SELECT * FROM t WHERE status = $1 AND id IN ($2, $3)", result.sql());
    assertArrayEquals(new String[] {"active", "1", "2"}, result.params());
  }

  @Test
  void arrayExpansion() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM t WHERE id IN (:ids)",
            Map.of("ids", new int[] {10, 20, 30}),
            "$",
            registry);
    assertEquals("SELECT * FROM t WHERE id IN ($1, $2, $3)", result.sql());
    assertArrayEquals(new String[] {"10", "20", "30"}, result.params());
  }

  @Test
  void stringListExpansion() {
    var result =
        NamedParamParser.parse(
            "SELECT * FROM t WHERE name IN (:names)",
            Map.of("names", List.of("Alice", "Bob")),
            "$",
            registry);
    assertEquals("SELECT * FROM t WHERE name IN ($1, $2)", result.sql());
    assertArrayEquals(new String[] {"Alice", "Bob"}, result.params());
  }

  // --- parseTemplate tests ---

  @Test
  void parseTemplatePg() {
    var template =
        NamedParamParser.parseTemplate(
            "SELECT * FROM users WHERE name = :name AND age = :age", "$");
    assertEquals("SELECT * FROM users WHERE name = $1 AND age = $2", template.sql());
    assertEquals(List.of("name", "age"), template.parameterNames());
  }

  @Test
  void parseTemplateMysql() {
    var template =
        NamedParamParser.parseTemplate(
            "SELECT * FROM users WHERE name = :name AND age = :age", "?");
    assertEquals("SELECT * FROM users WHERE name = ? AND age = ?", template.sql());
    assertEquals(List.of("name", "age"), template.parameterNames());
  }

  @Test
  void parseTemplateNoNamedParams() {
    var template = NamedParamParser.parseTemplate("SELECT $1::int", "$");
    assertEquals("SELECT $1::int", template.sql());
    assertTrue(template.parameterNames().isEmpty());
  }

  @Test
  void parseTemplateSkipsQuotedAndComments() {
    var template =
        NamedParamParser.parseTemplate("SELECT ':skip' -- :comment\nWHERE id = :id", "$");
    assertEquals("SELECT ':skip' -- :comment\nWHERE id = $1", template.sql());
    assertEquals(List.of("id"), template.parameterNames());
  }

  @Test
  void parseTemplatePgCast() {
    var template = NamedParamParser.parseTemplate("SELECT :val::int", "$");
    assertEquals("SELECT $1::int", template.sql());
    assertEquals(List.of("val"), template.parameterNames());
  }

  // --- containsNamedParams tests ---

  @Test
  void containsNamedParamsTrue() {
    assertTrue(NamedParamParser.containsNamedParams("SELECT :name"));
  }

  @Test
  void containsNamedParamsFalseForCast() {
    assertFalse(NamedParamParser.containsNamedParams("SELECT 1::int"));
  }

  @Test
  void containsNamedParamsFalseForPlainSql() {
    assertFalse(NamedParamParser.containsNamedParams("SELECT $1"));
  }

  // --- resolveParams tests ---

  @Test
  void resolveParamsBasic() {
    var result =
        NamedParamParser.resolveParams(List.of("name", "age"), Map.of("name", "Alice", "age", 30));
    assertArrayEquals(new Object[] {"Alice", 30}, result);
  }

  @Test
  void resolveParamsMissing() {
    assertThrows(
        IllegalArgumentException.class,
        () -> NamedParamParser.resolveParams(List.of("name"), Map.of()));
  }

  @Test
  void resolveParamsPassesThroughCollection() {
    var ids = List.of(1, 2, 3);
    var result = NamedParamParser.resolveParams(List.of("ids"), Map.of("ids", ids));
    assertSame(ids, result[0]);
  }

  @Test
  void resolveParamsPassesThroughArray() {
    var ids = new int[] {1, 2};
    var result = NamedParamParser.resolveParams(List.of("ids"), Map.of("ids", ids));
    assertSame(ids, result[0]);
  }

  @Test
  void resolveParamsWithNull() {
    var params = new HashMap<String, Object>();
    params.put("val", null);
    var result = NamedParamParser.resolveParams(List.of("val"), params);
    assertNull(result[0]);
  }

  // --- Dollar-quoted string tests ---

  @Test
  void paramInsideDollarQuoteNotSupported() {
    // Dollar-quoted strings ($tag$...$tag$) are NOT supported by the parser.
    // The :text inside is treated as a named parameter. This is a known limitation.
    // Users should use single-quoted strings instead of dollar-quoting when using named params.
    var result =
        NamedParamParser.parse(
            "SELECT $tag$some :text$tag$ WHERE x = :param",
            Map.of("text", "ignored", "param", "yes"),
            "$",
            registry);
    assertEquals(2, result.params().length);
  }

  @Test
  void repeatedParamName() {
    var result =
        NamedParamParser.parse(
            "SELECT :val AS a, :val AS b", Map.of("val", "hello"), "$", registry);
    assertEquals("SELECT $1 AS a, $2 AS b", result.sql());
    assertArrayEquals(new String[] {"hello", "hello"}, result.params());
  }

  @Test
  void pgCastWithMultipleParts() {
    var result =
        NamedParamParser.parse(
            "SELECT :val::timestamp::date", Map.of("val", "2025-01-01"), "$", registry);
    assertEquals("SELECT $1::timestamp::date", result.sql());
    assertEquals(1, result.params().length);
  }

  @Test
  void paramInsideBlockCommentIgnored() {
    var result =
        NamedParamParser.parse(
            "SELECT /* :not_a_param */ :real", Map.of("real", "x"), "$", registry);
    assertEquals("SELECT /* :not_a_param */ $1", result.sql());
    assertEquals(1, result.params().length);
  }
}
