package io.djb.impl;

import io.djb.TypeRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NamedParamParserTest {

    private final TypeRegistry registry = TypeRegistry.defaults();

    @Test
    void singleParam() {
        var result = NamedParamParser.parse(
            "SELECT * FROM users WHERE id = :id",
            Map.of("id", 42), "$", registry);
        assertEquals("SELECT * FROM users WHERE id = $1", result.sql());
        assertArrayEquals(new String[]{"42"}, result.params());
    }

    @Test
    void multipleParams() {
        var result = NamedParamParser.parse(
            "SELECT * FROM users WHERE name = :name AND age > :age",
            Map.of("name", "Alice", "age", 21), "$", registry);
        assertEquals("SELECT * FROM users WHERE name = $1 AND age > $2", result.sql());
        assertEquals(2, result.params().length);
    }

    @Test
    void mysqlPlaceholders() {
        var result = NamedParamParser.parse(
            "SELECT * FROM users WHERE id = :id",
            Map.of("id", 42), "?", registry);
        assertEquals("SELECT * FROM users WHERE id = ?", result.sql());
        assertArrayEquals(new String[]{"42"}, result.params());
    }

    @Test
    void paramInsideQuotesNotReplaced() {
        var result = NamedParamParser.parse(
            "SELECT ':not_a_param' AS val WHERE id = :id",
            Map.of("id", 1), "$", registry);
        assertEquals("SELECT ':not_a_param' AS val WHERE id = $1", result.sql());
        assertEquals(1, result.params().length);
    }

    @Test
    void paramInsideDoubleQuotesNotReplaced() {
        var result = NamedParamParser.parse(
            "SELECT \":not_a_param\" AS val WHERE id = :id",
            Map.of("id", 1), "$", registry);
        assertEquals("SELECT \":not_a_param\" AS val WHERE id = $1", result.sql());
    }

    @Test
    void nullParam() {
        var result = NamedParamParser.parse(
            "INSERT INTO t VALUES (:val)",
            Map.of("val", "test"), "$", registry);
        assertEquals("INSERT INTO t VALUES ($1)", result.sql());
    }

    @Test
    void underscoreInParamName() {
        var result = NamedParamParser.parse(
            "SELECT :first_name, :last_name",
            Map.of("first_name", "Alice", "last_name", "Smith"), "$", registry);
        assertEquals("SELECT $1, $2", result.sql());
    }

    @Test
    void missingParamThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            NamedParamParser.parse("SELECT :missing", Map.of(), "$", registry));
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
        var result = NamedParamParser.parse(
            "SELECT :val::int",
            Map.of("val", 42), "$", registry);
        assertEquals("SELECT $1::int", result.sql());
        assertArrayEquals(new String[]{"42"}, result.params());
    }
}
