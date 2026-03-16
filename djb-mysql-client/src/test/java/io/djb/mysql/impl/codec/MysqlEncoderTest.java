package io.djb.mysql.impl.codec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MysqlEncoderTest {

    @Test
    void interpolateNoParams() {
        assertEquals("SELECT 1", MysqlEncoder.interpolateParams("SELECT 1", null));
        assertEquals("SELECT 1", MysqlEncoder.interpolateParams("SELECT 1", new String[0]));
    }

    @Test
    void interpolateSingleParam() {
        assertEquals("SELECT 'hello'",
            MysqlEncoder.interpolateParams("SELECT ?", new String[]{"hello"}));
    }

    @Test
    void interpolateMultipleParams() {
        assertEquals("SELECT 'a', 'b', 'c'",
            MysqlEncoder.interpolateParams("SELECT ?, ?, ?", new String[]{"a", "b", "c"}));
    }

    @Test
    void interpolateNull() {
        assertEquals("SELECT NULL",
            MysqlEncoder.interpolateParams("SELECT ?", new String[]{null}));
    }

    @Test
    void interpolateQuestionMarkInString() {
        // ? inside quotes should NOT be replaced
        assertEquals("SELECT '?' , 'hello'",
            MysqlEncoder.interpolateParams("SELECT '?' , ?", new String[]{"hello"}));
    }

    @Test
    void interpolateQuestionMarkInDoubleQuotedString() {
        assertEquals("SELECT \"?\" , 'hello'",
            MysqlEncoder.interpolateParams("SELECT \"?\" , ?", new String[]{"hello"}));
    }

    @Test
    void escapeSpecialChars() {
        assertEquals("it\\'s", MysqlEncoder.escapeString("it's"));
        assertEquals("line1\\nline2", MysqlEncoder.escapeString("line1\nline2"));
        assertEquals("tab\\there", MysqlEncoder.escapeString("tab\there"));
        assertEquals("back\\\\slash", MysqlEncoder.escapeString("back\\slash"));
        assertEquals("null\\0byte", MysqlEncoder.escapeString("null\0byte"));
    }

    @Test
    void interpolateWithSpecialChars() {
        assertEquals("SELECT 'it\\'s a \\\"test\\\"'",
            MysqlEncoder.interpolateParams("SELECT ?", new String[]{"it's a \"test\""}));
    }

    @Test
    void interpolateNumericValue() {
        assertEquals("SELECT '42'",
            MysqlEncoder.interpolateParams("SELECT ?", new String[]{"42"}));
    }
}
