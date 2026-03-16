package io.djb.impl;

import io.djb.TypeRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses SQL with :name style named parameters and converts to positional placeholders.
 */
public final class NamedParamParser {

    private NamedParamParser() {}

    public record ParsedQuery(String sql, String[] params) {}

    /**
     * Parse SQL containing :name parameters and convert to positional placeholders.
     *
     * @param sql the SQL with :name parameters
     * @param params the parameter values keyed by name
     * @param placeholderPrefix "$" for PG (generates $1, $2), "?" for MySQL (generates ?)
     * @param typeRegistry for converting values to strings
     * @return parsed query with positional SQL and ordered parameter array
     */
    public static ParsedQuery parse(String sql, Map<String, Object> params,
                                     String placeholderPrefix, TypeRegistry typeRegistry) {
        var result = new StringBuilder(sql.length());
        var paramValues = new ArrayList<String>();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean escaped = false;
        int paramIndex = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (escaped) {
                result.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                result.append(c);
                escaped = true;
                continue;
            }
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                result.append(c);
                continue;
            }
            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                result.append(c);
                continue;
            }

            if (c == ':' && !inSingleQuote && !inDoubleQuote && i + 1 < sql.length()
                && isNameStart(sql.charAt(i + 1))
                && (i == 0 || sql.charAt(i - 1) != ':')) { // skip :: (PG cast)
                // Found a named parameter
                int nameStart = i + 1;
                int nameEnd = nameStart;
                while (nameEnd < sql.length() && isNameChar(sql.charAt(nameEnd))) {
                    nameEnd++;
                }
                String name = sql.substring(nameStart, nameEnd);
                if (!params.containsKey(name)) {
                    throw new IllegalArgumentException("Missing named parameter: :" + name);
                }
                paramIndex++;
                if ("$".equals(placeholderPrefix)) {
                    result.append('$').append(paramIndex);
                } else {
                    result.append('?');
                }
                paramValues.add(typeRegistry.bind(params.get(name)));
                i = nameEnd - 1; // skip the name (loop will i++)
            } else {
                result.append(c);
            }
        }

        return new ParsedQuery(result.toString(), paramValues.toArray(new String[0]));
    }

    private static boolean isNameStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isNameChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
