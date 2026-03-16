package io.djb.impl;

import io.djb.TypeRegistry;

import java.util.ArrayList;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Parses SQL with :name style named parameters and converts to positional placeholders.
 * Correctly skips parameters inside single-quoted strings, double-quoted identifiers,
 * line comments ({@code --}), and block comments ({@code /* ... * /}).
 */
public final class NamedParamParser {

    private NamedParamParser() {}

    public record ParsedQuery(String sql, @Nullable String[] params) {}

    /**
     * Parse SQL containing :name parameters and convert to positional placeholders.
     *
     * @param sql the SQL with :name parameters
     * @param params the parameter values keyed by name
     * @param placeholderPrefix "$" for PG (generates $1, $2), "?" for MySQL (generates ?)
     * @param typeRegistry for converting values to strings
     * @return parsed query with positional SQL and ordered parameter array
     */
    public static ParsedQuery parse(String sql, Map<String, @Nullable Object> params,
                                     String placeholderPrefix, TypeRegistry typeRegistry) {
        var result = new StringBuilder(sql.length());
        var paramValues = new ArrayList<String>();
        int paramIndex = 0;
        int len = sql.length();

        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);

            // Line comment: -- until end of line
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                if (end == -1) end = len;
                result.append(sql, i, end);
                i = end - 1; // loop will i++
                continue;
            }

            // Block comment: /* ... */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end == -1) end = len - 2; // unterminated comment: consume rest
                end += 2; // include the */
                result.append(sql, i, end);
                i = end - 1; // loop will i++
                continue;
            }

            // Single-quoted string literal (with '' escape)
            if (c == '\'') {
                result.append(c);
                i++;
                while (i < len) {
                    char sc = sql.charAt(i);
                    result.append(sc);
                    if (sc == '\'') {
                        // Check for escaped quote ''
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                            i++;
                            result.append(sql.charAt(i));
                        } else {
                            break; // end of string
                        }
                    } else if (sc == '\\') {
                        // Backslash escape: copy next char too
                        if (i + 1 < len) {
                            i++;
                            result.append(sql.charAt(i));
                        }
                    }
                    i++;
                }
                continue;
            }

            // Double-quoted identifier
            if (c == '"') {
                result.append(c);
                i++;
                while (i < len) {
                    char dc = sql.charAt(i);
                    result.append(dc);
                    if (dc == '"') {
                        // Check for escaped quote ""
                        if (i + 1 < len && sql.charAt(i + 1) == '"') {
                            i++;
                            result.append(sql.charAt(i));
                        } else {
                            break; // end of identifier
                        }
                    }
                    i++;
                }
                continue;
            }

            // Named parameter :name (but not :: cast operator)
            if (c == ':' && i + 1 < len
                && isNameStart(sql.charAt(i + 1))
                && (i == 0 || sql.charAt(i - 1) != ':')) {
                int nameStart = i + 1;
                int nameEnd = nameStart;
                while (nameEnd < len && isNameChar(sql.charAt(nameEnd))) {
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
