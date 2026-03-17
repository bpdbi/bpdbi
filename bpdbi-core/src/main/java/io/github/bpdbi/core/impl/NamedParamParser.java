package io.github.bpdbi.core.impl;

import io.github.bpdbi.core.BinderRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.NonNull;

/**
 * Parses SQL with :name style named parameters and converts to positional placeholders. Correctly
 * skips parameters inside single-quoted strings, double-quoted identifiers, line comments ({@code
 * --}), and block comments ({@code /* ... * /}).
 *
 * <p>Supports collection expansion: if a named parameter value is a {@link Collection} or array,
 * the single placeholder is expanded to match the element count. For example, {@code WHERE id IN
 * (:ids)} with a 3-element list becomes {@code WHERE id IN ($1, $2, $3)} (PG) or {@code WHERE id IN
 * (?, ?, ?)} (MySQL).
 */
public final class NamedParamParser {

  private NamedParamParser() {}

  @SuppressWarnings("ArrayRecordComponent") // internal record; callers handle the array correctly
  public record ParsedQuery(String sql, String[] params) {}

  /**
   * A parsed SQL template with named parameter positions but no resolved values. Used by prepared
   * statements to store the name→position mapping at prepare time.
   */
  public record ParsedTemplate(String sql, List<String> parameterNames) {}

  /**
   * Parse SQL containing :name parameters and return the rewritten SQL plus the ordered parameter
   * names. No values are resolved — this is for prepare-time usage.
   *
   * @param sql the SQL with :name parameters
   * @param placeholderPrefix "$" for PG (generates $1, $2), "?" for MySQL (generates ?)
   * @return parsed template with positional SQL and ordered parameter names
   */
  public static @NonNull ParsedTemplate parseTemplate(
      @NonNull String sql, @NonNull String placeholderPrefix) {
    var result = new StringBuilder(sql.length());
    var paramNames = new ArrayList<String>();
    int paramIndex = 0;
    int len = sql.length();

    for (int i = 0; i < len; i++) {
      char c = sql.charAt(i);

      // Line comment: -- until end of line
      if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
        int end = sql.indexOf('\n', i);
        if (end == -1) {
          end = len;
        }
        result.append(sql, i, end);
        i = end - 1;
        continue;
      }

      // Block comment: /* ... */
      if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
        int end = sql.indexOf("*/", i + 2);
        if (end == -1) {
          end = len - 2;
        }
        end += 2;
        result.append(sql, i, end);
        i = end - 1;
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
            if (i + 1 < len && sql.charAt(i + 1) == '\'') {
              i++;
              result.append(sql.charAt(i));
            } else {
              break;
            }
          } else if (sc == '\\') {
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
            if (i + 1 < len && sql.charAt(i + 1) == '"') {
              i++;
              result.append(sql.charAt(i));
            } else {
              break;
            }
          }
          i++;
        }
        continue;
      }

      // Named parameter :name (but not :: cast operator)
      if (c == ':'
          && i + 1 < len
          && isNameStart(sql.charAt(i + 1))
          && (i == 0 || sql.charAt(i - 1) != ':')) {
        int nameStart = i + 1;
        int nameEnd = nameStart;
        while (nameEnd < len && isNameChar(sql.charAt(nameEnd))) {
          nameEnd++;
        }
        String name = sql.substring(nameStart, nameEnd);
        paramIndex++;
        if ("$".equals(placeholderPrefix)) {
          result.append('$').append(paramIndex);
        } else {
          result.append('?');
        }
        paramNames.add(name);
        i = nameEnd - 1;
      } else {
        result.append(c);
      }
    }

    return new ParsedTemplate(result.toString(), Collections.unmodifiableList(paramNames));
  }

  /** Quick check whether SQL contains named parameters (`:name` but not `::`). */
  public static boolean containsNamedParams(@NonNull String sql) {
    int len = sql.length();
    for (int i = 0; i < len - 1; i++) {
      char c = sql.charAt(i);
      if (c == ':' && isNameStart(sql.charAt(i + 1)) && (i == 0 || sql.charAt(i - 1) != ':')) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse SQL containing :name parameters and convert to positional placeholders.
   *
   * @param sql the SQL with :name parameters
   * @param params the parameter values keyed by name
   * @param placeholderPrefix "$" for PG (generates $1, $2), "?" for MySQL (generates ?)
   * @param binderRegistry for converting values to strings
   * @return parsed query with positional SQL and ordered parameter array
   */
  public static @NonNull ParsedQuery parse(
      @NonNull String sql,
      @NonNull Map<String, Object> params,
      @NonNull String placeholderPrefix,
      @NonNull BinderRegistry binderRegistry) {
    var result = new StringBuilder(sql.length());
    var paramValues = new ArrayList<String>();
    int paramIndex = 0;
    int len = sql.length();

    for (int i = 0; i < len; i++) {
      char c = sql.charAt(i);

      // Line comment: -- until end of line
      if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
        int end = sql.indexOf('\n', i);
        if (end == -1) {
          end = len;
        }
        result.append(sql, i, end);
        i = end - 1; // loop will i++
        continue;
      }

      // Block comment: /* ... */
      if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
        int end = sql.indexOf("*/", i + 2);
        if (end == -1) {
          end = len - 2; // unterminated comment: consume rest
        }
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
      if (c == ':'
          && i + 1 < len
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
        Object value = params.get(name);
        Collection<?> elements = toCollection(value);
        if (elements != null) {
          // Collection/array expansion: :ids -> $1, $2, $3
          boolean first = true;
          for (Object element : elements) {
            if (!first) {
              result.append(", ");
            }
            paramIndex++;
            if ("$".equals(placeholderPrefix)) {
              result.append('$').append(paramIndex);
            } else {
              result.append('?');
            }
            paramValues.add(binderRegistry.bind(element));
            first = false;
          }
          if (elements.isEmpty()) {
            // Empty collection: produce NULL to avoid SQL syntax error
            result.append("NULL");
          }
        } else {
          paramIndex++;
          if ("$".equals(placeholderPrefix)) {
            result.append('$').append(paramIndex);
          } else {
            result.append('?');
          }
          paramValues.add(binderRegistry.bind(value));
        }
        i = nameEnd - 1; // skip the name (loop will i++)
      } else {
        result.append(c);
      }
    }

    return new ParsedQuery(result.toString(), paramValues.toArray(new String[0]));
  }

  /**
   * Resolve a named parameter Map to a positional Object array using the given parameter names.
   * Throws if a required parameter is missing. Collection/array values are passed through as-is —
   * the caller is responsible for handling them (e.g. converting to database array literals for
   * Postgres, or rejecting them for MySQL).
   */
  public static @NonNull Object[] resolveParams(
      @NonNull List<String> parameterNames, @NonNull Map<String, Object> params) {
    Object[] result = new Object[parameterNames.size()];
    for (int i = 0; i < parameterNames.size(); i++) {
      String name = parameterNames.get(i);
      if (!params.containsKey(name)) {
        throw new IllegalArgumentException("Missing named parameter: :" + name);
      }
      result[i] = params.get(name);
    }
    return result;
  }

  /** Convert a value to a Collection if it is a Collection or array; return null otherwise. */
  private static Collection<?> toCollection(Object value) {
    if (value instanceof Collection<?> c) {
      return c;
    }
    if (value != null && value.getClass().isArray()) {
      // Convert primitive and object arrays to a list
      int length = java.lang.reflect.Array.getLength(value);
      var list = new ArrayList<>(length);
      for (int j = 0; j < length; j++) {
        list.add(java.lang.reflect.Array.get(value, j));
      }
      return list;
    }
    return null;
  }

  private static boolean isNameStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isNameChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }
}
