package io.github.bpdbi.core.impl;

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
 * (:ids)} with a 3-element list becomes {@code WHERE id IN ($1, $2, $3)}.
 */
public final class NamedParamParser {

  private NamedParamParser() {}

  public record ParsedQuery(String sql, Object[] params) {}

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
   * @param placeholderPrefix "$" for PG (generates $1, $2), or "?" for drivers that use positional
   *     ?
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

      int skip = skipLiteral(sql, i, len, result);
      if (skip >= 0) {
        i = skip - 1; // -1 because loop will i++
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

  /**
   * Quick check whether SQL contains named parameters (`:name` but not `::`), skipping literals.
   */
  public static boolean containsNamedParams(@NonNull String sql) {
    int len = sql.length();
    for (int i = 0; i < len - 1; i++) {
      char c = sql.charAt(i);
      int skip = skipLiteralNoAppend(sql, i, len);
      if (skip >= 0) {
        i = skip - 1;
        continue;
      }
      if (c == ':' && isNameStart(sql.charAt(i + 1)) && (i == 0 || sql.charAt(i - 1) != ':')) {
        return true;
      }
    }
    return false;
  }

  /** Like {@link #skipLiteral} but without appending to a StringBuilder. Returns -1 if no skip. */
  private static int skipLiteralNoAppend(String sql, int i, int len) {
    char c = sql.charAt(i);
    if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
      int end = sql.indexOf('\n', i);
      return end == -1 ? len : end;
    }
    if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
      int end = sql.indexOf("*/", i + 2);
      return (end == -1 ? len - 2 : end) + 2;
    }
    if (c == '\'') {
      i++;
      while (i < len) {
        if (sql.charAt(i) == '\'') {
          i++;
          if (i >= len || sql.charAt(i) != '\'') return i;
        } else {
          i++;
        }
      }
      return len;
    }
    return -1;
  }

  /**
   * Parse SQL containing :name parameters and convert to positional placeholders.
   *
   * @param sql the SQL with :name parameters
   * @param params the parameter values keyed by name
   * @param placeholderPrefix "$" for PG (generates $1, $2), or "?" for drivers that use positional
   *     ?
   * @return parsed query with positional SQL and ordered parameter array
   */
  public static @NonNull ParsedQuery parse(
      @NonNull String sql, @NonNull Map<String, Object> params, @NonNull String placeholderPrefix) {
    var result = new StringBuilder(sql.length());
    var paramValues = new ArrayList<>();
    int paramIndex = 0;
    int len = sql.length();

    for (int i = 0; i < len; i++) {
      char c = sql.charAt(i);

      int skip = skipLiteral(sql, i, len, result);
      if (skip >= 0) {
        i = skip - 1; // -1 because loop will i++
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
            paramValues.add(element);
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
          paramValues.add(value);
        }
        i = nameEnd - 1; // skip the name (loop will i++)
      } else {
        result.append(c);
      }
    }

    return new ParsedQuery(result.toString(), paramValues.toArray(new Object[0]));
  }

  /**
   * Resolve a named parameter Map to a positional Object array using the given parameter names.
   * Throws if a required parameter is missing. Collection/array values are passed through as-is —
   * the caller is responsible for handling them (e.g., converting to Postgres array literals).
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

  /**
   * Skip past a comment, string literal, or quoted identifier starting at position {@code i}.
   * Appends the skipped content to {@code result}. Returns the new position (index of the next char
   * to process), or {@code -1} if the character at position {@code i} is not the start of such a
   * token.
   */
  private static int skipLiteral(String sql, int i, int len, StringBuilder result) {
    char c = sql.charAt(i);

    // Line comment: -- until end of line
    if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
      int end = sql.indexOf('\n', i);
      if (end == -1) {
        end = len;
      }
      result.append(sql, i, end);
      return end;
    }

    // Block comment: /* ... */
    if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
      int end = sql.indexOf("*/", i + 2);
      if (end == -1) {
        end = len - 2; // unterminated comment: consume rest
      }
      end += 2; // include the */
      result.append(sql, i, end);
      return end;
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
      return i + 1;
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
      return i + 1;
    }

    return -1;
  }

  /**
   * Convert a value to a Collection if it is a Collection or array; return null otherwise. Uses
   * explicit primitive array checks instead of java.lang.reflect.Array to keep bpdbi-core
   * reflection-free.
   */
  private static Collection<?> toCollection(Object value) {
    if (value instanceof Collection<?> c) return c;
    if (value instanceof Object[] a) return List.of(a);
    if (value instanceof int[] a) {
      var list = new ArrayList<Object>(a.length);
      for (int v : a) list.add(v);
      return list;
    }
    if (value instanceof long[] a) {
      var list = new ArrayList<Object>(a.length);
      for (long v : a) list.add(v);
      return list;
    }
    if (value instanceof double[] a) {
      var list = new ArrayList<Object>(a.length);
      for (double v : a) list.add(v);
      return list;
    }
    if (value instanceof float[] a) {
      var list = new ArrayList<Object>(a.length);
      for (float v : a) list.add(v);
      return list;
    }
    if (value instanceof short[] a) {
      var list = new ArrayList<Object>(a.length);
      for (short v : a) list.add(v);
      return list;
    }
    if (value instanceof boolean[] a) {
      var list = new ArrayList<Object>(a.length);
      for (boolean v : a) list.add(v);
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
