package io.github.bpdbi.core.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * Parses Postgres text-format array literals ({@code {val1,val2,...}}) into a list of element
 * strings. Handles:
 *
 * <ul>
 *   <li>Quoted elements with backslash escaping: {@code {"hello \"world\"",other}}
 *   <li>NULL literals (excluded from result — use the nullable typed getters for null-aware access)
 *   <li>Empty arrays: {@code {}}
 *   <li>Nested arrays are not supported (flattened to string representation)
 * </ul>
 */
public final class PgArrayParser {

  private PgArrayParser() {}

  /**
   * Parse a Postgres array literal string into a list of element strings.
   *
   * @param text the array literal, e.g. {@code {1,2,3}} or {@code {"hello","world"}}
   * @return list of element strings (NULL elements are excluded)
   * @throws IllegalArgumentException if the text is not a valid array literal
   */
  public static @NonNull List<String> parse(@NonNull String text) {
    if (text.isEmpty() || text.charAt(0) != '{') {
      throw new IllegalArgumentException("Not a Postgres array literal: " + text);
    }
    if (text.equals("{}")) {
      return Collections.emptyList();
    }
    List<String> result = new ArrayList<>();
    int len = text.length();
    int i = 1; // skip opening '{'

    while (i < len) {
      char c = text.charAt(i);
      if (c == '}') {
        break;
      }
      if (c == ',') {
        i++;
        continue;
      }
      if (c == '"') {
        // Quoted element
        i++;
        var sb = new StringBuilder();
        while (i < len) {
          char qc = text.charAt(i);
          if (qc == '\\' && i + 1 < len) {
            sb.append(text.charAt(i + 1));
            i += 2;
          } else if (qc == '"') {
            i++;
            break;
          } else {
            sb.append(qc);
            i++;
          }
        }
        result.add(sb.toString());
      } else {
        // Unquoted element (may be NULL)
        int start = i;
        while (i < len && text.charAt(i) != ',' && text.charAt(i) != '}') {
          i++;
        }
        String element = text.substring(start, i);
        if (!"NULL".equals(element)) {
          result.add(element);
        }
      }
    }
    return result;
  }
}
