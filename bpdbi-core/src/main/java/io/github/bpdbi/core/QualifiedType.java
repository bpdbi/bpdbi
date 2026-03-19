package io.github.bpdbi.core;

import java.lang.annotation.Annotation;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.NonNull;

/**
 * A Java type paired with qualifier annotations for domain-specific binding. Enables different
 * encoding for the same Java class based on context.
 *
 * <p>Example: bind a {@code String} differently depending on whether it's a regular text value or
 * an NVarchar:
 *
 * <pre>{@code
 * var nvarcharString = QualifiedType.of(String.class, NVarchar.class);
 * registry.register(nvarcharString, v -> "N'" + v + "'");
 * }</pre>
 *
 * @param <T> the Java type
 * @see BinderRegistry#register(QualifiedType, Binder)
 */
public final class QualifiedType<T> {

  private final Class<T> type;
  private final Set<Class<? extends Annotation>> qualifiers;

  private QualifiedType(Class<T> type, Set<Class<? extends Annotation>> qualifiers) {
    this.type = type;
    this.qualifiers = Set.copyOf(qualifiers);
  }

  /** Create a qualified type with a single qualifier annotation. */
  public static <T> @NonNull QualifiedType<T> of(
      @NonNull Class<T> type, @NonNull Class<? extends Annotation> qualifier) {
    return new QualifiedType<>(type, Set.of(qualifier));
  }

  /** Create a qualified type with multiple qualifier annotations. */
  @SafeVarargs
  public static <T> @NonNull QualifiedType<T> of(
      @NonNull Class<T> type, @NonNull Class<? extends Annotation>... qualifiers) {
    return new QualifiedType<>(type, Set.of(qualifiers));
  }

  public @NonNull Class<T> type() {
    return type;
  }

  public @NonNull Set<Class<? extends Annotation>> qualifiers() {
    return qualifiers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QualifiedType<?> that)) {
      return false;
    }
    return type.equals(that.type) && qualifiers.equals(that.qualifiers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, qualifiers);
  }

  @Override
  public String toString() {
    if (qualifiers.isEmpty()) {
      return type.getSimpleName();
    }
    var sb = new StringBuilder();
    for (var q : qualifiers) {
      sb.append('@').append(q.getSimpleName()).append(' ');
    }
    sb.append(type.getSimpleName());
    return sb.toString();
  }
}
