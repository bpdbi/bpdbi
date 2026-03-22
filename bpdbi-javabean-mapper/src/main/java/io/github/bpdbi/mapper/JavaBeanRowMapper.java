package io.github.bpdbi.mapper;

import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowExtractors;
import io.github.bpdbi.core.RowMapper;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.jspecify.annotations.NonNull;

/**
 * A {@link RowMapper} that maps rows to JavaBeans (POJOs with a no-arg constructor and setter
 * methods) by consuming columns in field declaration order.
 *
 * <p>Nested beans are flattened: their fields consume consecutive columns from the row, just like
 * the record and Kotlin mappers.
 *
 * <pre>{@code
 * public class Address {
 *     private String street;
 *     private String city;
 *     public Address() {}
 *     // getters + setters...
 * }
 *
 * public class User {
 *     private int id;
 *     private String name;
 *     private Address address;
 *     public User() {}
 *     // getters + setters...
 * }
 *
 * // SELECT id, name, street, city FROM ...
 * RowMapper<User> mapper = JavaBeanRowMapper.of(User.class);
 * List<User> users = conn.query("SELECT id, name, street, city FROM ...").mapTo(mapper);
 * }</pre>
 */
public final class JavaBeanRowMapper<T> implements RowMapper<T> {

  private final Slot[] slots;
  private final Class<T> beanType;
  private final Constructor<T> constructor;

  private JavaBeanRowMapper(Class<T> beanType) {
    this.constructor = resolveConstructor(beanType);
    this.beanType = beanType;
    this.slots = buildSlots(beanType);
  }

  /** Create a mapper for the given JavaBean type. */
  public static <T> @NonNull JavaBeanRowMapper<T> of(@NonNull Class<T> beanType) {
    return new JavaBeanRowMapper<>(beanType);
  }

  @Override
  public @NonNull T map(@NonNull Row row) {
    int[] cursor = {0};
    return populate(constructor, slots, row, cursor);
  }

  // --- Internal: slot tree for nested beans ---

  private sealed interface Slot {}

  private record ScalarSlot(Method setter, BiFunction<Row, Integer, Object> extractor)
      implements Slot {}

  @SuppressWarnings("ArrayRecordComponent")
  // internal record; array represents an ordered tree of children
  private record NestedSlot(Method setter, Constructor<?> constructor, Slot[] children)
      implements Slot {}

  private static Slot[] buildSlots(Class<?> beanType) {
    Map<String, PropertyDescriptor> descriptorMap = propertyDescriptorMap(beanType);

    // Use declared field order for stable column positioning
    List<Slot> slots = new ArrayList<>();
    for (Field field : beanType.getDeclaredFields()) {
      PropertyDescriptor pd = descriptorMap.get(field.getName());
      if (pd == null || pd.getWriteMethod() == null) {
        continue;
      }

      Method setter = pd.getWriteMethod();
      setter.setAccessible(true);
      Class<?> type = pd.getPropertyType();

      BiFunction<Row, Integer, Object> extractor = RowExtractors.extractorFor(type);
      if (extractor != null) {
        slots.add(new ScalarSlot(setter, extractor));
      } else if (isBean(type)) {
        slots.add(new NestedSlot(setter, resolveConstructor(type), buildSlots(type)));
      } else {
        throw new IllegalArgumentException(
            "Unsupported type "
                + type.getName()
                + " for property '"
                + pd.getName()
                + "'. "
                + "Supported: "
                + RowExtractors.SUPPORTED_TYPES_MESSAGE
                + ", and nested beans");
      }
    }
    return slots.toArray(Slot[]::new);
  }

  // Unchecked is safe: populate() is only called with the matching constructor from Slot metadata
  @SuppressWarnings("unchecked")
  private static <T> T populate(Constructor<T> ctor, Slot[] slots, Row row, int[] cursor) {
    T instance;
    try {
      instance = ctor.newInstance();
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Failed to construct " + ctor.getDeclaringClass().getName(), e);
    }
    for (Slot slot : slots) {
      try {
        switch (slot) {
          case ScalarSlot s -> s.setter().invoke(instance, s.extractor().apply(row, cursor[0]++));
          case NestedSlot n -> {
            Object nested = populate(n.constructor(), n.children(), row, cursor);
            n.setter().invoke(instance, nested);
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to set property on " + ctor.getDeclaringClass().getName(), e);
      }
    }
    return instance;
  }

  private static boolean isBean(Class<?> type) {
    if (type.isPrimitive() || type.isArray() || type.isEnum() || type.isInterface()) {
      return false;
    }
    if (type.getPackageName().startsWith("java.")) {
      return false;
    }
    try {
      var unused = type.getDeclaredConstructor();
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  @SuppressWarnings("unchecked") // caller ensures T matches the class
  private static <T> Constructor<T> resolveConstructor(Class<T> type) {
    try {
      Constructor<T> ctor = type.getDeclaredConstructor();
      ctor.setAccessible(true);
      return ctor;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(type.getName() + " must have a no-arg constructor");
    }
  }

  private static Map<String, PropertyDescriptor> propertyDescriptorMap(Class<?> beanType) {
    BeanInfo beanInfo;
    try {
      beanInfo = Introspector.getBeanInfo(beanType, Object.class);
    } catch (IntrospectionException e) {
      throw new IllegalArgumentException("Failed to introspect " + beanType.getName(), e);
    }
    Map<String, PropertyDescriptor> map = new HashMap<>();
    for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
      map.put(pd.getName(), pd);
    }
    return map;
  }
}
