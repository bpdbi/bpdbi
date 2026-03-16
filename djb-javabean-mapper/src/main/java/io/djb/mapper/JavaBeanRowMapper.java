package io.djb.mapper;

import io.djb.Row;
import io.djb.RowExtractors;
import io.djb.RowMapper;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * A {@link RowMapper} that maps rows to JavaBeans (POJOs with a no-arg constructor and setter
 * methods) by consuming columns in field declaration order.
 *
 * <p>Nested beans are flattened: their fields consume consecutive columns from the row,
 * just like the record and Kotlin mappers.
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

  private JavaBeanRowMapper(Class<T> beanType) {
    requireNoArgConstructor(beanType);
    this.beanType = beanType;
    this.slots = buildSlots(beanType);
  }

  /**
   * Create a mapper for the given JavaBean type.
   */
  public static <T> JavaBeanRowMapper<T> of(Class<T> beanType) {
    return new JavaBeanRowMapper<>(beanType);
  }

  @Override
  public T map(Row row) {
    int[] cursor = {0};
    return populate(beanType, slots, row, cursor);
  }

  // --- Internal: slot tree for nested beans ---

  private sealed interface Slot {

  }

  private record ScalarSlot(Method setter,
                            BiFunction<Row, Integer, Object> extractor) implements Slot {

  }

  @SuppressWarnings("ArrayRecordComponent")
  // internal record; array represents an ordered tree of children
  private record NestedSlot(Method setter, Class<?> nestedType, Slot[] children) implements Slot {

  }

  private static Slot[] buildSlots(Class<?> beanType) {
    requireNoArgConstructor(beanType);

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
        slots.add(new NestedSlot(setter, type, buildSlots(type)));
      } else {
        throw new IllegalArgumentException(
            "Unsupported type " + type.getName() + " for property '" + pd.getName() + "'. "
                + "Supported: " + RowExtractors.SUPPORTED_TYPES_MESSAGE + ", and nested beans");
      }
    }
    return slots.toArray(Slot[]::new);
  }

  @SuppressWarnings("unchecked")
  private static <T> T populate(Class<T> beanType, Slot[] slots, Row row, int[] cursor) {
    T instance;
    try {
      instance = beanType.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to construct " + beanType.getName(), e);
    }
    for (Slot slot : slots) {
      try {
        switch (slot) {
          case ScalarSlot s -> s.setter().invoke(instance, s.extractor().apply(row, cursor[0]++));
          case NestedSlot n -> {
            Object nested = populate(n.nestedType(), n.children(), row, cursor);
            n.setter().invoke(instance, nested);
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to set property on " + beanType.getName(), e);
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

  private static void requireNoArgConstructor(Class<?> type) {
    try {
      var unused = type.getDeclaredConstructor();
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
