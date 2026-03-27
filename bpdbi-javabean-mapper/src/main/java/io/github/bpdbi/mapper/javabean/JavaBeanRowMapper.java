package io.github.bpdbi.mapper.javabean;

import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowMapper;
import io.github.bpdbi.core.spi.RowExtractors;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
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

  private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

  private final Slot[] slots;
  private final MethodHandle constructor;

  private JavaBeanRowMapper(Class<T> beanType) {
    this.constructor = resolveConstructor(beanType);
    this.slots = buildSlots(beanType);
  }

  /** Create a mapper for the given JavaBean type. */
  public static <T> @NonNull JavaBeanRowMapper<T> of(@NonNull Class<T> beanType) {
    return new JavaBeanRowMapper<>(beanType);
  }

  @Override
  public @NonNull T map(@NonNull Row row) {
    int[] cursor = {0};
    @SuppressWarnings("unchecked") // safe: constructor produces T
    T result = (T) populate(constructor, slots, row, cursor);
    return result;
  }

  // --- Internal: slot tree for nested beans ---

  private sealed interface Slot {}

  private record ScalarSlot(MethodHandle setter, BiFunction<Row, Integer, Object> extractor)
      implements Slot {}

  @SuppressWarnings("ArrayRecordComponent")
  // internal record; array represents an ordered tree of children
  private record NestedSlot(MethodHandle setter, MethodHandle constructor, Slot[] children)
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

      Class<?> type = pd.getPropertyType();
      MethodHandle setter = unreflectSetter(pd);

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

  private static Object populate(MethodHandle ctor, Slot[] slots, Row row, int[] cursor) {
    Object instance;
    try {
      instance = ctor.invoke();
    } catch (Throwable e) {
      throw new IllegalStateException("Failed to construct bean", e);
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
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to set property on bean", e);
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

  private static MethodHandle resolveConstructor(Class<?> type) {
    try {
      var ctor = type.getDeclaredConstructor();
      ctor.setAccessible(true);
      return LOOKUP.unreflectConstructor(ctor);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(type.getName() + " must have a no-arg constructor");
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Cannot access constructor of " + type.getName(), e);
    }
  }

  private static MethodHandle unreflectSetter(PropertyDescriptor pd) {
    try {
      var setter = pd.getWriteMethod();
      setter.setAccessible(true);
      return LOOKUP.unreflect(setter);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          "Cannot access setter for property '" + pd.getName() + "'", e);
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
