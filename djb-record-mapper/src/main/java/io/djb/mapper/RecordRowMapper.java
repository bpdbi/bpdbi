package io.djb.mapper;

import io.djb.Row;
import io.djb.RowExtractors;
import io.djb.RowMapper;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.function.BiFunction;
import org.jspecify.annotations.NonNull;

/**
 * A {@link RowMapper} that maps rows to Java records by consuming columns in declaration order.
 * Nested records are flattened: their components consume the next consecutive columns from the row.
 *
 * <pre>{@code
 * record Address(String street, String city) {}
 * record User(int id, String name, Address address) {}
 *
 * // SELECT id, name, street, city FROM ...
 * RowMapper<User> mapper = RecordRowMapper.of(User.class);
 * List<User> users = conn.query("SELECT id, name, street, city FROM ...").mapTo(mapper);
 * }</pre>
 */
public final class RecordRowMapper<T extends Record> implements RowMapper<T> {

  private final Slot[] slots;
  private final Constructor<T> constructor;

  private RecordRowMapper(Class<T> recordType) {
    if (!recordType.isRecord()) {
      throw new IllegalArgumentException(recordType.getName() + " is not a record type");
    }
    this.slots = buildSlots(recordType);
    this.constructor = canonicalConstructor(recordType);
  }

  /** Create a mapper for the given record type. */
  public static <T extends Record> @NonNull RecordRowMapper<T> of(@NonNull Class<T> recordType) {
    return new RecordRowMapper<>(recordType);
  }

  @Override
  public @NonNull T map(@NonNull Row row) {
    int[] cursor = {0};
    Object[] args = extract(slots, constructor, row, cursor);
    try {
      return constructor.newInstance(args);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to construct record", e);
    }
  }

  // --- Internal: slot tree for nested records ---

  private sealed interface Slot {

    /** How many leaf columns this slot consumes. */
    int columnCount();
  }

  private record ScalarSlot(BiFunction<Row, Integer, Object> extractor) implements Slot {

    @Override
    public int columnCount() {
      return 1;
    }
  }

  private record NestedSlot(
      Slot[] children, Constructor<? extends Record> constructor, int columnCount)
      implements Slot {}

  private static Slot[] buildSlots(Class<? extends Record> recordType) {
    RecordComponent[] components = recordType.getRecordComponents();
    Slot[] slots = new Slot[components.length];
    for (int i = 0; i < components.length; i++) {
      Class<?> type = components[i].getType();
      if (type.isRecord()) {
        Slot[] children = buildSlots(type.asSubclass(Record.class));
        int count = 0;
        for (Slot child : children) {
          count += child.columnCount();
        }
        slots[i] =
            new NestedSlot(children, canonicalConstructor(type.asSubclass(Record.class)), count);
      } else {
        slots[i] = new ScalarSlot(indexedExtractorFor(type, components[i].getName()));
      }
    }
    return slots;
  }

  private static <R extends Record> Constructor<R> canonicalConstructor(Class<R> recordType) {
    RecordComponent[] components = recordType.getRecordComponents();
    Class<?>[] paramTypes = new Class<?>[components.length];
    for (int i = 0; i < components.length; i++) {
      paramTypes[i] = components[i].getType();
    }
    try {
      Constructor<R> ctor = recordType.getDeclaredConstructor(paramTypes);
      ctor.setAccessible(true);
      return ctor;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Cannot find canonical constructor for " + recordType.getName(), e);
    }
  }

  private static Object[] extract(
      Slot[] slots, Constructor<? extends Record> ctor, Row row, int[] cursor) {
    Object[] args = new Object[slots.length];
    for (int i = 0; i < slots.length; i++) {
      switch (slots[i]) {
        case ScalarSlot s -> args[i] = s.extractor().apply(row, cursor[0]++);
        case NestedSlot n -> {
          Object[] childArgs = extract(n.children(), n.constructor(), row, cursor);
          try {
            args[i] = n.constructor().newInstance(childArgs);
          } catch (Exception e) {
            throw new IllegalStateException("Failed to construct nested record", e);
          }
        }
      }
    }
    return args;
  }

  private static BiFunction<Row, Integer, Object> indexedExtractorFor(
      Class<?> type, String componentName) {
    BiFunction<Row, Integer, Object> extractor = RowExtractors.extractorFor(type);
    if (extractor != null) {
      return extractor;
    }
    throw new IllegalArgumentException(
        "Unsupported type "
            + type.getName()
            + " for component '"
            + componentName
            + "'. "
            + "Supported: "
            + RowExtractors.SUPPORTED_TYPES_MESSAGE
            + ", and nested records");
  }
}
