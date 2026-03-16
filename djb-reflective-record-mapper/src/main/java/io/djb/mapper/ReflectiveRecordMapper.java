package io.djb.mapper;

import io.djb.Row;
import io.djb.RowMapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * A {@link RowMapper} that maps rows to Java records by matching column names
 * to record component names.
 *
 * <p>Column names must match record component names exactly, unless overridden
 * with {@link ColumnName @ColumnName}.
 *
 * <pre>{@code
 * record User(int id, String name, String email) {}
 *
 * RowMapper<User> mapper = ReflectiveRecordMapper.of(User.class);
 * List<User> users = conn.query("SELECT id, name, email FROM users").mapTo(mapper);
 * }</pre>
 */
public final class ReflectiveRecordMapper<T extends Record> implements RowMapper<T> {

    private final Constructor<T> constructor;
    private final Accessor[] accessors;

    private ReflectiveRecordMapper(Class<T> recordType) {
        if (!recordType.isRecord()) {
            throw new IllegalArgumentException(recordType.getName() + " is not a record type");
        }

        RecordComponent[] components = recordType.getRecordComponents();
        this.accessors = new Accessor[components.length];

        Class<?>[] paramTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            paramTypes[i] = components[i].getType();
            ColumnName annotation = components[i].getAnnotation(ColumnName.class);
            String colName = annotation != null ? annotation.value() : components[i].getName();
            accessors[i] = new Accessor(
                colName,
                extractorFor(components[i].getType(), components[i].getName())
            );
        }

        try {
            this.constructor = recordType.getDeclaredConstructor(paramTypes);
            this.constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Cannot find canonical constructor for " + recordType.getName(), e);
        }
    }

    /**
     * Create a mapper for the given record type.
     */
    public static <T extends Record> ReflectiveRecordMapper<T> of(Class<T> recordType) {
        return new ReflectiveRecordMapper<>(recordType);
    }

    @Override
    public T map(Row row) {
        Object[] args = new Object[accessors.length];
        for (int i = 0; i < accessors.length; i++) {
            args[i] = accessors[i].extract(row);
        }
        try {
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to construct record", e);
        }
    }

    private static BiFunction<Row, String, Object> extractorFor(Class<?> type, String componentName) {
        if (type == String.class)           return Row::getString;
        if (type == int.class)              return (r, c) -> { Integer v = r.getInteger(c); return v == null ? 0 : v; };
        if (type == Integer.class)          return Row::getInteger;
        if (type == long.class)             return (r, c) -> { Long v = r.getLong(c); return v == null ? 0L : v; };
        if (type == Long.class)             return Row::getLong;
        if (type == short.class)            return (r, c) -> { Short v = r.getShort(c); return v == null ? (short) 0 : v; };
        if (type == Short.class)            return Row::getShort;
        if (type == float.class)            return (r, c) -> { Float v = r.getFloat(c); return v == null ? 0f : v; };
        if (type == Float.class)            return Row::getFloat;
        if (type == double.class)           return (r, c) -> { Double v = r.getDouble(c); return v == null ? 0d : v; };
        if (type == Double.class)           return Row::getDouble;
        if (type == boolean.class)          return (r, c) -> { Boolean v = r.getBoolean(c); return v == null ? false : v; };
        if (type == Boolean.class)          return Row::getBoolean;
        if (type == BigDecimal.class)       return Row::getBigDecimal;
        if (type == UUID.class)             return Row::getUUID;
        if (type == byte[].class)           return Row::getBytes;
        if (type == LocalDate.class)        return Row::getLocalDate;
        if (type == LocalTime.class)        return Row::getLocalTime;
        if (type == LocalDateTime.class)    return Row::getLocalDateTime;
        if (type == OffsetDateTime.class)   return Row::getOffsetDateTime;
        throw new IllegalArgumentException(
            "Unsupported type " + type.getName() + " for component '" + componentName + "'. " +
            "Supported: String, int, long, short, float, double, boolean (and boxed), " +
            "BigDecimal, UUID, byte[], LocalDate, LocalTime, LocalDateTime, OffsetDateTime");
    }

    private record Accessor(String columnName, BiFunction<Row, String, Object> extractor) {
        Object extract(Row row) {
            return extractor.apply(row, columnName);
        }
    }
}
