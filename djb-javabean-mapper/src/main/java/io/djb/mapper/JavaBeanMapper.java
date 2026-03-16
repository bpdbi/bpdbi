package io.djb.mapper;

import io.djb.Row;
import io.djb.RowMapper;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * A {@link RowMapper} that maps rows to JavaBeans (POJOs with a no-arg constructor
 * and setter methods) by matching column names to property names.
 *
 * <pre>{@code
 * public class User {
 *     private int id;
 *     private String name;
 *     public User() {}
 *     public int getId() { return id; }
 *     public void setId(int id) { this.id = id; }
 *     public String getName() { return name; }
 *     public void setName(String name) { this.name = name; }
 * }
 *
 * RowMapper<User> mapper = JavaBeanMapper.of(User.class);
 * List<User> users = conn.query("SELECT id, name FROM users").mapTo(mapper);
 * }</pre>
 */
public final class JavaBeanMapper<T> implements RowMapper<T> {

    private static final Map<Class<?>, BiFunction<Row, String, Object>> EXTRACTORS = Map.ofEntries(
        Map.entry(String.class,          Row::getString),
        Map.entry(int.class,             (r, c) -> { Integer v = r.getInteger(c); return v == null ? 0 : v; }),
        Map.entry(Integer.class,         Row::getInteger),
        Map.entry(long.class,            (r, c) -> { Long v = r.getLong(c); return v == null ? 0L : v; }),
        Map.entry(Long.class,            Row::getLong),
        Map.entry(short.class,           (r, c) -> { Short v = r.getShort(c); return v == null ? (short) 0 : v; }),
        Map.entry(Short.class,           Row::getShort),
        Map.entry(float.class,           (r, c) -> { Float v = r.getFloat(c); return v == null ? 0f : v; }),
        Map.entry(Float.class,           Row::getFloat),
        Map.entry(double.class,          (r, c) -> { Double v = r.getDouble(c); return v == null ? 0d : v; }),
        Map.entry(Double.class,          Row::getDouble),
        Map.entry(boolean.class,         (r, c) -> { Boolean v = r.getBoolean(c); return v == null ? false : v; }),
        Map.entry(Boolean.class,         Row::getBoolean),
        Map.entry(BigDecimal.class,      Row::getBigDecimal),
        Map.entry(UUID.class,            Row::getUUID),
        Map.entry(byte[].class,          Row::getBytes),
        Map.entry(LocalDate.class,       Row::getLocalDate),
        Map.entry(LocalTime.class,       Row::getLocalTime),
        Map.entry(LocalDateTime.class,   Row::getLocalDateTime),
        Map.entry(OffsetDateTime.class,  Row::getOffsetDateTime)
    );

    private final Class<T> beanType;
    private final Property[] properties;

    private JavaBeanMapper(Class<T> beanType) {
        try {
            beanType.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                beanType.getName() + " must have a no-arg constructor");
        }

        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(beanType, Object.class);
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException("Failed to introspect " + beanType.getName(), e);
        }

        var props = new ArrayList<Property>();
        for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
            Method setter = pd.getWriteMethod();
            if (setter == null) continue;
            Class<?> type = pd.getPropertyType();
            BiFunction<Row, String, Object> extractor = EXTRACTORS.get(type);
            if (extractor == null) {
                throw new IllegalArgumentException(
                    "Unsupported type " + type.getName() + " for property '" + pd.getName() + "'. " +
                    "Supported: String, int, long, short, float, double, boolean (and boxed), " +
                    "BigDecimal, UUID, byte[], LocalDate, LocalTime, LocalDateTime, OffsetDateTime");
            }
            setter.setAccessible(true);
            props.add(new Property(pd.getName(), setter, extractor));
        }

        this.beanType = beanType;
        this.properties = props.toArray(Property[]::new);
    }

    /**
     * Create a mapper for the given JavaBean type.
     */
    public static <T> JavaBeanMapper<T> of(Class<T> beanType) {
        return new JavaBeanMapper<>(beanType);
    }

    @Override
    public T map(Row row) {
        T instance;
        try {
            instance = beanType.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to construct " + beanType.getName(), e);
        }
        for (Property prop : properties) {
            Object value = prop.extractor.apply(row, prop.columnName);
            try {
                prop.setter.invoke(instance, value);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to set property '" + prop.columnName + "' on " + beanType.getName(), e);
            }
        }
        return instance;
    }

    private record Property(String columnName, Method setter, BiFunction<Row, String, Object> extractor) {}
}
