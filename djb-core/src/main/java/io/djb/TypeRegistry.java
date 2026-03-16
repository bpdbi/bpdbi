package io.djb;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

import org.jspecify.annotations.Nullable;

/**
 * Registry of {@link TypeBinder}s for converting Java objects to SQL parameter strings.
 * Used by the connection when encoding parameters for queries.
 */
public final class TypeRegistry {

    private final Map<Class<?>, TypeBinder<?>> binders = new LinkedHashMap<>();
    private final Set<Class<?>> jsonTypes = new LinkedHashSet<>();

    public <T> TypeRegistry register(Class<T> type, TypeBinder<T> binder) {
        binders.put(type, binder);
        return this;
    }

    /**
     * Register a type as JSON. When binding parameters, values of this type will be
     * serialized via the connection's {@link JsonMapper} instead of {@link TypeBinder}.
     * When reading results, columns mapped to this type will be deserialized as JSON
     * even if the column is not a JSON/JSONB type in the database.
     */
    public TypeRegistry registerAsJson(Class<?> type) {
        jsonTypes.add(type);
        return this;
    }

    public Set<Class<?>> jsonTypes() {
        return Collections.unmodifiableSet(jsonTypes);
    }

    public boolean isJsonType(Class<?> type) {
        return jsonTypes.contains(type);
    }

    /**
     * Convert a value to its SQL string representation using the registered binder.
     * Returns null if the value is null.
     */
    @SuppressWarnings("unchecked")
    public @Nullable String bind(@Nullable Object value) {
        if (value == null) return null;
        Class<?> type = value.getClass();
        TypeBinder<Object> binder = (TypeBinder<Object>) binders.get(type);
        if (binder != null) {
            return binder.bind(value);
        }
        // Check supertypes
        for (var entry : binders.entrySet()) {
            if (entry.getKey().isAssignableFrom(type)) {
                return ((TypeBinder<Object>) entry.getValue()).bind(value);
            }
        }
        // Fallback to toString
        return value.toString();
    }

    /**
     * Create a registry with built-in binders for common types.
     */
    public static TypeRegistry defaults() {
        var reg = new TypeRegistry();
        reg.register(String.class, v -> v);
        reg.register(Integer.class, Object::toString);
        reg.register(Long.class, Object::toString);
        reg.register(Short.class, Object::toString);
        reg.register(Float.class, Object::toString);
        reg.register(Double.class, Object::toString);
        reg.register(Boolean.class, Object::toString);
        reg.register(BigDecimal.class, BigDecimal::toPlainString);
        reg.register(UUID.class, Object::toString);
        reg.register(LocalDate.class, Object::toString);
        reg.register(LocalTime.class, Object::toString);
        reg.register(LocalDateTime.class, Object::toString);
        reg.register(OffsetDateTime.class, Object::toString);
        reg.register(byte[].class, TypeRegistry::hexEncode);
        return reg;
    }

    static String hexEncode(byte[] bytes) {
        var sb = new StringBuilder("\\x");
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
