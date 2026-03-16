package io.djb;

import java.math.BigDecimal;
import java.time.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Registry of {@link ColumnMapper}s for converting database text values to Java types.
 * Used by {@link Row#get(int, Class)} and {@link Row#get(String, Class)}.
 */
public final class MapperRegistry {

    private final Map<Class<?>, ColumnMapper<?>> mappers = new LinkedHashMap<>();

    public <T> MapperRegistry register(Class<T> type, ColumnMapper<T> mapper) {
        mappers.put(type, mapper);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T map(Class<T> type, String value, String columnName) {
        ColumnMapper<T> mapper = (ColumnMapper<T>) mappers.get(type);
        if (mapper != null) {
            return mapper.map(value, columnName);
        }
        // Check supertypes
        for (var entry : mappers.entrySet()) {
            if (entry.getKey().isAssignableFrom(type)) {
                return ((ColumnMapper<T>) entry.getValue()).map(value, columnName);
            }
        }
        throw new IllegalArgumentException(
            "No ColumnMapper registered for type " + type.getName()
            + " (column: " + columnName + "). Register one via MapperRegistry.register().");
    }

    public boolean hasMapper(Class<?> type) {
        if (mappers.containsKey(type)) return true;
        for (var key : mappers.keySet()) {
            if (key.isAssignableFrom(type)) return true;
        }
        return false;
    }

    /**
     * Create a registry with built-in mappers for common types.
     */
    public static MapperRegistry defaults() {
        var reg = new MapperRegistry();
        reg.register(String.class, (v, c) -> v);
        reg.register(Integer.class, (v, c) -> Integer.parseInt(v));
        reg.register(Long.class, (v, c) -> Long.parseLong(v));
        reg.register(Short.class, (v, c) -> Short.parseShort(v));
        reg.register(Float.class, (v, c) -> Float.parseFloat(v));
        reg.register(Double.class, (v, c) -> Double.parseDouble(v));
        reg.register(Boolean.class, (v, c) -> "t".equals(v) || "true".equalsIgnoreCase(v) || "1".equals(v));
        reg.register(BigDecimal.class, (v, c) -> new BigDecimal(v));
        reg.register(UUID.class, (v, c) -> UUID.fromString(v));
        reg.register(LocalDate.class, (v, c) -> LocalDate.parse(v));
        reg.register(LocalTime.class, (v, c) -> LocalTime.parse(v));
        reg.register(LocalDateTime.class, (v, c) -> LocalDateTime.parse(v.replace(' ', 'T')));
        reg.register(OffsetDateTime.class, (v, c) -> OffsetDateTime.parse(v.replace(' ', 'T')));
        return reg;
    }
}
