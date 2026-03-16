package io.djb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MapperRegistryTest {

    @Test
    void defaultMapString() {
        assertEquals("hello", MapperRegistry.defaults().map(String.class, "hello", "col"));
    }

    @Test
    void defaultMapInteger() {
        assertEquals(42, MapperRegistry.defaults().map(Integer.class, "42", "col"));
    }

    @Test
    void defaultMapLong() {
        assertEquals(9999999999L, MapperRegistry.defaults().map(Long.class, "9999999999", "col"));
    }

    @Test
    void defaultMapDouble() {
        assertEquals(3.14, MapperRegistry.defaults().map(Double.class, "3.14", "col"), 0.001);
    }

    @Test
    void defaultMapBoolean() {
        assertTrue(MapperRegistry.defaults().map(Boolean.class, "t", "col"));
        assertTrue(MapperRegistry.defaults().map(Boolean.class, "true", "col"));
        assertFalse(MapperRegistry.defaults().map(Boolean.class, "f", "col"));
    }

    @Test
    void defaultMapBigDecimal() {
        assertEquals(new BigDecimal("123.456"),
            MapperRegistry.defaults().map(BigDecimal.class, "123.456", "col"));
    }

    @Test
    void defaultMapUUID() {
        var uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertEquals(uuid, MapperRegistry.defaults().map(UUID.class,
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "col"));
    }

    @Test
    void defaultMapLocalDate() {
        assertEquals(LocalDate.of(2024, 1, 15),
            MapperRegistry.defaults().map(LocalDate.class, "2024-01-15", "col"));
    }

    @Test
    void customMapper() {
        record Money(BigDecimal amount) {}
        var reg = MapperRegistry.defaults();
        reg.register(Money.class, (v, c) -> new Money(new BigDecimal(v)));
        var money = reg.map(Money.class, "9.99", "price");
        assertEquals(new BigDecimal("9.99"), money.amount());
    }

    @Test
    void unknownTypeThrows() {
        record Unknown() {}
        assertThrows(IllegalArgumentException.class,
            () -> MapperRegistry.defaults().map(Unknown.class, "x", "col"));
    }

    @Test
    void hasMapper() {
        var reg = MapperRegistry.defaults();
        assertTrue(reg.hasMapper(String.class));
        assertTrue(reg.hasMapper(Integer.class));
        assertFalse(reg.hasMapper(Object.class));
    }
}
