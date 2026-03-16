package io.djb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TypeRegistryTest {

    @Test
    void defaultBindString() {
        var reg = TypeRegistry.defaults();
        assertEquals("hello", reg.bind("hello"));
    }

    @Test
    void defaultBindInteger() {
        var reg = TypeRegistry.defaults();
        assertEquals("42", reg.bind(42));
    }

    @Test
    void defaultBindLong() {
        assertEquals("9999999999", TypeRegistry.defaults().bind(9999999999L));
    }

    @Test
    void defaultBindBigDecimal() {
        assertEquals("123.456", TypeRegistry.defaults().bind(new BigDecimal("123.456")));
    }

    @Test
    void defaultBindBoolean() {
        assertEquals("true", TypeRegistry.defaults().bind(true));
        assertEquals("false", TypeRegistry.defaults().bind(false));
    }

    @Test
    void defaultBindUUID() {
        var uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", TypeRegistry.defaults().bind(uuid));
    }

    @Test
    void defaultBindLocalDate() {
        assertEquals("2024-01-15", TypeRegistry.defaults().bind(LocalDate.of(2024, 1, 15)));
    }

    @Test
    void defaultBindByteArray() {
        var result = TypeRegistry.defaults().bind(new byte[]{(byte) 0xCA, (byte) 0xFE});
        assertEquals("\\xcafe", result);
    }

    @Test
    void bindNull() {
        assertNull(TypeRegistry.defaults().bind(null));
    }

    @Test
    void customBinder() {
        record Money(BigDecimal amount, String currency) {}
        var reg = TypeRegistry.defaults();
        reg.register(Money.class, m -> m.amount().toPlainString());
        assertEquals("9.99", reg.bind(new Money(new BigDecimal("9.99"), "USD")));
    }

    @Test
    void unknownTypeFallsBackToString() {
        record Custom(int x) {
            @Override public String toString() { return "custom:" + x; }
        }
        assertEquals("custom:42", TypeRegistry.defaults().bind(new Custom(42)));
    }
}
