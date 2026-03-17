package io.djb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

class BinderRegistryTest {

  @Test
  void defaultBindString() {
    var reg = BinderRegistry.defaults();
    assertEquals("hello", reg.bind("hello"));
  }

  @Test
  void defaultBindInteger() {
    var reg = BinderRegistry.defaults();
    assertEquals("42", reg.bind(42));
  }

  @Test
  void defaultBindLong() {
    assertEquals("9999999999", BinderRegistry.defaults().bind(9999999999L));
  }

  @Test
  void defaultBindBigDecimal() {
    assertEquals("123.456", BinderRegistry.defaults().bind(new BigDecimal("123.456")));
  }

  @Test
  void defaultBindBoolean() {
    assertEquals("true", BinderRegistry.defaults().bind(true));
    assertEquals("false", BinderRegistry.defaults().bind(false));
  }

  @Test
  void defaultBindUUID() {
    var uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", BinderRegistry.defaults().bind(uuid));
  }

  @Test
  void defaultBindLocalDate() {
    assertEquals("2024-01-15", BinderRegistry.defaults().bind(LocalDate.of(2024, 1, 15)));
  }

  @Test
  void defaultBindByteArray() {
    var result = BinderRegistry.defaults().bind(new byte[] {(byte) 0xCA, (byte) 0xFE});
    assertEquals("\\xcafe", result);
  }

  @Test
  void bindNull() {
    assertNull(BinderRegistry.defaults().bind(null));
  }

  @Test
  void customBinder() {
    record Money(BigDecimal amount, String currency) {}

    var reg = BinderRegistry.defaults();
    reg.register(Money.class, m -> m.amount().toPlainString());
    assertEquals("9.99", reg.bind(new Money(new BigDecimal("9.99"), "USD")));
  }

  @Test
  void unknownTypeFallsBackToString() {
    record Custom(int x) {

      @Override
      public @NonNull String toString() {
        return "custom:" + x;
      }
    }
    assertEquals("custom:42", BinderRegistry.defaults().bind(new Custom(42)));
  }

  @Test
  void registerAsJsonAddsToJsonTypes() {
    record OrderMeta(String note) {}
    var reg = BinderRegistry.defaults();
    assertFalse(reg.isJsonType(OrderMeta.class));
    assertTrue(reg.jsonTypes().isEmpty());

    reg.registerAsJson(OrderMeta.class);

    assertTrue(reg.isJsonType(OrderMeta.class));
    assertTrue(reg.jsonTypes().contains(OrderMeta.class));
    assertFalse(reg.isJsonType(String.class));
  }

  @Test
  void registerAsJsonReturnsRegistryForChaining() {
    var reg = BinderRegistry.defaults();
    var result = reg.registerAsJson(String.class);
    assertEquals(reg, result);
  }
}
