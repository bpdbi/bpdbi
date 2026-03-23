package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
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

  @Retention(RetentionPolicy.RUNTIME)
  @interface NVarchar {}

  @Retention(RetentionPolicy.RUNTIME)
  @interface Encrypted {}

  @Test
  void qualifiedTypeBinderOverridesDefault() {
    var reg = BinderRegistry.defaults();
    var nvarchar = QualifiedType.of(String.class, NVarchar.class);
    reg.register(nvarchar, v -> "N'" + v + "'");

    // Qualified binding uses the qualified binder
    assertEquals("N'hello'", reg.bind(nvarchar, "hello"));
    // Unqualified binding uses the default String binder
    assertEquals("hello", reg.bind("hello"));
  }

  @Test
  void qualifiedTypeBinderFallsBackToUnqualified() {
    var reg = BinderRegistry.defaults();
    var encrypted = QualifiedType.of(String.class, Encrypted.class);

    // No qualified binder registered — falls back to default String binder
    assertEquals("plain", reg.bind(encrypted, "plain"));
  }

  @Test
  void qualifiedTypeBinderHandlesNull() {
    var reg = BinderRegistry.defaults();
    var nvarchar = QualifiedType.of(String.class, NVarchar.class);
    reg.register(nvarchar, v -> "N'" + v + "'");

    assertNull(reg.bind(nvarchar, null));
  }

  // --- ParamEncoder tests ---

  @Test
  void encoderConvertsType() {
    record UserId(UUID uuid) {}

    var reg = BinderRegistry.defaults();
    reg.registerEncoder(UserId.class, id -> id.uuid());

    var id = new UserId(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    Object encoded = reg.encode(id);
    assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), encoded);
  }

  @Test
  void encoderReturnsOriginalWhenNoEncoderRegistered() {
    var reg = BinderRegistry.defaults();
    assertEquals(42, reg.encode(42));
    assertEquals("hello", reg.encode("hello"));
  }

  @Test
  void encoderReturnsNullForNull() {
    var reg = BinderRegistry.defaults();
    assertNull(reg.encode(null));
  }

  @Test
  void encoderMatchesSupertype() {
    var reg = BinderRegistry.defaults();
    reg.registerEncoder(Number.class, n -> n.longValue());

    // Integer extends Number — should match
    assertEquals(42L, reg.encode(42));
    assertEquals(100L, reg.encode(100L));
  }

  @Test
  void hasEncodersFalseByDefault() {
    assertFalse(BinderRegistry.defaults().hasEncoders());
  }

  @Test
  void hasEncodersTrueAfterRegister() {
    var reg = BinderRegistry.defaults();
    reg.registerEncoder(StringBuilder.class, sb -> sb.toString());
    assertTrue(reg.hasEncoders());
  }

  @Test
  void qualifiedTypeEquality() {
    var qt1 = QualifiedType.of(String.class, NVarchar.class);
    var qt2 = QualifiedType.of(String.class, NVarchar.class);
    var qt3 = QualifiedType.of(String.class, Encrypted.class);

    assertEquals(qt1, qt2);
    assertNotEquals(qt1, qt3);
    assertEquals(qt1.hashCode(), qt2.hashCode());
  }
}
