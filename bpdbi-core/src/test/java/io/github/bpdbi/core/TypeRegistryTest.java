package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.test.StubBinaryCodec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TypeRegistryTest {

  private static final BinaryCodec CODEC = StubBinaryCodec.INSTANCE;

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  // --- Full codec (encode + decode) ---

  record Money(BigDecimal amount) {}

  @Test
  void fullCodecEncode() {
    var reg = new TypeRegistry();
    reg.register(Money.class, BigDecimal.class, Money::amount, Money::new);

    var encoded = reg.encode(new Money(new BigDecimal("9.99")));
    assertEquals(new BigDecimal("9.99"), encoded);
  }

  @Test
  void fullCodecDecode() {
    var reg = new TypeRegistry();
    reg.register(Money.class, BigDecimal.class, Money::amount, Money::new);

    byte[] buf = utf8("9.99");
    Money result = reg.decode(Money.class, CODEC, buf, 0, buf.length);
    assertEquals(new Money(new BigDecimal("9.99")), result);
  }

  // --- Encode-only ---

  record WriteOnly(String text) {}

  @Test
  void encodeOnlyEncode() {
    var reg = new TypeRegistry();
    reg.register(WriteOnly.class, String.class, WriteOnly::text, null);

    assertEquals("hello", reg.encode(new WriteOnly("hello")));
  }

  @Test
  void encodeOnlyDecodeThrows() {
    var reg = new TypeRegistry();
    reg.register(WriteOnly.class, String.class, WriteOnly::text, null);

    byte[] buf = utf8("hello");
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> reg.decode(WriteOnly.class, CODEC, buf, 0, buf.length));
    assertTrue(ex.getMessage().contains("encode-only"));
    assertTrue(ex.getMessage().contains("WriteOnly"));
  }

  // --- Decode-only ---

  record ReadOnly(String value) {}

  @Test
  void decodeOnlyDecode() {
    var reg = new TypeRegistry();
    reg.register(ReadOnly.class, String.class, null, ReadOnly::new);

    byte[] buf = utf8("hello");
    ReadOnly result = reg.decode(ReadOnly.class, CODEC, buf, 0, buf.length);
    assertEquals(new ReadOnly("hello"), result);
  }

  @Test
  void decodeOnlyEncodePassesThrough() {
    var reg = new TypeRegistry();
    reg.register(ReadOnly.class, String.class, null, ReadOnly::new);

    var value = new ReadOnly("hello");
    // No encoder registered, so encode returns the original value unchanged
    assertEquals(value, reg.encode(value));
  }

  // --- Unregistered type ---

  @Test
  void decodeUnregisteredTypeThrows() {
    var reg = new TypeRegistry();

    byte[] buf = utf8("x");
    var ex =
        assertThrows(
            IllegalStateException.class, () -> reg.decode(Money.class, CODEC, buf, 0, buf.length));
    assertTrue(ex.getMessage().contains("No TypeRegistry entry"));
    assertTrue(ex.getMessage().contains("Money"));
  }

  @Test
  void encodeUnregisteredTypePassesThrough() {
    var reg = new TypeRegistry();
    assertEquals("hello", reg.encode("hello"));
    assertEquals(42, reg.encode(42));
  }

  @Test
  void encodeNullReturnsNull() {
    var reg = new TypeRegistry();
    assertNull(reg.encode(null));
  }

  // --- canDecode ---

  @Test
  void canDecodeWithDecoder() {
    var reg = new TypeRegistry();
    reg.register(Money.class, BigDecimal.class, Money::amount, Money::new);
    assertTrue(reg.canDecode(Money.class));
  }

  @Test
  void canDecodeWithoutDecoder() {
    var reg = new TypeRegistry();
    reg.register(WriteOnly.class, String.class, WriteOnly::text, null);
    assertFalse(reg.canDecode(WriteOnly.class));
  }

  @Test
  void canDecodeUnregistered() {
    var reg = new TypeRegistry();
    assertFalse(reg.canDecode(Money.class));
  }

  // --- hasEncoders ---

  @Test
  void hasEncodersEmpty() {
    var reg = new TypeRegistry();
    assertFalse(reg.hasEncoders());
  }

  @Test
  void hasEncodersWithEncoder() {
    var reg = new TypeRegistry();
    reg.register(Money.class, BigDecimal.class, Money::amount, Money::new);
    assertTrue(reg.hasEncoders());
  }

  @Test
  void hasEncodersDecodeOnly() {
    var reg = new TypeRegistry();
    reg.register(ReadOnly.class, String.class, null, ReadOnly::new);
    assertFalse(reg.hasEncoders());
  }

  // --- JSON types ---

  @Test
  void registerAsJson() {
    var reg = new TypeRegistry();
    reg.registerAsJson(Money.class);
    assertTrue(reg.isJsonType(Money.class));
    assertFalse(reg.isJsonType(String.class));
    assertEquals(1, reg.jsonTypes().size());
  }

  // --- Supertype matching ---

  interface Animal {}

  record Dog(String name) implements Animal {}

  @Test
  void supertypeMatchEncode() {
    var reg = new TypeRegistry();
    reg.register(Animal.class, String.class, a -> a.toString(), null);

    // Dog implements Animal, so the encoder should match
    var dog = new Dog("Rex");
    assertEquals(dog.toString(), reg.encode(dog));
  }

  @Test
  void supertypeMatchCanDecode() {
    var reg = new TypeRegistry();
    reg.register(Animal.class, String.class, null, s -> new Dog(s));

    // Exact match fails, supertype match succeeds
    assertTrue(reg.canDecode(Animal.class));
  }

  // --- UUID standard type ---

  record UserId(UUID uuid) {}

  @Test
  void uuidStandardType() {
    var reg = new TypeRegistry();
    reg.register(UserId.class, UUID.class, UserId::uuid, UserId::new);

    var uid = new UserId(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
    assertEquals(uid.uuid(), reg.encode(uid));

    byte[] buf = utf8("550e8400-e29b-41d4-a716-446655440000");
    UserId decoded = reg.decode(UserId.class, CODEC, buf, 0, buf.length);
    assertEquals(uid, decoded);
  }

  // --- Chaining ---

  @Test
  void registerReturnsRegistryForChaining() {
    var reg =
        new TypeRegistry()
            .register(Money.class, BigDecimal.class, Money::amount, Money::new)
            .register(UserId.class, UUID.class, UserId::uuid, UserId::new)
            .registerAsJson(String.class);

    assertTrue(reg.canDecode(Money.class));
    assertTrue(reg.canDecode(UserId.class));
    assertTrue(reg.isJsonType(String.class));
  }

  // --- Offset decode ---

  @Test
  void decodeWithOffset() {
    var reg = new TypeRegistry();
    reg.register(Money.class, BigDecimal.class, Money::amount, Money::new);

    // "XX9.99" with offset=2, len=4
    byte[] buf = utf8("XX9.99");
    Money result = reg.decode(Money.class, CODEC, buf, 2, 4);
    assertEquals(new Money(new BigDecimal("9.99")), result);
  }
}
