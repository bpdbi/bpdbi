package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ColumnMapperRegistryTest {

  @Test
  void defaultMapString() {
    assertEquals("hello", ColumnMapperRegistry.defaults().map(String.class, "hello", "col"));
  }

  @Test
  void defaultMapInteger() {
    assertEquals(42, ColumnMapperRegistry.defaults().map(Integer.class, "42", "col"));
  }

  @Test
  void defaultMapLong() {
    assertEquals(9999999999L, ColumnMapperRegistry.defaults().map(Long.class, "9999999999", "col"));
  }

  @Test
  void defaultMapDouble() {
    assertEquals(3.14, ColumnMapperRegistry.defaults().map(Double.class, "3.14", "col"), 0.001);
  }

  @Test
  void defaultMapBoolean() {
    assertTrue(ColumnMapperRegistry.defaults().map(Boolean.class, "t", "col"));
    assertTrue(ColumnMapperRegistry.defaults().map(Boolean.class, "true", "col"));
    assertFalse(ColumnMapperRegistry.defaults().map(Boolean.class, "f", "col"));
  }

  @Test
  void defaultMapBigDecimal() {
    assertEquals(
        new BigDecimal("123.456"),
        ColumnMapperRegistry.defaults().map(BigDecimal.class, "123.456", "col"));
  }

  @Test
  void defaultMapUUID() {
    var uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assertEquals(
        uuid,
        ColumnMapperRegistry.defaults()
            .map(UUID.class, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "col"));
  }

  @Test
  void defaultMapLocalDate() {
    assertEquals(
        LocalDate.of(2024, 1, 15),
        ColumnMapperRegistry.defaults().map(LocalDate.class, "2024-01-15", "col"));
  }

  @Test
  void customMapper() {
    record Money(BigDecimal amount) {}

    var reg = ColumnMapperRegistry.defaults();
    reg.register(Money.class, (v, c) -> new Money(new BigDecimal(v)));
    var money = reg.map(Money.class, "9.99", "price");
    assertEquals(new BigDecimal("9.99"), money.amount());
  }

  @Test
  void unknownTypeThrows() {
    record Unknown() {}
    assertThrows(
        IllegalArgumentException.class,
        () -> ColumnMapperRegistry.defaults().map(Unknown.class, "x", "col"));
  }

  enum Status {
    ACTIVE,
    INACTIVE
  }

  @Test
  void mapsEnumByName() {
    assertEquals(
        Status.ACTIVE, ColumnMapperRegistry.defaults().map(Status.class, "ACTIVE", "status"));
    assertEquals(
        Status.INACTIVE, ColumnMapperRegistry.defaults().map(Status.class, "INACTIVE", "status"));
  }

  @Test
  void enumUnknownValueThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ColumnMapperRegistry.defaults().map(Status.class, "UNKNOWN", "status"));
  }

  @Test
  void hasMapper() {
    var reg = ColumnMapperRegistry.defaults();
    assertTrue(reg.hasMapper(String.class));
    assertTrue(reg.hasMapper(Integer.class));
    assertFalse(reg.hasMapper(Object.class));
    assertTrue(reg.hasMapper(Status.class));
  }
}
