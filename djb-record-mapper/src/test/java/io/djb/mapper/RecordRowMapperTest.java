package io.djb.mapper;

import static io.djb.test.TestRows.row;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.djb.Row;
import io.djb.RowMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecordRowMapperTest {

  // --- Test records ---

  record SimpleUser(Integer id, String name, String email) {}

  record PrimitiveUser(int id, String name) {}

  record AllTypes(
      String s,
      Integer i,
      Long l,
      Short sh,
      Float f,
      Double d,
      Boolean b,
      BigDecimal bd,
      UUID uuid) {}

  record DateTimeRecord(LocalDate date, LocalTime time, LocalDateTime dateTime) {}

  record PrimitivesRecord(int i, long l, short s, float f, double d, boolean b) {}

  record SingleField(String value) {}

  record BytesRecord(String name, byte[] data) {}

  record SnakeCaseUser(int id, String firstName, String lastName) {}

  record MixedAnnotation(int id, String displayName, String email) {}

  record WithOffsetDateTime(String name, OffsetDateTime created) {}

  record MixedNulls(int id, Integer nullableId, String name, Long count) {}

  record EmptyStringsRecord(String first, String second) {}

  enum Status {
    ACTIVE,
    INACTIVE
  }

  record OrderWithStatus(int id, Status status) {}

  // --- Tests ---

  @Test
  void mapsSimpleRecord() {
    RowMapper<SimpleUser> mapper = RecordRowMapper.of(SimpleUser.class);
    Row r =
        row(
            new String[] {"id", "name", "email"},
            new String[] {"42", "Alice", "alice@example.com"});

    SimpleUser user = mapper.map(r);

    assertEquals(42, user.id());
    assertEquals("Alice", user.name());
    assertEquals("alice@example.com", user.email());
  }

  @Test
  void mapsPrimitiveInt() {
    RowMapper<PrimitiveUser> mapper = RecordRowMapper.of(PrimitiveUser.class);
    Row r = row(new String[] {"id", "name"}, new String[] {"7", "Bob"});

    PrimitiveUser user = mapper.map(r);

    assertEquals(7, user.id());
    assertEquals("Bob", user.name());
  }

  @Test
  void primitiveDefaultsToZeroOnNull() {
    RowMapper<PrimitivesRecord> mapper = RecordRowMapper.of(PrimitivesRecord.class);
    Row r =
        row(
            new String[] {"i", "l", "s", "f", "d", "b"},
            new String[] {null, null, null, null, null, null});

    PrimitivesRecord rec = mapper.map(r);

    assertEquals(0, rec.i());
    assertEquals(0L, rec.l());
    assertEquals((short) 0, rec.s());
    assertEquals(0f, rec.f());
    assertEquals(0d, rec.d());
    assertFalse(rec.b());
  }

  @Test
  void boxedTypesReturnNullOnNull() {
    RowMapper<SimpleUser> mapper = RecordRowMapper.of(SimpleUser.class);
    Row r = row(new String[] {"id", "name", "email"}, new String[] {null, null, null});

    SimpleUser user = mapper.map(r);

    assertNull(user.id());
    assertNull(user.name());
    assertNull(user.email());
  }

  @Test
  void mapsNumericAndUuidTypes() {
    UUID testUuid = UUID.randomUUID();
    RowMapper<AllTypes> mapper = RecordRowMapper.of(AllTypes.class);
    Row r =
        row(
            new String[] {"s", "i", "l", "sh", "f", "d", "b", "bd", "uuid"},
            new String[] {
              "hello", "1", "2", "3", "1.5", "2.5", "true", "99.99", testUuid.toString()
            });

    AllTypes rec = mapper.map(r);

    assertEquals("hello", rec.s());
    assertEquals(1, rec.i());
    assertEquals(2L, rec.l());
    assertEquals((short) 3, rec.sh());
    assertEquals(1.5f, rec.f());
    assertEquals(2.5, rec.d());
    assertTrue(rec.b());
    assertEquals(new BigDecimal("99.99"), rec.bd());
    assertEquals(testUuid, rec.uuid());
  }

  @Test
  void mapsDateTimeTypes() {
    RowMapper<DateTimeRecord> mapper = RecordRowMapper.of(DateTimeRecord.class);
    Row r =
        row(
            new String[] {"date", "time", "dateTime"},
            new String[] {"2024-06-15", "14:30:00", "2024-06-15T14:30:00"});

    DateTimeRecord rec = mapper.map(r);

    assertEquals(LocalDate.of(2024, 6, 15), rec.date());
    assertEquals(LocalTime.of(14, 30, 0), rec.time());
    assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), rec.dateTime());
  }

  @Test
  void mapsSingleFieldRecord() {
    RowMapper<SingleField> mapper = RecordRowMapper.of(SingleField.class);
    Row r = row(new String[] {"value"}, new String[] {"test"});

    assertEquals("test", mapper.map(r).value());
  }

  @Test
  void mapsByteArray() {
    RowMapper<BytesRecord> mapper = RecordRowMapper.of(BytesRecord.class);
    Row r = row(new String[] {"name", "data"}, new String[] {"file.bin", "rawbytes"});

    BytesRecord rec = mapper.map(r);
    assertEquals("file.bin", rec.name());
    assertArrayEquals("rawbytes".getBytes(StandardCharsets.UTF_8), rec.data());
  }

  @Test
  void reuseMapperAcrossMultipleRows() {
    RowMapper<SimpleUser> mapper = RecordRowMapper.of(SimpleUser.class);

    SimpleUser u1 =
        mapper.map(row(new String[] {"id", "name", "email"}, new String[] {"1", "A", "a@x"}));
    SimpleUser u2 =
        mapper.map(row(new String[] {"id", "name", "email"}, new String[] {"2", "B", "b@x"}));

    assertEquals(1, u1.id());
    assertEquals("A", u1.name());
    assertEquals(2, u2.id());
    assertEquals("B", u2.name());
  }

  @Test
  void mapsColumnsByPosition() {
    RowMapper<SnakeCaseUser> mapper = RecordRowMapper.of(SnakeCaseUser.class);
    // Column names don't matter — only position
    Row r =
        row(
            new String[] {"user_id", "first_name", "last_name"},
            new String[] {"1", "Alice", "Smith"});

    SnakeCaseUser user = mapper.map(r);

    assertEquals(1, user.id());
    assertEquals("Alice", user.firstName());
    assertEquals("Smith", user.lastName());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void rejectsNonRecordClass() {
    assertThrows(IllegalArgumentException.class, () -> RecordRowMapper.of((Class) String.class));
  }

  @Test
  void rejectsUnsupportedComponentType() {
    record BadRecord(Object unsupported) {}

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RecordRowMapper.of(BadRecord.class));
    assertTrue(ex.getMessage().contains("Unsupported type"));
    assertTrue(ex.getMessage().contains("unsupported"));
  }

  @Test
  void throwsWhenRowHasFewerColumnsThanRecord() {
    RowMapper<SimpleUser> mapper = RecordRowMapper.of(SimpleUser.class);
    Row r = row(new String[] {"id", "name"}, new String[] {"1", "Alice"});

    // Record expects 3 columns but row only has 2
    assertThrows(IndexOutOfBoundsException.class, () -> mapper.map(r));
  }

  @Test
  void mapsOffsetDateTime() {
    RowMapper<WithOffsetDateTime> mapper = RecordRowMapper.of(WithOffsetDateTime.class);
    Row r =
        row(new String[] {"name", "created"}, new String[] {"event", "2024-06-15T14:30:00+02:00"});

    WithOffsetDateTime rec = mapper.map(r);

    assertEquals("event", rec.name());
    assertEquals(
        OffsetDateTime.of(2024, 6, 15, 14, 30, 0, 0, ZoneOffset.ofHours(2)), rec.created());
  }

  @Test
  void mixedPrimitiveAndBoxedNulls() {
    RowMapper<MixedNulls> mapper = RecordRowMapper.of(MixedNulls.class);
    Row r =
        row(
            new String[] {"id", "nullable_id", "name", "count"},
            new String[] {null, null, "Alice", null});

    MixedNulls rec = mapper.map(r);

    // primitive int defaults to 0 on null
    assertEquals(0, rec.id());
    // boxed Integer returns null
    assertNull(rec.nullableId());
    assertEquals("Alice", rec.name());
    // boxed Long returns null
    assertNull(rec.count());
  }

  @Test
  void distinguishesEmptyStringFromNull() {
    RowMapper<EmptyStringsRecord> mapper = RecordRowMapper.of(EmptyStringsRecord.class);
    Row r = row(new String[] {"first", "second"}, new String[] {"", null});

    EmptyStringsRecord rec = mapper.map(r);

    assertEquals("", rec.first());
    assertNull(rec.second());
  }

  @Test
  void mapsRowWithMoreColumnsThanRecordComponents() {
    // Extra trailing columns should be ignored (only first N consumed)
    RowMapper<PrimitiveUser> mapper = RecordRowMapper.of(PrimitiveUser.class);
    Row r =
        row(
            new String[] {"id", "name", "extra1", "extra2"},
            new String[] {"5", "Charlie", "ignored", "also_ignored"});

    PrimitiveUser user = mapper.map(r);

    assertEquals(5, user.id());
    assertEquals("Charlie", user.name());
  }

  @Test
  void mapsEnumField() {
    RowMapper<OrderWithStatus> mapper = RecordRowMapper.of(OrderWithStatus.class);
    Row r = row(new String[] {"id", "status"}, new String[] {"1", "ACTIVE"});

    OrderWithStatus order = mapper.map(r);

    assertEquals(1, order.id());
    assertEquals(Status.ACTIVE, order.status());
  }

  @Test
  void nullEnumFieldReturnsNull() {
    RowMapper<OrderWithStatus> mapper = RecordRowMapper.of(OrderWithStatus.class);
    Row r = row(new String[] {"id", "status"}, new String[] {"1", null});

    OrderWithStatus order = mapper.map(r);

    assertEquals(1, order.id());
    assertNull(order.status());
  }

  @Test
  void mapsAllNullBigDecimalAndUuid() {
    RowMapper<AllTypes> mapper = RecordRowMapper.of(AllTypes.class);
    Row r =
        row(
            new String[] {"s", "i", "l", "sh", "f", "d", "b", "bd", "uuid"},
            new String[] {null, null, null, null, null, null, null, null, null});

    AllTypes rec = mapper.map(r);

    assertNull(rec.s());
    assertNull(rec.i());
    assertNull(rec.l());
    assertNull(rec.sh());
    assertNull(rec.f());
    assertNull(rec.d());
    assertNull(rec.b());
    assertNull(rec.bd());
    assertNull(rec.uuid());
  }

  // --- 2. Comprehensive null matrix ---

  record NullableTypes(
      Integer i,
      Long l,
      Short s,
      Float f,
      Double d,
      Boolean b,
      BigDecimal bd,
      UUID uuid,
      LocalDate date,
      LocalTime time,
      LocalDateTime dateTime,
      OffsetDateTime odt) {}

  @Test
  void allBoxedTypesReturnNullIndependently() {
    RowMapper<NullableTypes> mapper = RecordRowMapper.of(NullableTypes.class);
    Row r =
        row(
            new String[] {
              "i", "l", "s", "f", "d", "b", "bd", "uuid", "date", "time", "dateTime", "odt"
            },
            new String[] {null, null, null, null, null, null, null, null, null, null, null, null});

    NullableTypes rec = mapper.map(r);

    assertNull(rec.i());
    assertNull(rec.l());
    assertNull(rec.s());
    assertNull(rec.f());
    assertNull(rec.d());
    assertNull(rec.b());
    assertNull(rec.bd());
    assertNull(rec.uuid());
    assertNull(rec.date());
    assertNull(rec.time());
    assertNull(rec.dateTime());
    assertNull(rec.odt());
  }

  // --- 3. Error messages with column context ---

  @Test
  void errorMessageIncludesFieldNameForUnsupportedType() {
    record UnsupportedField(String name, Object badField) {}

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> RecordRowMapper.of(UnsupportedField.class));
    assertTrue(
        ex.getMessage().contains("badField"),
        "Error message should include the field name 'badField', got: " + ex.getMessage());
  }

  @Test
  void errorMessageForTooFewColumnsWithNestedRecord() {
    record Inner(String a, String b) {}

    record Outer(int id, Inner inner) {}

    RowMapper<Outer> mapper = RecordRowMapper.of(Outer.class);
    // Outer needs 3 columns (id, a, b) but we only provide 2
    Row r = row(new String[] {"id", "a"}, new String[] {"1", "hello"});

    assertThrows(IndexOutOfBoundsException.class, () -> mapper.map(r));
  }

  // --- 4. Nested null patterns ---

  record OptionalAddress(String street, String city) {}

  record UserOptAddr(int id, String name, OptionalAddress addr) {}

  @Test
  void nestedRecordAllNullFieldsStillConstructsObject() {
    RowMapper<UserOptAddr> mapper = RecordRowMapper.of(UserOptAddr.class);
    Row r =
        row(new String[] {"id", "name", "street", "city"}, new String[] {"1", "Alice", null, null});

    UserOptAddr user = mapper.map(r);

    assertEquals(1, user.id());
    assertEquals("Alice", user.name());
    assertNotNull(
        user.addr(), "Nested record with all null fields should still be constructed, not null");
    assertNull(user.addr().street());
    assertNull(user.addr().city());
  }

  @Test
  void nestedWithMixedNullAndNonNull() {
    RowMapper<UserOptAddr> mapper = RecordRowMapper.of(UserOptAddr.class);
    Row r =
        row(
            new String[] {"id", "name", "street", "city"},
            new String[] {"2", "Bob", null, "Springfield"});

    UserOptAddr user = mapper.map(r);

    assertEquals(2, user.id());
    assertEquals("Bob", user.name());
    assertNull(user.addr().street());
    assertEquals("Springfield", user.addr().city());
  }

  // --- 5. Type conversion edge cases ---

  @Test
  void extremeIntegerValues() {
    record IntRecord(Integer i) {}

    RowMapper<IntRecord> mapper = RecordRowMapper.of(IntRecord.class);

    IntRecord max =
        mapper.map(row(new String[] {"i"}, new String[] {String.valueOf(Integer.MAX_VALUE)}));
    IntRecord min =
        mapper.map(row(new String[] {"i"}, new String[] {String.valueOf(Integer.MIN_VALUE)}));

    assertEquals(Integer.MAX_VALUE, max.i());
    assertEquals(Integer.MIN_VALUE, min.i());
  }

  @Test
  void extremeLongValues() {
    record LongRecord(Long l) {}

    RowMapper<LongRecord> mapper = RecordRowMapper.of(LongRecord.class);

    LongRecord max =
        mapper.map(row(new String[] {"l"}, new String[] {String.valueOf(Long.MAX_VALUE)}));
    LongRecord min =
        mapper.map(row(new String[] {"l"}, new String[] {String.valueOf(Long.MIN_VALUE)}));

    assertEquals(Long.MAX_VALUE, max.l());
    assertEquals(Long.MIN_VALUE, min.l());
  }

  @Test
  void largeBigDecimal() {
    record BdRecord(BigDecimal bd) {}

    RowMapper<BdRecord> mapper = RecordRowMapper.of(BdRecord.class);

    String large = "123456789012345678901234567890.123456789";
    BdRecord rec = mapper.map(row(new String[] {"bd"}, new String[] {large}));

    assertEquals(new BigDecimal(large), rec.bd());
  }

  @Test
  void negativeNumbers() {
    record Negatives(int i, long l, float f, double d, short s) {}

    RowMapper<Negatives> mapper = RecordRowMapper.of(Negatives.class);
    Row r =
        row(
            new String[] {"i", "l", "f", "d", "s"},
            new String[] {"-42", "-9999999999", "-3.14", "-2.718281828", "-7"});

    Negatives rec = mapper.map(r);

    assertEquals(-42, rec.i());
    assertEquals(-9999999999L, rec.l());
    assertEquals(-3.14f, rec.f(), 0.001f);
    assertEquals(-2.718281828, rec.d(), 0.000000001);
    assertEquals((short) -7, rec.s());
  }

  // --- 6. Date/time edge cases ---

  @Test
  void dateTimeWithSpaceInsteadOfT() {
    // MySQL returns "2024-06-15 14:30:00" (space instead of T).
    // Row.getLocalDateTime replaces space with T before parsing, so this succeeds.
    record DtRecord(LocalDateTime dateTime) {}

    RowMapper<DtRecord> mapper = RecordRowMapper.of(DtRecord.class);
    Row r = row(new String[] {"dateTime"}, new String[] {"2024-06-15 14:30:00"});

    DtRecord rec = mapper.map(r);

    assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), rec.dateTime());
  }

  @Test
  void offsetDateTimeWithDifferentOffsets() {
    record OdtRecord(OffsetDateTime india, OffsetDateTime brazil) {}

    RowMapper<OdtRecord> mapper = RecordRowMapper.of(OdtRecord.class);
    Row r =
        row(
            new String[] {"india", "brazil"},
            new String[] {"2024-06-15T14:30:00+05:30", "2024-06-15T14:30:00-03:00"});

    OdtRecord rec = mapper.map(r);

    assertEquals(ZoneOffset.ofHoursMinutes(5, 30), rec.india().getOffset());
    assertEquals(ZoneOffset.ofHours(-3), rec.brazil().getOffset());
    assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), rec.india().toLocalDateTime());
    assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), rec.brazil().toLocalDateTime());
  }

  // --- 7. Special characters ---

  @Test
  void unicodeInStrings() {
    record UnicodeRecord(String emoji, String chinese, String accented) {}

    RowMapper<UnicodeRecord> mapper = RecordRowMapper.of(UnicodeRecord.class);
    Row r = row(new String[] {"emoji", "chinese", "accented"}, new String[] {"🎉", "你好", "àéîöü"});

    UnicodeRecord rec = mapper.map(r);

    assertEquals("🎉", rec.emoji());
    assertEquals("你好", rec.chinese());
    assertEquals("àéîöü", rec.accented());
  }

  @Test
  void specialSqlCharacters() {
    record SqlChars(String quoted, String escaped, String semicolon) {}

    RowMapper<SqlChars> mapper = RecordRowMapper.of(SqlChars.class);
    Row r =
        row(
            new String[] {"quoted", "escaped", "semicolon"},
            new String[] {"it's a test", "back\\slash", "SELECT 1; DROP TABLE users"});

    SqlChars rec = mapper.map(r);

    assertEquals("it's a test", rec.quoted());
    assertEquals("back\\slash", rec.escaped());
    assertEquals("SELECT 1; DROP TABLE users", rec.semicolon());
  }

  // --- 8. Column position tracking ---

  @Test
  void columnPositionWithManyFields() {
    record ManyFields(
        String f0,
        int f1,
        long f2,
        String f3,
        double f4,
        boolean f5,
        String f6,
        short f7,
        float f8,
        String f9,
        Integer f10,
        Long f11) {}

    RowMapper<ManyFields> mapper = RecordRowMapper.of(ManyFields.class);
    Row r =
        row(
            new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11"},
            new String[] {
              "zero", "1", "2", "three", "4.0", "true", "six", "7", "8.5", "nine", "10", "11"
            });

    ManyFields rec = mapper.map(r);

    assertEquals("zero", rec.f0());
    assertEquals(1, rec.f1());
    assertEquals(2L, rec.f2());
    assertEquals("three", rec.f3());
    assertEquals(4.0, rec.f4());
    assertTrue(rec.f5());
    assertEquals("six", rec.f6());
    assertEquals((short) 7, rec.f7());
    assertEquals(8.5f, rec.f8());
    assertEquals("nine", rec.f9());
    assertEquals(10, rec.f10());
    assertEquals(11L, rec.f11());
  }
}
