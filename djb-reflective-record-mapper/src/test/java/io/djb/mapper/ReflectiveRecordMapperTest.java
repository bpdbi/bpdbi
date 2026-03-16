package io.djb.mapper;

import io.djb.ColumnDescriptor;
import io.djb.Row;
import io.djb.RowMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReflectiveRecordMapperTest {

    // --- Test records ---

    record SimpleUser(Integer id, String name, String email) {}
    record PrimitiveUser(int id, String name) {}
    record AllTypes(
        String s, Integer i, Long l, Short sh, Float f, Double d, Boolean b,
        BigDecimal bd, UUID uuid
    ) {}
    record DateTimeRecord(LocalDate date, LocalTime time, LocalDateTime dateTime) {}
    record PrimitivesRecord(int i, long l, short s, float f, double d, boolean b) {}
    record SingleField(String value) {}
    record BytesRecord(String name, byte[] data) {}
    record SnakeCaseUser(
        int id,
        @ColumnName("first_name") String firstName,
        @ColumnName("last_name") String lastName
    ) {}
    record MixedAnnotation(
        int id,
        @ColumnName("display_name") String displayName,
        String email
    ) {}

    // --- Helper to build a Row from column names and text values ---

    private static Row row(String[] columnNames, String[] values) {
        ColumnDescriptor[] cols = new ColumnDescriptor[columnNames.length];
        byte[][] rawValues = new byte[values.length][];
        for (int i = 0; i < columnNames.length; i++) {
            cols[i] = new ColumnDescriptor(columnNames[i], 0, (short) 0, 0, (short) 0, 0);
            rawValues[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
        }
        return new Row(cols, rawValues, null, null);
    }

    // --- Tests ---

    @Test
    void mapsSimpleRecord() {
        RowMapper<SimpleUser> mapper = ReflectiveRecordMapper.of(SimpleUser.class);
        Row r = row(new String[]{"id", "name", "email"}, new String[]{"42", "Alice", "alice@example.com"});

        SimpleUser user = mapper.map(r);

        assertEquals(42, user.id());
        assertEquals("Alice", user.name());
        assertEquals("alice@example.com", user.email());
    }

    @Test
    void mapsPrimitiveInt() {
        RowMapper<PrimitiveUser> mapper = ReflectiveRecordMapper.of(PrimitiveUser.class);
        Row r = row(new String[]{"id", "name"}, new String[]{"7", "Bob"});

        PrimitiveUser user = mapper.map(r);

        assertEquals(7, user.id());
        assertEquals("Bob", user.name());
    }

    @Test
    void primitiveDefaultsToZeroOnNull() {
        RowMapper<PrimitivesRecord> mapper = ReflectiveRecordMapper.of(PrimitivesRecord.class);
        Row r = row(
            new String[]{"i", "l", "s", "f", "d", "b"},
            new String[]{null, null, null, null, null, null}
        );

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
        RowMapper<SimpleUser> mapper = ReflectiveRecordMapper.of(SimpleUser.class);
        Row r = row(new String[]{"id", "name", "email"}, new String[]{null, null, null});

        SimpleUser user = mapper.map(r);

        assertNull(user.id());
        assertNull(user.name());
        assertNull(user.email());
    }

    @Test
    void mapsNumericAndUuidTypes() {
        UUID testUuid = UUID.randomUUID();
        RowMapper<AllTypes> mapper = ReflectiveRecordMapper.of(AllTypes.class);
        Row r = row(
            new String[]{"s", "i", "l", "sh", "f", "d", "b", "bd", "uuid"},
            new String[]{"hello", "1", "2", "3", "1.5", "2.5", "true", "99.99", testUuid.toString()}
        );

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
        RowMapper<DateTimeRecord> mapper = ReflectiveRecordMapper.of(DateTimeRecord.class);
        Row r = row(
            new String[]{"date", "time", "dateTime"},
            new String[]{"2024-06-15", "14:30:00", "2024-06-15T14:30:00"}
        );

        DateTimeRecord rec = mapper.map(r);

        assertEquals(LocalDate.of(2024, 6, 15), rec.date());
        assertEquals(LocalTime.of(14, 30, 0), rec.time());
        assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), rec.dateTime());
    }

    @Test
    void mapsSingleFieldRecord() {
        RowMapper<SingleField> mapper = ReflectiveRecordMapper.of(SingleField.class);
        Row r = row(new String[]{"value"}, new String[]{"test"});

        assertEquals("test", mapper.map(r).value());
    }

    @Test
    void mapsByteArray() {
        RowMapper<BytesRecord> mapper = ReflectiveRecordMapper.of(BytesRecord.class);
        Row r = row(new String[]{"name", "data"}, new String[]{"file.bin", "rawbytes"});

        BytesRecord rec = mapper.map(r);
        assertEquals("file.bin", rec.name());
        assertArrayEquals("rawbytes".getBytes(StandardCharsets.UTF_8), rec.data());
    }

    @Test
    void reuseMapperAcrossMultipleRows() {
        RowMapper<SimpleUser> mapper = ReflectiveRecordMapper.of(SimpleUser.class);

        SimpleUser u1 = mapper.map(row(new String[]{"id", "name", "email"}, new String[]{"1", "A", "a@x"}));
        SimpleUser u2 = mapper.map(row(new String[]{"id", "name", "email"}, new String[]{"2", "B", "b@x"}));

        assertEquals(1, u1.id());
        assertEquals("A", u1.name());
        assertEquals(2, u2.id());
        assertEquals("B", u2.name());
    }

    @Test
    void columnNameAnnotationOverridesComponentName() {
        RowMapper<SnakeCaseUser> mapper = ReflectiveRecordMapper.of(SnakeCaseUser.class);
        Row r = row(
            new String[]{"id", "first_name", "last_name"},
            new String[]{"1", "Alice", "Smith"}
        );

        SnakeCaseUser user = mapper.map(r);

        assertEquals(1, user.id());
        assertEquals("Alice", user.firstName());
        assertEquals("Smith", user.lastName());
    }

    @Test
    void mixedAnnotatedAndUnannotatedComponents() {
        RowMapper<MixedAnnotation> mapper = ReflectiveRecordMapper.of(MixedAnnotation.class);
        Row r = row(
            new String[]{"id", "display_name", "email"},
            new String[]{"5", "Bob", "bob@example.com"}
        );

        MixedAnnotation rec = mapper.map(r);

        assertEquals(5, rec.id());
        assertEquals("Bob", rec.displayName());
        assertEquals("bob@example.com", rec.email());
    }

    @Test
    void rejectsNonRecordClass() {
        assertThrows(IllegalArgumentException.class, () ->
            ReflectiveRecordMapper.of((Class) String.class));
    }

    @Test
    void rejectsUnsupportedComponentType() {
        record BadRecord(Object unsupported) {}

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ReflectiveRecordMapper.of(BadRecord.class));
        assertTrue(ex.getMessage().contains("Unsupported type"));
        assertTrue(ex.getMessage().contains("unsupported"));
    }

    @Test
    void throwsOnMissingColumn() {
        RowMapper<SimpleUser> mapper = ReflectiveRecordMapper.of(SimpleUser.class);
        Row r = row(new String[]{"id", "name"}, new String[]{"1", "Alice"});

        // "email" column is missing from the row
        assertThrows(IllegalArgumentException.class, () -> mapper.map(r));
    }
}
