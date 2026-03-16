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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JavaBeanMapperTest {

    // --- Test beans ---

    public static class SimpleUser {
        private Integer id;
        private String name;
        private String email;
        public SimpleUser() {}
        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
    }

    public static class PrimitiveBean {
        private int i;
        private long l;
        private short s;
        private float f;
        private double d;
        private boolean b;
        public PrimitiveBean() {}
        public int getI() { return i; }
        public void setI(int i) { this.i = i; }
        public long getL() { return l; }
        public void setL(long l) { this.l = l; }
        public short getS() { return s; }
        public void setS(short s) { this.s = s; }
        public float getF() { return f; }
        public void setF(float f) { this.f = f; }
        public double getD() { return d; }
        public void setD(double d) { this.d = d; }
        public boolean isB() { return b; }
        public void setB(boolean b) { this.b = b; }
    }

    public static class AllTypesBean {
        private String s;
        private Integer i;
        private Long l;
        private BigDecimal bd;
        private UUID uuid;
        private LocalDate date;
        private LocalTime time;
        private LocalDateTime dateTime;
        public AllTypesBean() {}
        public String getS() { return s; }
        public void setS(String s) { this.s = s; }
        public Integer getI() { return i; }
        public void setI(Integer i) { this.i = i; }
        public Long getL() { return l; }
        public void setL(Long l) { this.l = l; }
        public BigDecimal getBd() { return bd; }
        public void setBd(BigDecimal bd) { this.bd = bd; }
        public UUID getUuid() { return uuid; }
        public void setUuid(UUID uuid) { this.uuid = uuid; }
        public LocalDate getDate() { return date; }
        public void setDate(LocalDate date) { this.date = date; }
        public LocalTime getTime() { return time; }
        public void setTime(LocalTime time) { this.time = time; }
        public LocalDateTime getDateTime() { return dateTime; }
        public void setDateTime(LocalDateTime dateTime) { this.dateTime = dateTime; }
    }

    public static class ReadOnlyBean {
        private final String value;
        public ReadOnlyBean() { this.value = null; }
        public String getValue() { return value; }
        // no setter — should be skipped
    }

    public static class UnsupportedTypeBean {
        private Object bad;
        public UnsupportedTypeBean() {}
        public Object getBad() { return bad; }
        public void setBad(Object bad) { this.bad = bad; }
    }

    public static class NoDefaultConstructor {
        private String name;
        public NoDefaultConstructor(String name) { this.name = name; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    // --- Helper ---

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
    void mapsSimpleBean() {
        RowMapper<SimpleUser> mapper = JavaBeanMapper.of(SimpleUser.class);
        Row r = row(new String[]{"id", "name", "email"}, new String[]{"42", "Alice", "alice@example.com"});

        SimpleUser user = mapper.map(r);

        assertEquals(42, user.getId());
        assertEquals("Alice", user.getName());
        assertEquals("alice@example.com", user.getEmail());
    }

    @Test
    void primitiveDefaultsToZeroOnNull() {
        RowMapper<PrimitiveBean> mapper = JavaBeanMapper.of(PrimitiveBean.class);
        Row r = row(
            new String[]{"i", "l", "s", "f", "d", "b"},
            new String[]{null, null, null, null, null, null}
        );

        PrimitiveBean bean = mapper.map(r);

        assertEquals(0, bean.getI());
        assertEquals(0L, bean.getL());
        assertEquals((short) 0, bean.getS());
        assertEquals(0f, bean.getF());
        assertEquals(0d, bean.getD());
        assertFalse(bean.isB());
    }

    @Test
    void boxedTypesReturnNullOnNull() {
        RowMapper<SimpleUser> mapper = JavaBeanMapper.of(SimpleUser.class);
        Row r = row(new String[]{"id", "name", "email"}, new String[]{null, null, null});

        SimpleUser user = mapper.map(r);

        assertNull(user.getId());
        assertNull(user.getName());
        assertNull(user.getEmail());
    }

    @Test
    void mapsAllSupportedTypes() {
        UUID testUuid = UUID.randomUUID();
        RowMapper<AllTypesBean> mapper = JavaBeanMapper.of(AllTypesBean.class);
        Row r = row(
            new String[]{"s", "i", "l", "bd", "uuid", "date", "time", "dateTime"},
            new String[]{"hello", "1", "2", "99.99", testUuid.toString(),
                "2024-06-15", "14:30:00", "2024-06-15T14:30:00"}
        );

        AllTypesBean bean = mapper.map(r);

        assertEquals("hello", bean.getS());
        assertEquals(1, bean.getI());
        assertEquals(2L, bean.getL());
        assertEquals(new BigDecimal("99.99"), bean.getBd());
        assertEquals(testUuid, bean.getUuid());
        assertEquals(LocalDate.of(2024, 6, 15), bean.getDate());
        assertEquals(LocalTime.of(14, 30, 0), bean.getTime());
        assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), bean.getDateTime());
    }

    @Test
    void skipsPropertiesWithoutSetter() {
        RowMapper<ReadOnlyBean> mapper = JavaBeanMapper.of(ReadOnlyBean.class);
        // "value" has no setter, so the mapper has no properties to set — should not error
        Row r = row(new String[]{"value"}, new String[]{"test"});

        ReadOnlyBean bean = mapper.map(r);
        assertNull(bean.getValue());
    }

    @Test
    void reuseMapperAcrossMultipleRows() {
        RowMapper<SimpleUser> mapper = JavaBeanMapper.of(SimpleUser.class);

        SimpleUser u1 = mapper.map(row(new String[]{"id", "name", "email"}, new String[]{"1", "A", "a@x"}));
        SimpleUser u2 = mapper.map(row(new String[]{"id", "name", "email"}, new String[]{"2", "B", "b@x"}));

        assertEquals(1, u1.getId());
        assertEquals("A", u1.getName());
        assertEquals(2, u2.getId());
        assertEquals("B", u2.getName());
    }

    @Test
    void rejectsUnsupportedPropertyType() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            JavaBeanMapper.of(UnsupportedTypeBean.class));
        assertTrue(ex.getMessage().contains("Unsupported type"));
        assertTrue(ex.getMessage().contains("bad"));
    }

    @Test
    void rejectsNoDefaultConstructor() {
        assertThrows(IllegalArgumentException.class, () ->
            JavaBeanMapper.of(NoDefaultConstructor.class));
    }
}
