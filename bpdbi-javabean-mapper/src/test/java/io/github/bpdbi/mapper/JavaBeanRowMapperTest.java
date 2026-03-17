package io.github.bpdbi.mapper;

import static io.github.bpdbi.core.test.TestRows.row;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JavaBeanRowMapperTest {

  // --- Test beans ---

  public static class SimpleUser {

    private Integer id;
    private String name;
    private String email;

    public SimpleUser() {}

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }
  }

  public static class PrimitiveBean {

    private int i;
    private long l;
    private short s;
    private float f;
    private double d;
    private boolean b;

    public PrimitiveBean() {}

    public int getI() {
      return i;
    }

    public void setI(int i) {
      this.i = i;
    }

    public long getL() {
      return l;
    }

    public void setL(long l) {
      this.l = l;
    }

    public short getS() {
      return s;
    }

    public void setS(short s) {
      this.s = s;
    }

    public float getF() {
      return f;
    }

    public void setF(float f) {
      this.f = f;
    }

    public double getD() {
      return d;
    }

    public void setD(double d) {
      this.d = d;
    }

    public boolean isB() {
      return b;
    }

    public void setB(boolean b) {
      this.b = b;
    }
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

    public String getS() {
      return s;
    }

    public void setS(String s) {
      this.s = s;
    }

    public Integer getI() {
      return i;
    }

    public void setI(Integer i) {
      this.i = i;
    }

    public Long getL() {
      return l;
    }

    public void setL(Long l) {
      this.l = l;
    }

    public BigDecimal getBd() {
      return bd;
    }

    public void setBd(BigDecimal bd) {
      this.bd = bd;
    }

    public UUID getUuid() {
      return uuid;
    }

    public void setUuid(UUID uuid) {
      this.uuid = uuid;
    }

    public LocalDate getDate() {
      return date;
    }

    public void setDate(LocalDate date) {
      this.date = date;
    }

    public LocalTime getTime() {
      return time;
    }

    public void setTime(LocalTime time) {
      this.time = time;
    }

    public LocalDateTime getDateTime() {
      return dateTime;
    }

    public void setDateTime(LocalDateTime dateTime) {
      this.dateTime = dateTime;
    }
  }

  public static class ReadOnlyBean {

    private final String value;

    public ReadOnlyBean() {
      this.value = null;
    }

    public String getValue() {
      return value;
    }
    // no setter — should be skipped
  }

  public static class UnsupportedTypeBean {

    private Object bad;

    public UnsupportedTypeBean() {}

    public Object getBad() {
      return bad;
    }

    public void setBad(Object bad) {
      this.bad = bad;
    }
  }

  public static class NoDefaultConstructor {

    private String name;

    public NoDefaultConstructor(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  // --- Nested bean types ---

  public static class Address {

    private String street;
    private String city;

    public Address() {}

    public String getStreet() {
      return street;
    }

    public void setStreet(String street) {
      this.street = street;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }
  }

  public static class UserWithAddress {

    private int id;
    private String name;
    private Address address;

    public UserWithAddress() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Address getAddress() {
      return address;
    }

    public void setAddress(Address address) {
      this.address = address;
    }
  }

  public static class Coordinate {

    private double lat;
    private double lng;

    public Coordinate() {}

    public double getLat() {
      return lat;
    }

    public void setLat(double lat) {
      this.lat = lat;
    }

    public double getLng() {
      return lng;
    }

    public void setLng(double lng) {
      this.lng = lng;
    }
  }

  public static class Location {

    private String name;
    private Coordinate coord;

    public Location() {}

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Coordinate getCoord() {
      return coord;
    }

    public void setCoord(Coordinate coord) {
      this.coord = coord;
    }
  }

  public static class Trip {

    private int id;
    private Location origin;
    private Location destination;

    public Trip() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Location getOrigin() {
      return origin;
    }

    public void setOrigin(Location origin) {
      this.origin = origin;
    }

    public Location getDestination() {
      return destination;
    }

    public void setDestination(Location destination) {
      this.destination = destination;
    }
  }

  public static class WithUnsupportedList {

    private int id;
    private List<String> tags;

    public WithUnsupportedList() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }
  }

  public static class BoxedNumericBean {

    private Short sh;
    private Float f;
    private Double d;
    private Boolean b;

    public BoxedNumericBean() {}

    public Short getSh() {
      return sh;
    }

    public void setSh(Short sh) {
      this.sh = sh;
    }

    public Float getF() {
      return f;
    }

    public void setF(Float f) {
      this.f = f;
    }

    public Double getD() {
      return d;
    }

    public void setD(Double d) {
      this.d = d;
    }

    public Boolean getB() {
      return b;
    }

    public void setB(Boolean b) {
      this.b = b;
    }
  }

  public static class BytesBean {

    private String name;
    private byte[] data;

    public BytesBean() {}

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public byte[] getData() {
      return data;
    }

    public void setData(byte[] data) {
      this.data = data;
    }
  }

  public static class OffsetDateTimeBean {

    private String name;
    private OffsetDateTime created;

    public OffsetDateTimeBean() {}

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public OffsetDateTime getCreated() {
      return created;
    }

    public void setCreated(OffsetDateTime created) {
      this.created = created;
    }
  }

  public enum Status {
    ACTIVE,
    INACTIVE
  }

  public static class OrderBean {

    private int id;
    private Status status;

    public OrderBean() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Status getStatus() {
      return status;
    }

    public void setStatus(Status status) {
      this.status = status;
    }
  }

  public static class MixedNullBean {

    private int id;
    private Integer nullableId;
    private String name;
    private Long count;

    public MixedNullBean() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Integer getNullableId() {
      return nullableId;
    }

    public void setNullableId(Integer nullableId) {
      this.nullableId = nullableId;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Long getCount() {
      return count;
    }

    public void setCount(Long count) {
      this.count = count;
    }
  }

  // --- Flat bean tests ---

  @Test
  void mapsSimpleBean() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r =
        row(
            new String[] {"id", "name", "email"},
            new String[] {"42", "Alice", "alice@example.com"});

    SimpleUser user = mapper.map(r);

    assertEquals(42, user.getId());
    assertEquals("Alice", user.getName());
    assertEquals("alice@example.com", user.getEmail());
  }

  @Test
  void primitiveDefaultsToZeroOnNull() {
    RowMapper<PrimitiveBean> mapper = JavaBeanRowMapper.of(PrimitiveBean.class);
    Row r =
        row(
            new String[] {"i", "l", "s", "f", "d", "b"},
            new String[] {null, null, null, null, null, null});

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
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r = row(new String[] {"id", "name", "email"}, new String[] {null, null, null});

    SimpleUser user = mapper.map(r);

    assertNull(user.getId());
    assertNull(user.getName());
    assertNull(user.getEmail());
  }

  @Test
  void mapsAllSupportedTypes() {
    UUID testUuid = UUID.randomUUID();
    RowMapper<AllTypesBean> mapper = JavaBeanRowMapper.of(AllTypesBean.class);
    Row r =
        row(
            new String[] {"s", "i", "l", "bd", "uuid", "date", "time", "dateTime"},
            new String[] {
              "hello",
              "1",
              "2",
              "99.99",
              testUuid.toString(),
              "2024-06-15",
              "14:30:00",
              "2024-06-15T14:30:00"
            });

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
    RowMapper<ReadOnlyBean> mapper = JavaBeanRowMapper.of(ReadOnlyBean.class);
    Row r = row(new String[] {"value"}, new String[] {"test"});

    ReadOnlyBean bean = mapper.map(r);
    assertNull(bean.getValue());
  }

  @Test
  void reuseMapperAcrossMultipleRows() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);

    SimpleUser u1 =
        mapper.map(row(new String[] {"id", "name", "email"}, new String[] {"1", "A", "a@x"}));
    SimpleUser u2 =
        mapper.map(row(new String[] {"id", "name", "email"}, new String[] {"2", "B", "b@x"}));

    assertEquals(1, u1.getId());
    assertEquals("A", u1.getName());
    assertEquals(2, u2.getId());
    assertEquals("B", u2.getName());
  }

  @Test
  void rejectsUnsupportedPropertyType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> JavaBeanRowMapper.of(UnsupportedTypeBean.class));
    assertTrue(ex.getMessage().contains("Unsupported type"));
    assertTrue(ex.getMessage().contains("bad"));
  }

  @Test
  void rejectsNoDefaultConstructor() {
    assertThrows(
        IllegalArgumentException.class, () -> JavaBeanRowMapper.of(NoDefaultConstructor.class));
  }

  // --- Nested bean tests ---

  @Test
  void nestedBeanConsumesFlatColumns() {
    // Columns: id, name, street, city
    RowMapper<UserWithAddress> mapper = JavaBeanRowMapper.of(UserWithAddress.class);
    Row r =
        row(
            new String[] {"id", "name", "street", "city"},
            new String[] {"1", "Alice", "123 Main St", "Springfield"});

    UserWithAddress user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("Alice", user.getName());
    assertNotNull(user.getAddress());
    assertEquals("123 Main St", user.getAddress().getStreet());
    assertEquals("Springfield", user.getAddress().getCity());
  }

  @Test
  void deeplyNestedThreeLevels() {
    // Trip(id, origin: Location(name, coord: Coordinate(lat, lng)), destination: Location(...))
    RowMapper<Trip> mapper = JavaBeanRowMapper.of(Trip.class);
    Row r =
        row(
            new String[] {"id", "name", "lat", "lng", "name", "lat", "lng"},
            new String[] {"1", "Home", "52.37", "4.89", "Office", "52.35", "4.91"});

    Trip trip = mapper.map(r);

    assertEquals(1, trip.getId());
    assertEquals("Home", trip.getOrigin().getName());
    assertEquals(52.37, trip.getOrigin().getCoord().getLat());
    assertEquals(4.89, trip.getOrigin().getCoord().getLng());
    assertEquals("Office", trip.getDestination().getName());
    assertEquals(52.35, trip.getDestination().getCoord().getLat());
    assertEquals(4.91, trip.getDestination().getCoord().getLng());
  }

  @Test
  void nestedBeanWithNullScalarFields() {
    RowMapper<UserWithAddress> mapper = JavaBeanRowMapper.of(UserWithAddress.class);
    Row r =
        row(new String[] {"id", "name", "street", "city"}, new String[] {"1", "Alice", null, null});

    UserWithAddress user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("Alice", user.getName());
    assertNotNull(user.getAddress());
    assertNull(user.getAddress().getStreet());
    assertNull(user.getAddress().getCity());
  }

  @Test
  void reuseMapperWithNestedBeans() {
    RowMapper<UserWithAddress> mapper = JavaBeanRowMapper.of(UserWithAddress.class);

    UserWithAddress u1 =
        mapper.map(
            row(
                new String[] {"id", "name", "street", "city"},
                new String[] {"1", "Alice", "Main St", "CityA"}));
    UserWithAddress u2 =
        mapper.map(
            row(
                new String[] {"id", "name", "street", "city"},
                new String[] {"2", "Bob", "Oak Ave", "CityB"}));

    assertEquals("Main St", u1.getAddress().getStreet());
    assertEquals("Oak Ave", u2.getAddress().getStreet());
  }

  @Test
  void listPropertyIsStillRejected() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> JavaBeanRowMapper.of(WithUnsupportedList.class));
    assertTrue(ex.getMessage().contains("Unsupported type"));
  }

  // --- Additional coverage tests ---

  @Test
  void mapsBoxedShortFloatDoubleBoolean() {
    RowMapper<BoxedNumericBean> mapper = JavaBeanRowMapper.of(BoxedNumericBean.class);
    Row r = row(new String[] {"sh", "f", "d", "b"}, new String[] {"3", "1.5", "2.5", "true"});

    BoxedNumericBean bean = mapper.map(r);

    assertEquals((short) 3, bean.getSh());
    assertEquals(1.5f, bean.getF());
    assertEquals(2.5, bean.getD());
    assertTrue(bean.getB());
  }

  @Test
  void boxedNumericTypesReturnNullOnNull() {
    RowMapper<BoxedNumericBean> mapper = JavaBeanRowMapper.of(BoxedNumericBean.class);
    Row r = row(new String[] {"sh", "f", "d", "b"}, new String[] {null, null, null, null});

    BoxedNumericBean bean = mapper.map(r);

    assertNull(bean.getSh());
    assertNull(bean.getF());
    assertNull(bean.getD());
    assertNull(bean.getB());
  }

  @Test
  void mapsByteArray() {
    RowMapper<BytesBean> mapper = JavaBeanRowMapper.of(BytesBean.class);
    Row r = row(new String[] {"name", "data"}, new String[] {"file.bin", "rawbytes"});

    BytesBean bean = mapper.map(r);

    assertEquals("file.bin", bean.getName());
    assertArrayEquals("rawbytes".getBytes(StandardCharsets.UTF_8), bean.getData());
  }

  @Test
  void mapsOffsetDateTime() {
    RowMapper<OffsetDateTimeBean> mapper = JavaBeanRowMapper.of(OffsetDateTimeBean.class);
    Row r =
        row(new String[] {"name", "created"}, new String[] {"event", "2024-06-15T14:30:00+02:00"});

    OffsetDateTimeBean bean = mapper.map(r);

    assertEquals("event", bean.getName());
    assertEquals(
        OffsetDateTime.of(2024, 6, 15, 14, 30, 0, 0, ZoneOffset.ofHours(2)), bean.getCreated());
  }

  @Test
  void mixedPrimitiveAndBoxedNulls() {
    RowMapper<MixedNullBean> mapper = JavaBeanRowMapper.of(MixedNullBean.class);
    Row r =
        row(
            new String[] {"id", "nullableId", "name", "count"},
            new String[] {null, null, "Alice", null});

    MixedNullBean bean = mapper.map(r);

    // primitive int defaults to 0 on null
    assertEquals(0, bean.getId());
    // boxed Integer returns null
    assertNull(bean.getNullableId());
    assertEquals("Alice", bean.getName());
    // boxed Long returns null
    assertNull(bean.getCount());
  }

  @Test
  void mapsEnumField() {
    RowMapper<OrderBean> mapper = JavaBeanRowMapper.of(OrderBean.class);
    Row r = row(new String[] {"id", "status"}, new String[] {"1", "ACTIVE"});

    OrderBean order = mapper.map(r);

    assertEquals(1, order.getId());
    assertEquals(Status.ACTIVE, order.getStatus());
  }

  @Test
  void nullEnumFieldReturnsNull() {
    RowMapper<OrderBean> mapper = JavaBeanRowMapper.of(OrderBean.class);
    Row r = row(new String[] {"id", "status"}, new String[] {"1", null});

    OrderBean order = mapper.map(r);

    assertEquals(1, order.getId());
    assertNull(order.getStatus());
  }

  @Test
  void distinguishesEmptyStringFromNull() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r = row(new String[] {"id", "name", "email"}, new String[] {"1", "", null});

    SimpleUser user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("", user.getName());
    assertNull(user.getEmail());
  }

  @Test
  void throwsWhenRowHasFewerColumnsThanProperties() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r = row(new String[] {"id", "name"}, new String[] {"1", "Alice"});

    // Bean has 3 settable properties but row only has 2 columns;
    // the ArrayIndexOutOfBoundsException is wrapped in IllegalStateException
    assertThrows(IllegalStateException.class, () -> mapper.map(r));
  }

  // --- Comprehensive null matrix ---

  public static class AllNullableBean {

    private Integer i;
    private Long l;
    private Short s;
    private Float f;
    private Double d;
    private Boolean b;
    private BigDecimal bd;
    private UUID uuid;

    public AllNullableBean() {}

    public Integer getI() {
      return i;
    }

    public void setI(Integer i) {
      this.i = i;
    }

    public Long getL() {
      return l;
    }

    public void setL(Long l) {
      this.l = l;
    }

    public Short getS() {
      return s;
    }

    public void setS(Short s) {
      this.s = s;
    }

    public Float getF() {
      return f;
    }

    public void setF(Float f) {
      this.f = f;
    }

    public Double getD() {
      return d;
    }

    public void setD(Double d) {
      this.d = d;
    }

    public Boolean getB() {
      return b;
    }

    public void setB(Boolean b) {
      this.b = b;
    }

    public BigDecimal getBd() {
      return bd;
    }

    public void setBd(BigDecimal bd) {
      this.bd = bd;
    }

    public UUID getUuid() {
      return uuid;
    }

    public void setUuid(UUID uuid) {
      this.uuid = uuid;
    }
  }

  @Test
  void allBoxedTypesReturnNullIndependently() {
    RowMapper<AllNullableBean> mapper = JavaBeanRowMapper.of(AllNullableBean.class);
    Row r =
        row(
            new String[] {"i", "l", "s", "f", "d", "b", "bd", "uuid"},
            new String[] {null, null, null, null, null, null, null, null});

    AllNullableBean bean = mapper.map(r);

    assertNull(bean.getI());
    assertNull(bean.getL());
    assertNull(bean.getS());
    assertNull(bean.getF());
    assertNull(bean.getD());
    assertNull(bean.getB());
    assertNull(bean.getBd());
    assertNull(bean.getUuid());
  }

  // --- Nested null patterns ---

  @Test
  void nestedBeanAllNullFieldsStillConstructsObject() {
    RowMapper<UserWithAddress> mapper = JavaBeanRowMapper.of(UserWithAddress.class);
    Row r =
        row(new String[] {"id", "name", "street", "city"}, new String[] {null, null, null, null});

    UserWithAddress user = mapper.map(r);

    assertEquals(0, user.getId()); // primitive defaults to 0
    assertNull(user.getName());
    assertNotNull(user.getAddress()); // nested bean is constructed, not null
    assertNull(user.getAddress().getStreet());
    assertNull(user.getAddress().getCity());
  }

  @Test
  void nestedWithMixedNullAndNonNull() {
    RowMapper<UserWithAddress> mapper = JavaBeanRowMapper.of(UserWithAddress.class);
    Row r =
        row(
            new String[] {"id", "name", "street", "city"},
            new String[] {"1", "Alice", null, "Springfield"});

    UserWithAddress user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("Alice", user.getName());
    assertNotNull(user.getAddress());
    assertNull(user.getAddress().getStreet());
    assertEquals("Springfield", user.getAddress().getCity());
  }

  // --- Type conversion edge cases ---

  @Test
  void extremeIntegerValues() {
    RowMapper<AllNullableBean> mapper = JavaBeanRowMapper.of(AllNullableBean.class);
    Row r =
        row(
            new String[] {"i", "l", "s", "f", "d", "b", "bd", "uuid"},
            new String[] {
              String.valueOf(Integer.MAX_VALUE),
              String.valueOf(Long.MAX_VALUE),
              String.valueOf(Short.MAX_VALUE),
              "0",
              "0",
              "false",
              "0",
              "00000000-0000-0000-0000-000000000000"
            });

    AllNullableBean bean = mapper.map(r);

    assertEquals(Integer.MAX_VALUE, bean.getI());
    assertEquals(Long.MAX_VALUE, bean.getL());
    assertEquals(Short.MAX_VALUE, bean.getS());

    Row r2 =
        row(
            new String[] {"i", "l", "s", "f", "d", "b", "bd", "uuid"},
            new String[] {
              String.valueOf(Integer.MIN_VALUE),
              String.valueOf(Long.MIN_VALUE),
              String.valueOf(Short.MIN_VALUE),
              "0",
              "0",
              "false",
              "0",
              "00000000-0000-0000-0000-000000000000"
            });

    AllNullableBean bean2 = mapper.map(r2);

    assertEquals(Integer.MIN_VALUE, bean2.getI());
    assertEquals(Long.MIN_VALUE, bean2.getL());
    assertEquals(Short.MIN_VALUE, bean2.getS());
  }

  public static class AllNumericBean {

    private int i;
    private long l;
    private short s;
    private float f;
    private double d;
    private BigDecimal bd;

    public AllNumericBean() {}

    public int getI() {
      return i;
    }

    public void setI(int i) {
      this.i = i;
    }

    public long getL() {
      return l;
    }

    public void setL(long l) {
      this.l = l;
    }

    public short getS() {
      return s;
    }

    public void setS(short s) {
      this.s = s;
    }

    public float getF() {
      return f;
    }

    public void setF(float f) {
      this.f = f;
    }

    public double getD() {
      return d;
    }

    public void setD(double d) {
      this.d = d;
    }

    public BigDecimal getBd() {
      return bd;
    }

    public void setBd(BigDecimal bd) {
      this.bd = bd;
    }
  }

  @Test
  void negativeNumbers() {
    RowMapper<AllNumericBean> mapper = JavaBeanRowMapper.of(AllNumericBean.class);
    Row r =
        row(
            new String[] {"i", "l", "s", "f", "d", "bd"},
            new String[] {"-1", "-100", "-5", "-1.5", "-2.5", "-99.99"});

    AllNumericBean bean = mapper.map(r);

    assertEquals(-1, bean.getI());
    assertEquals(-100L, bean.getL());
    assertEquals((short) -5, bean.getS());
    assertEquals(-1.5f, bean.getF());
    assertEquals(-2.5, bean.getD());
    assertEquals(new BigDecimal("-99.99"), bean.getBd());
  }

  // --- Special characters ---

  @Test
  void unicodeInStrings() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r =
        row(
            new String[] {"id", "name", "email"},
            new String[] {"1", "\uD83D\uDE00\u4F60\u597D\u00E9", "user@example.com"});

    SimpleUser user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("\uD83D\uDE00\u4F60\u597D\u00E9", user.getName());
    assertEquals("user@example.com", user.getEmail());
  }

  @Test
  void specialSqlCharacters() {
    RowMapper<SimpleUser> mapper = JavaBeanRowMapper.of(SimpleUser.class);
    Row r =
        row(
            new String[] {"id", "name", "email"},
            new String[] {"1", "O'Reilly\\; DROP TABLE--", "user@example.com"});

    SimpleUser user = mapper.map(r);

    assertEquals(1, user.getId());
    assertEquals("O'Reilly\\; DROP TABLE--", user.getName());
    assertEquals("user@example.com", user.getEmail());
  }

  // --- Column position tracking ---

  public static class FullAddress {

    private String street;
    private String city;
    private String state;
    private String zip;

    public FullAddress() {}

    public String getStreet() {
      return street;
    }

    public void setStreet(String street) {
      this.street = street;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public String getZip() {
      return zip;
    }

    public void setZip(String zip) {
      this.zip = zip;
    }
  }

  public static class UserWithFullAddress {

    private int id;
    private String name;
    private FullAddress address;
    private String phone;

    public UserWithFullAddress() {}

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public FullAddress getAddress() {
      return address;
    }

    public void setAddress(FullAddress address) {
      this.address = address;
    }

    public String getPhone() {
      return phone;
    }

    public void setPhone(String phone) {
      this.phone = phone;
    }
  }

  @Test
  void nestedBeanColumnPositionTracking() {
    RowMapper<UserWithFullAddress> mapper = JavaBeanRowMapper.of(UserWithFullAddress.class);
    Row r =
        row(
            new String[] {"id", "name", "street", "city", "state", "zip", "phone"},
            new String[] {"42", "Alice", "123 Main St", "Springfield", "IL", "62704", "555-1234"});

    UserWithFullAddress user = mapper.map(r);

    assertEquals(42, user.getId());
    assertEquals("Alice", user.getName());
    assertNotNull(user.getAddress());
    assertEquals("123 Main St", user.getAddress().getStreet());
    assertEquals("Springfield", user.getAddress().getCity());
    assertEquals("IL", user.getAddress().getState());
    assertEquals("62704", user.getAddress().getZip());
    assertEquals("555-1234", user.getPhone());
  }
}
