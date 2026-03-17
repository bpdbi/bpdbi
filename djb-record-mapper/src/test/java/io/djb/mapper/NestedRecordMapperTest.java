package io.djb.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.djb.ColumnDescriptor;
import io.djb.Row;
import io.djb.RowMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests demonstrating nested record support in RecordRowMapper. Nested records are flattened: their
 * components consume consecutive columns.
 */
class NestedRecordRowMapperTest {

  // --- Test records ---

  record Address(String street, String city) {}

  record UserWithAddress(int id, String name, Address address) {}

  record Coordinate(double lat, double lng) {}

  record Location(String name, Coordinate coord) {}

  record Trip(int id, Location origin, Location destination) {}

  record WithList(int id, List<String> tags) {}

  private static Row row(String[] columnNames, String[] values) {
    ColumnDescriptor[] cols = new ColumnDescriptor[columnNames.length];
    byte[][] rawValues = new byte[values.length][];
    for (int i = 0; i < columnNames.length; i++) {
      cols[i] = new ColumnDescriptor(columnNames[i], 0, (short) 0, 0, (short) 0, 0);
      rawValues[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
    }
    return new Row(cols, rawValues, null, null);
  }

  @Test
  void nestedRecordConsumesFlatColumns() {
    // Columns: id, name, street, city
    RowMapper<UserWithAddress> mapper = RecordRowMapper.of(UserWithAddress.class);
    Row r =
        row(
            new String[] {"id", "name", "street", "city"},
            new String[] {"1", "Alice", "123 Main St", "Springfield"});

    UserWithAddress user = mapper.map(r);

    assertEquals(1, user.id());
    assertEquals("Alice", user.name());
    assertEquals(new Address("123 Main St", "Springfield"), user.address());
  }

  @Test
  void deeplyNestedThreeLevels() {
    // Trip(id, origin: Location(name, coord: Coordinate(lat, lng)), destination: Location(name,
    // coord: Coordinate(lat, lng)))
    // Columns: id, origin_name, origin_lat, origin_lng, dest_name, dest_lat, dest_lng
    RowMapper<Trip> mapper = RecordRowMapper.of(Trip.class);
    Row r =
        row(
            new String[] {"id", "name", "lat", "lng", "name", "lat", "lng"},
            new String[] {"1", "Home", "52.37", "4.89", "Office", "52.35", "4.91"});

    Trip trip = mapper.map(r);

    assertEquals(1, trip.id());
    assertEquals(new Location("Home", new Coordinate(52.37, 4.89)), trip.origin());
    assertEquals(new Location("Office", new Coordinate(52.35, 4.91)), trip.destination());
  }

  @Test
  void nestedRecordWithNullScalarFields() {
    RowMapper<UserWithAddress> mapper = RecordRowMapper.of(UserWithAddress.class);
    Row r =
        row(new String[] {"id", "name", "street", "city"}, new String[] {"1", "Alice", null, null});

    UserWithAddress user = mapper.map(r);

    assertEquals(1, user.id());
    assertEquals("Alice", user.name());
    // Nested record is still constructed, but with null fields
    assertEquals(new Address(null, null), user.address());
  }

  @Test
  void reuseMapperWithNestedRecords() {
    RowMapper<UserWithAddress> mapper = RecordRowMapper.of(UserWithAddress.class);

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

    assertEquals(new Address("Main St", "CityA"), u1.address());
    assertEquals(new Address("Oak Ave", "CityB"), u2.address());
  }

  @Test
  void listComponentIsStillRejected() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RecordRowMapper.of(WithList.class));
    assertTrue(ex.getMessage().contains("Unsupported type"));
  }
}
