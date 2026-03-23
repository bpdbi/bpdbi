package io.github.bpdbi.kotlin

import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Tests demonstrating nested data class and collection support in the Kotlin mapper.
 *
 * WORKS:
 * - Nested data classes (flattened from consecutive columns)
 * - Nullable nested data classes (null when all columns are NULL)
 * - Deeply nested (multi-level) data classes
 * - Lists from a JSON column via @SqlJsonValue
 * - Lists of nested objects from a JSON column via @SqlJsonValue
 *
 * DOES NOT WORK:
 * - List<DataClass> as a field mapped from multiple rows (each row = one item)
 * - Set<T> fields (no special handling for StructureKind.MAP)
 */
class NestedDecoderTest {

  // --- Data classes ---

  @Serializable
  data class Address(val street: String, val city: String)

  @Serializable
  data class UserWithAddress(val id: Int, val name: String, val address: Address)

  @Serializable
  data class UserWithNullableAddress(val id: Int, val name: String, val address: Address?)

  @Serializable
  data class Coordinate(val lat: Double, val lng: Double)

  @Serializable
  data class Location(val name: String, val coord: Coordinate)

  @Serializable
  data class Trip(val id: Int, val origin: Location, val destination: Location)

  @Serializable
  data class WithJsonList(val id: Int, @SqlJsonValue val tags: List<String>)

  @Serializable
  data class Tag(val name: String, val priority: Int)

  @Serializable
  data class WithJsonObjectList(val id: Int, @SqlJsonValue val tags: List<Tag>)

  @Serializable
  data class WithStringList(val id: Int, val tags: List<String>)

  @Serializable
  data class WithStringSet(val id: Int, val tags: Set<String>)

  @Serializable
  data class WithIntList(val id: Int, val numbers: List<Int>)

  @Serializable
  data class NestedInNullable(val id: Int, val home: Address?, val work: Address?)

  @Serializable
  data class WithLongList(val id: Int, val numbers: List<Long>)

  @Serializable
  data class WithDoubleList(val id: Int, val values: List<Double>)

  @Serializable
  data class WithFloatList(val id: Int, val values: List<Float>)

  @Serializable
  data class WithBoolList(val id: Int, val flags: List<Boolean>)

  @Serializable
  data class WithShortList(val id: Int, val values: List<Short>)


  // =========================================================================
  // WORKING: Nested data classes
  // =========================================================================

  @Test
  fun `nested data class - columns are consumed in order`() {
    // Columns: id, name, street, city
    val r = testRow("1", "Alice", "123 Main St", "Springfield")
    val result = serializer<UserWithAddress>().deserialize(RowDecoder(r))
    assertEquals(UserWithAddress(1, "Alice", Address("123 Main St", "Springfield")), result)
  }

  @Test
  fun `nullable nested data class - present`() {
    val r = testRow("1", "Alice", "123 Main St", "Springfield")
    val result = serializer<UserWithNullableAddress>().deserialize(RowDecoder(r))
    assertEquals(
      UserWithNullableAddress(1, "Alice", Address("123 Main St", "Springfield")),
      result
    )
  }

  @Test
  fun `nullable nested data class - null when all columns null`() {
    val r = testRow("1", "Alice", null, null)
    val result = serializer<UserWithNullableAddress>().deserialize(RowDecoder(r))
    assertEquals(UserWithNullableAddress(1, "Alice", null), result)
  }

  @Test
  fun `deeply nested - three levels`() {
    // Trip(id, origin: Location(name, coord: Coordinate(lat, lng)), destination: Location(name, coord: Coordinate(lat, lng)))
    // Columns: id, origin_name, origin_lat, origin_lng, dest_name, dest_lat, dest_lng
    val r = testRow("1", "Home", "52.370", "4.890", "Office", "52.350", "4.910")
    val result = serializer<Trip>().deserialize(RowDecoder(r))
    assertEquals(
      Trip(
        1,
        Location("Home", Coordinate(52.370, 4.890)),
        Location("Office", Coordinate(52.350, 4.910))
      ),
      result
    )
  }

  @Test
  fun `multiple nullable nested - both present`() {
    val r = testRow("1", "Main St", "CityA", "Work Ave", "CityB")
    val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
    assertEquals(
      NestedInNullable(1, Address("Main St", "CityA"), Address("Work Ave", "CityB")),
      result
    )
  }

  @Test
  fun `multiple nullable nested - one null`() {
    val r = testRow("1", null, null, "Work Ave", "CityB")
    val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
    assertEquals(NestedInNullable(1, null, Address("Work Ave", "CityB")), result)
  }

  @Test
  fun `multiple nullable nested - both null`() {
    val r = testRow("1", null, null, null, null)
    val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
    assertEquals(NestedInNullable(1, null, null), result)
  }

  // =========================================================================
  // WORKING: Lists via @SqlJsonValue (JSON column)
  // =========================================================================

  @Test
  fun `list of strings from JSON column`() {
    val r = testRow("1", """["alpha","beta","gamma"]""")
    val result = serializer<WithJsonList>().deserialize(RowDecoder(r))
    assertEquals(WithJsonList(1, listOf("alpha", "beta", "gamma")), result)
  }

  @Test
  fun `list of nested objects from JSON column`() {
    val r = testRow("1", """[{"name":"urgent","priority":1},{"name":"low","priority":5}]""")
    val result = serializer<WithJsonObjectList>().deserialize(RowDecoder(r))
    assertEquals(
      WithJsonObjectList(1, listOf(Tag("urgent", 1), Tag("low", 5))),
      result
    )
  }

  // =========================================================================
  // List<String> from JSON array format
  // =========================================================================

  @Test
  fun `list of strings from JSON array format`() {
    val r = testRow("1", """["foo","bar","baz"]""")
    val result = serializer<WithStringList>().deserialize(RowDecoder(r))
    assertEquals(WithStringList(1, listOf("foo", "bar", "baz")), result)
  }

  @Test
  fun `list of ints from JSON array`() {
    val r = testRow("1", "[1,2,3]")
    val result = serializer<WithIntList>().deserialize(RowDecoder(r))
    assertEquals(WithIntList(1, listOf(1, 2, 3)), result)
  }

  @Test
  fun `set of strings from JSON array`() {
    val r = testRow("1", """["foo","bar","baz"]""")
    val result = serializer<WithStringSet>().deserialize(RowDecoder(r))
    assertEquals(WithStringSet(1, setOf("foo", "bar", "baz")), result)
  }

}
