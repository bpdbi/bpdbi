package io.github.bpdbi.kotlin

import io.github.bpdbi.core.ColumnDescriptor
import io.github.bpdbi.core.Row
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

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
 * - List<Int>/List<Long> from a PG array column (parsePgArray returns List<String>)
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

    // --- Helpers ---

    private fun col(name: String) = ColumnDescriptor(name, 0, 0, 0, 0, 0)

    private fun row(vararg values: String?): Row {
        val columns = values.mapIndexed { i, _ -> col("col$i") }.toTypedArray()
        val byteValues: Array<ByteArray?> =
            values.map { it?.toByteArray(Charsets.UTF_8) }.toTypedArray()
        @Suppress("UNCHECKED_CAST")
        return Row(columns, byteValues as Array<ByteArray>, null, null)
    }

    // =========================================================================
    // WORKING: Nested data classes
    // =========================================================================

    @Test
    fun `nested data class - columns are consumed in order`() {
        // Columns: id, name, street, city
        val r = row("1", "Alice", "123 Main St", "Springfield")
        val result = serializer<UserWithAddress>().deserialize(RowDecoder(r))
        assertEquals(UserWithAddress(1, "Alice", Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `nullable nested data class - present`() {
        val r = row("1", "Alice", "123 Main St", "Springfield")
        val result = serializer<UserWithNullableAddress>().deserialize(RowDecoder(r))
        assertEquals(
            UserWithNullableAddress(1, "Alice", Address("123 Main St", "Springfield")),
            result
        )
    }

    @Test
    fun `nullable nested data class - null when all columns null`() {
        val r = row("1", "Alice", null, null)
        val result = serializer<UserWithNullableAddress>().deserialize(RowDecoder(r))
        assertEquals(UserWithNullableAddress(1, "Alice", null), result)
    }

    @Test
    fun `deeply nested - three levels`() {
        // Trip(id, origin: Location(name, coord: Coordinate(lat, lng)), destination: Location(name, coord: Coordinate(lat, lng)))
        // Columns: id, origin_name, origin_lat, origin_lng, dest_name, dest_lat, dest_lng
        val r = row("1", "Home", "52.37", "4.89", "Office", "52.35", "4.91")
        val result = serializer<Trip>().deserialize(RowDecoder(r))
        assertEquals(
            Trip(
                1,
                Location("Home", Coordinate(52.37, 4.89)),
                Location("Office", Coordinate(52.35, 4.91))
            ),
            result
        )
    }

    @Test
    fun `multiple nullable nested - both present`() {
        val r = row("1", "Main St", "CityA", "Work Ave", "CityB")
        val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
        assertEquals(
            NestedInNullable(1, Address("Main St", "CityA"), Address("Work Ave", "CityB")),
            result
        )
    }

    @Test
    fun `multiple nullable nested - one null`() {
        val r = row("1", null, null, "Work Ave", "CityB")
        val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
        assertEquals(NestedInNullable(1, null, Address("Work Ave", "CityB")), result)
    }

    @Test
    fun `multiple nullable nested - both null`() {
        val r = row("1", null, null, null, null)
        val result = serializer<NestedInNullable>().deserialize(RowDecoder(r))
        assertEquals(NestedInNullable(1, null, null), result)
    }

    // =========================================================================
    // WORKING: Lists via @SqlJsonValue (JSON column)
    // =========================================================================

    @Test
    fun `list of strings from JSON column`() {
        val r = row("1", """["alpha","beta","gamma"]""")
        val result = serializer<WithJsonList>().deserialize(RowDecoder(r))
        assertEquals(WithJsonList(1, listOf("alpha", "beta", "gamma")), result)
    }

    @Test
    fun `list of nested objects from JSON column`() {
        val r = row("1", """[{"name":"urgent","priority":1},{"name":"low","priority":5}]""")
        val result = serializer<WithJsonObjectList>().deserialize(RowDecoder(r))
        assertEquals(
            WithJsonObjectList(1, listOf(Tag("urgent", 1), Tag("low", 5))),
            result
        )
    }

    // =========================================================================
    // WORKING: List<String> from PG array format
    // =========================================================================

    @Test
    fun `list of strings from PG array format`() {
        val r = row("1", "{foo,bar,baz}")
        val result = serializer<WithStringList>().deserialize(RowDecoder(r))
        assertEquals(WithStringList(1, listOf("foo", "bar", "baz")), result)
    }

    @Test
    fun `list of strings from JSON array format`() {
        val r = row("1", """["foo","bar","baz"]""")
        val result = serializer<WithStringList>().deserialize(RowDecoder(r))
        assertEquals(WithStringList(1, listOf("foo", "bar", "baz")), result)
    }

    // =========================================================================
    // Typed arrays from PG text format — all element types
    // =========================================================================

    @Test
    fun `list of ints from PG array`() {
        val r = row("1", "{1,2,3}")
        val result = serializer<WithIntList>().deserialize(RowDecoder(r))
        assertEquals(WithIntList(1, listOf(1, 2, 3)), result)
        // Verify they are actual Ints, not Strings
        assertEquals(6, result.numbers.sum())
    }

    @Test
    fun `list of ints from JSON array`() {
        val r = row("1", "[1,2,3]")
        val result = serializer<WithIntList>().deserialize(RowDecoder(r))
        assertEquals(WithIntList(1, listOf(1, 2, 3)), result)
    }

    @Test
    fun `list of longs from PG array`() {
        val r = row("1", "{100,200,300}")
        val result = serializer<WithLongList>().deserialize(RowDecoder(r))
        assertEquals(WithLongList(1, listOf(100L, 200L, 300L)), result)
        assertEquals(600L, result.numbers.sum())
    }

    @Test
    fun `list of doubles from PG array`() {
        val r = row("1", "{1.5,2.5,3.0}")
        val result = serializer<WithDoubleList>().deserialize(RowDecoder(r))
        assertEquals(WithDoubleList(1, listOf(1.5, 2.5, 3.0)), result)
    }

    @Test
    fun `list of floats from PG array`() {
        val r = row("1", "{1.5,2.5}")
        val result = serializer<WithFloatList>().deserialize(RowDecoder(r))
        assertEquals(WithFloatList(1, listOf(1.5f, 2.5f)), result)
    }

    @Test
    fun `list of booleans from PG array`() {
        val r = row("1", "{t,f,true,false}")
        val result = serializer<WithBoolList>().deserialize(RowDecoder(r))
        assertEquals(WithBoolList(1, listOf(true, false, true, false)), result)
    }

    @Test
    fun `list of shorts from PG array`() {
        val r = row("1", "{10,20,30}")
        val result = serializer<WithShortList>().deserialize(RowDecoder(r))
        assertEquals(WithShortList(1, listOf(10.toShort(), 20.toShort(), 30.toShort())), result)
    }

    @Test
    fun `set of strings from PG array`() {
        val r = row("1", "{foo,bar,baz}")
        val result = serializer<WithStringSet>().deserialize(RowDecoder(r))
        assertEquals(WithStringSet(1, setOf("foo", "bar", "baz")), result)
    }

    @Test
    fun `set of strings from JSON array`() {
        val r = row("1", """["foo","bar","baz"]""")
        val result = serializer<WithStringSet>().deserialize(RowDecoder(r))
        assertEquals(WithStringSet(1, setOf("foo", "bar", "baz")), result)
    }

    @Test
    fun `empty PG array`() {
        val r = row("1", "{}")
        val result = serializer<WithIntList>().deserialize(RowDecoder(r))
        assertEquals(WithIntList(1, emptyList()), result)
    }
}
