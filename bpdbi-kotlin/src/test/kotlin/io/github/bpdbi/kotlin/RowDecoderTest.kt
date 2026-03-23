package io.github.bpdbi.kotlin

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.serializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/** Unit tests for RowDecoder using mock Rows (no database needed). */
class RowDecoderTest {

    // --- Test data classes ---

    @Serializable
    data class Simple(val id: Int, val name: String, val active: Boolean)

    @Serializable
    data class AllPrimitives(
        val b: Boolean,
        val s: Short,
        val i: Int,
        val l: Long,
        val f: Float,
        val d: Double,
        val str: String
    )

    @Serializable
    data class WithNullable(val id: Int, val name: String?)

    @Serializable
    data class Nested(val id: Int, val address: Address)

    @Serializable
    data class Address(val street: String, val city: String)

    @Serializable
    data class WithNullableNested(val id: Int, val address: Address?)

    @Serializable
    data class WithJson(val id: Int, @SqlJsonValue val meta: Meta)

    @Serializable
    data class Meta(val source: String, val priority: Int)

    @Serializable
    data class WithNullableJson(val id: Int, @SqlJsonValue val meta: Meta?)

    @Serializable
    enum class Color { RED, GREEN, BLUE }

    @Serializable
    data class WithEnum(val id: Int, val color: Color)

    // --- Value classes (JVM inline) ---

    @Serializable
    @JvmInline
    value class UserId(val value: Int)

    @Serializable
    @JvmInline
    value class Email(val value: String)

    @Serializable
    data class UserWithValueClasses(val id: UserId, val email: Email, val name: String)

    @Serializable
    data class UserWithNullableValueClass(val id: UserId, val email: Email?)

    // --- Deeply nested ---

    @Serializable
    data class Country(val name: String, val code: String)

    @Serializable
    data class FullAddress(val street: String, val city: String, val country: Country)

    @Serializable
    data class DeeplyNested(val id: Int, val address: FullAddress)

    // --- kotlinx.datetime types ---

    @Serializable
    data class WithDateTimeTypes(
        val id: Int,
        val date: LocalDate,
        val time: LocalTime,
        val dateTime: LocalDateTime
    )

    @Serializable
    data class WithNullableDateTime(
        val id: Int,
        val date: LocalDate?,
        val time: LocalTime?,
        val dateTime: LocalDateTime?
    )

    // --- Multiple nullable nested ---

    @Serializable
    data class Phone(val number: String, val ext: String)

    @Serializable
    data class TwoOptionalNested(val id: Int, val address: Address?, val phone: Phone?)

    // --- Tests ---

    @Test
    fun `decode simple data class`() {
        val r = testRow("42", "Alice", "t")
        val result = serializer<Simple>().deserialize(RowDecoder(r))
        assertEquals(Simple(42, "Alice", true), result)
    }

    @Test
    fun `decode all primitive types`() {
        val r = testRow("true", "7", "42", "123456789", "1.5", "3.140", "hello")
        val result = serializer<AllPrimitives>().deserialize(RowDecoder(r))
        assertEquals(AllPrimitives(true, 7, 42, 123456789L, 1.5f, 3.140, "hello"), result)
    }

    @Test
    fun `decode nullable field with value`() {
        val r = testRow("1", "Alice")
        val result = serializer<WithNullable>().deserialize(RowDecoder(r))
        assertEquals(WithNullable(1, "Alice"), result)
    }

    @Test
    fun `decode nullable field with null`() {
        val r = testRow("1", null)
        val result = serializer<WithNullable>().deserialize(RowDecoder(r))
        assertEquals(WithNullable(1, null), result)
    }

    @Test
    fun `null for non-nullable field throws`() {
        val r = testRow(null, "Alice", "t")
        assertThrows<SerializationException> {
            serializer<Simple>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `decode nested data class`() {
        val r = testRow("1", "123 Main St", "Springfield")
        val result = serializer<Nested>().deserialize(RowDecoder(r))
        assertEquals(Nested(1, Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `decode nullable nested with values`() {
        val r = testRow("1", "123 Main St", "Springfield")
        val result = serializer<WithNullableNested>().deserialize(RowDecoder(r))
        assertEquals(WithNullableNested(1, Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `decode nullable nested all null`() {
        val r = testRow("1", null, null)
        val result = serializer<WithNullableNested>().deserialize(RowDecoder(r))
        assertEquals(WithNullableNested(1, null), result)
    }

    @Test
    fun `decode SqlJsonValue field`() {
        val r = testRow("1", """{"source":"web","priority":5}""")
        val result = serializer<WithJson>().deserialize(RowDecoder(r))
        assertEquals(WithJson(1, Meta("web", 5)), result)
    }

    @Test
    fun `decode nullable SqlJsonValue with value`() {
        val r = testRow("1", """{"source":"api","priority":3}""")
        val result = serializer<WithNullableJson>().deserialize(RowDecoder(r))
        assertEquals(WithNullableJson(1, Meta("api", 3)), result)
    }

    @Test
    fun `decode nullable SqlJsonValue with null`() {
        val r = testRow("1", null)
        val result = serializer<WithNullableJson>().deserialize(RowDecoder(r))
        assertEquals(WithNullableJson(1, null), result)
    }

    @Test
    fun `decode enum`() {
        val r = testRow("1", "GREEN")
        val result = serializer<WithEnum>().deserialize(RowDecoder(r))
        assertEquals(WithEnum(1, Color.GREEN), result)
    }

    @Test
    fun `unknown enum value throws`() {
        val r = testRow("1", "PURPLE")
        assertThrows<SerializationException> {
            serializer<WithEnum>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `decode Instant from PG timestamp with timezone`() {
        val value = "2026-02-04 04:31:16.935337+01"
        val instant = parseInstant(value)
        assertNotNull(instant)
    }

    @Test
    fun `decode Instant from ISO format`() {
        val value = "2026-02-04T04:31:16.935337Z"
        val instant = parseInstant(value)
        assertNotNull(instant)
    }

    @Test
    fun `decode Instant without timezone assumes UTC`() {
        val value = "2026-02-04 04:31:16.935337"
        val instant = parseInstant(value)
        assertNotNull(instant)
    }

    // --- Value class tests ---

    @Test
    fun `decode value class fields`() {
        val r = testRow("42", "alice@x.com", "Alice")
        val result = serializer<UserWithValueClasses>().deserialize(RowDecoder(r))
        assertEquals(UserWithValueClasses(UserId(42), Email("alice@x.com"), "Alice"), result)
    }

    @Test
    fun `decode nullable value class with value`() {
        val r = testRow("7", "bob@y.org")
        val result = serializer<UserWithNullableValueClass>().deserialize(RowDecoder(r))
        assertEquals(UserWithNullableValueClass(UserId(7), Email("bob@y.org")), result)
    }

    @Test
    fun `decode nullable value class with null`() {
        val r = testRow("7", null)
        val result = serializer<UserWithNullableValueClass>().deserialize(RowDecoder(r))
        assertEquals(UserWithNullableValueClass(UserId(7), null), result)
    }

    // --- Deeply nested tests ---

    @Test
    fun `decode deeply nested three levels`() {
        val r = testRow("1", "123 Main St", "Springfield", "USA", "US")
        val result = serializer<DeeplyNested>().deserialize(RowDecoder(r))
        assertEquals(
            DeeplyNested(1, FullAddress("123 Main St", "Springfield", Country("USA", "US"))),
            result
        )
    }

    // --- Multiple nullable nested tests ---

    @Test
    fun `multiple nullable nested - both null`() {
        val r = testRow("1", null, null, null, null)
        val result = serializer<TwoOptionalNested>().deserialize(RowDecoder(r))
        assertEquals(TwoOptionalNested(1, null, null), result)
    }

    @Test
    fun `multiple nullable nested - one null one present`() {
        val r = testRow("1", null, null, "555-1234", "x100")
        val result = serializer<TwoOptionalNested>().deserialize(RowDecoder(r))
        assertEquals(TwoOptionalNested(1, null, Phone("555-1234", "x100")), result)
    }

    // --- Unicode and special characters ---

    @Test
    fun `unicode and special characters`() {
        val r = testRow("1", "\uD83D\uDE00 hello \u4F60\u597D")
        val result = serializer<WithNullable>().deserialize(RowDecoder(r))
        assertEquals(WithNullable(1, "\uD83D\uDE00 hello \u4F60\u597D"), result)
    }

    // --- Extreme numeric values ---

    // --- kotlinx.datetime tests ---

    @Test
    fun `decode kotlinx datetime types`() {
        val r = testRow("1", "2024-06-15", "14:30:00", "2024-06-15T14:30:00")
        val result = serializer<WithDateTimeTypes>().deserialize(RowDecoder(r))
        assertEquals(
            WithDateTimeTypes(
                1,
                LocalDate(2024, 6, 15),
                LocalTime(14, 30, 0),
                LocalDateTime(2024, 6, 15, 14, 30, 0)
            ),
            result
        )
    }

    @Test
    fun `decode LocalDateTime with space separator`() {
        val r = testRow("1", "2024-06-15", "14:30:00", "2024-06-15 14:30:00")
        val result = serializer<WithDateTimeTypes>().deserialize(RowDecoder(r))
        assertEquals(LocalDateTime(2024, 6, 15, 14, 30, 0), result.dateTime)
    }

    @Test
    fun `decode nullable kotlinx datetime with values`() {
        val r = testRow("1", "2024-06-15", "14:30:00", "2024-06-15T14:30:00")
        val result = serializer<WithNullableDateTime>().deserialize(RowDecoder(r))
        assertEquals(LocalDate(2024, 6, 15), result.date)
        assertEquals(LocalTime(14, 30, 0), result.time)
        assertEquals(LocalDateTime(2024, 6, 15, 14, 30, 0), result.dateTime)
    }

    @Test
    fun `decode nullable kotlinx datetime with nulls`() {
        val r = testRow("1", null, null, null)
        val result = serializer<WithNullableDateTime>().deserialize(RowDecoder(r))
        assertEquals(1, result.id)
        assertNull(result.date)
        assertNull(result.time)
        assertNull(result.dateTime)
    }

    // --- Extreme values ---

    @Test
    fun `extreme numeric values`() {
        val r = testRow(
            "true",
            "${Short.MAX_VALUE}",
            "${Int.MAX_VALUE}",
            "${Long.MIN_VALUE}",
            "${Float.MAX_VALUE}",
            "${Double.MIN_VALUE}",
            "edge"
        )
        val result = serializer<AllPrimitives>().deserialize(RowDecoder(r))
        assertEquals(
            AllPrimitives(
                b = true,
                s = Short.MAX_VALUE,
                i = Int.MAX_VALUE,
                l = Long.MIN_VALUE,
                f = Float.MAX_VALUE,
                d = Double.MIN_VALUE,
                str = "edge"
            ),
            result
        )
    }

    // --- Null handling for non-nullable primitives ---

    @Test
    fun `null for non-nullable int throws`() {
        @Serializable
        data class IntHolder(val value: Int)

        val r = testRow(null)
        assertThrows<SerializationException> {
            serializer<IntHolder>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `null for non-nullable boolean throws`() {
        @Serializable
        data class BoolHolder(val value: Boolean)

        val r = testRow(null)
        assertThrows<SerializationException> {
            serializer<BoolHolder>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `null for non-nullable string throws`() {
        @Serializable
        data class StrHolder(val value: String)

        val r = testRow(null)
        assertThrows<SerializationException> {
            serializer<StrHolder>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `decode single char`() {
        @Serializable
        data class CharHolder(val value: Char)

        val r = testRow("A")
        val result = serializer<CharHolder>().deserialize(RowDecoder(r))
        assertEquals('A', result.value)
    }
}
