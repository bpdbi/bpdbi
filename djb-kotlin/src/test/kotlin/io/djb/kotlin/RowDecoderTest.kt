package io.djb.kotlin

import io.djb.ColumnDescriptor
import io.djb.Row
import kotlinx.serialization.SerializationException
import kotlinx.serialization.Serializable
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

    // --- Helpers ---

    private fun col(name: String) = ColumnDescriptor(name, 0, 0, 0, 0, 0)

    private fun row(vararg values: String?): Row {
        val columns = values.mapIndexed { i, _ -> col("col$i") }.toTypedArray()
        val byteValues = values.map { it?.toByteArray(Charsets.UTF_8) }.toTypedArray()
        return Row(columns, byteValues, null, null)
    }

    // --- Tests ---

    @Test
    fun `decode simple data class`() {
        val r = row("42", "Alice", "t")
        val result = serializer<Simple>().deserialize(RowDecoder(r))
        assertEquals(Simple(42, "Alice", true), result)
    }

    @Test
    fun `decode all primitive types`() {
        val r = row("true", "7", "42", "123456789", "1.5", "3.14", "hello")
        val result = serializer<AllPrimitives>().deserialize(RowDecoder(r))
        assertEquals(AllPrimitives(true, 7, 42, 123456789L, 1.5f, 3.14, "hello"), result)
    }

    @Test
    fun `decode nullable field with value`() {
        val r = row("1", "Alice")
        val result = serializer<WithNullable>().deserialize(RowDecoder(r))
        assertEquals(WithNullable(1, "Alice"), result)
    }

    @Test
    fun `decode nullable field with null`() {
        val r = row("1", null)
        val result = serializer<WithNullable>().deserialize(RowDecoder(r))
        assertEquals(WithNullable(1, null), result)
    }

    @Test
    fun `null for non-nullable field throws`() {
        val r = row(null, "Alice", "t")
        assertThrows<SerializationException> {
            serializer<Simple>().deserialize(RowDecoder(r))
        }
    }

    @Test
    fun `decode nested data class`() {
        val r = row("1", "123 Main St", "Springfield")
        val result = serializer<Nested>().deserialize(RowDecoder(r))
        assertEquals(Nested(1, Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `decode nullable nested with values`() {
        val r = row("1", "123 Main St", "Springfield")
        val result = serializer<WithNullableNested>().deserialize(RowDecoder(r))
        assertEquals(WithNullableNested(1, Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `decode nullable nested all null`() {
        val r = row("1", null, null)
        val result = serializer<WithNullableNested>().deserialize(RowDecoder(r))
        assertEquals(WithNullableNested(1, null), result)
    }

    @Test
    fun `decode SqlJsonValue field`() {
        val r = row("1", """{"source":"web","priority":5}""")
        val result = serializer<WithJson>().deserialize(RowDecoder(r))
        assertEquals(WithJson(1, Meta("web", 5)), result)
    }

    @Test
    fun `decode nullable SqlJsonValue with value`() {
        val r = row("1", """{"source":"api","priority":3}""")
        val result = serializer<WithNullableJson>().deserialize(RowDecoder(r))
        assertEquals(WithNullableJson(1, Meta("api", 3)), result)
    }

    @Test
    fun `decode nullable SqlJsonValue with null`() {
        val r = row("1", null)
        val result = serializer<WithNullableJson>().deserialize(RowDecoder(r))
        assertEquals(WithNullableJson(1, null), result)
    }

    @Test
    fun `decode enum`() {
        val r = row("1", "GREEN")
        val result = serializer<WithEnum>().deserialize(RowDecoder(r))
        assertEquals(WithEnum(1, Color.GREEN), result)
    }

    @Test
    fun `unknown enum value throws`() {
        val r = row("1", "PURPLE")
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

    @Test
    fun `parsePgArray empty`() {
        assertEquals(emptyList<String>(), parsePgArray("{}"))
    }

    @Test
    fun `parsePgArray with values`() {
        assertEquals(listOf("a", "b", "c"), parsePgArray("{a,b,c}"))
    }

    @Test
    fun `parsePgArray with quoted values`() {
        assertEquals(listOf("hello world", "foo"), parsePgArray("""{"hello world","foo"}"""))
    }
}
