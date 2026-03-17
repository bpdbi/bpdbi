package io.github.bpdbi.kotlin

import io.github.bpdbi.core.ColumnDescriptor
import io.github.bpdbi.core.Row
import io.github.bpdbi.core.RowMapper
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

/** Tests for KotlinRowMapper — verifies it implements RowMapper correctly. */
class KotlinRowMapperTest {

    @Serializable
    data class User(val id: Int, val name: String, val email: String)

    @Serializable
    data class Address(val street: String, val city: String)

    @Serializable
    data class UserWithAddress(val id: Int, val name: String, val address: Address)

    @Serializable
    data class WithNullable(val id: Int, val name: String?)

    @Serializable
    data class WithJson(val id: Int, @SqlJsonValue val meta: Meta)

    @Serializable
    data class Meta(val source: String, val priority: Int)

    // --- Helpers ---

    private fun col(name: String) = ColumnDescriptor(name, 0, 0, 0, 0, 0)

    private fun row(vararg values: String?): Row {
        val columns = values.mapIndexed { i, _ -> col("col$i") }.toTypedArray()
        val byteValues: Array<ByteArray?> =
            values.map { it?.toByteArray(Charsets.UTF_8) }.toTypedArray()
        @Suppress("UNCHECKED_CAST") // see RowDecoderTest for explanation
        return Row(columns, byteValues as Array<ByteArray>, null, null)
    }

    // --- Tests ---

    @Test
    fun `implements RowMapper interface`() {
        val mapper: RowMapper<User> = KotlinRowMapper.of<User>()
        val result = mapper.map(row("1", "Alice", "alice@example.com"))
        assertEquals(User(1, "Alice", "alice@example.com"), result)
    }

    @Test
    fun `map simple data class`() {
        val mapper = KotlinRowMapper.of<User>()
        val result = mapper.map(row("42", "Bob", "bob@test.com"))
        assertEquals(User(42, "Bob", "bob@test.com"), result)
    }

    @Test
    fun `map nested data class`() {
        val mapper = KotlinRowMapper.of<UserWithAddress>()
        val result = mapper.map(row("1", "Alice", "123 Main St", "Springfield"))
        assertEquals(UserWithAddress(1, "Alice", Address("123 Main St", "Springfield")), result)
    }

    @Test
    fun `map nullable field with null`() {
        val mapper = KotlinRowMapper.of<WithNullable>()
        val result = mapper.map(row("1", null))
        assertEquals(1, result.id)
        assertNull(result.name)
    }

    @Test
    fun `map json field`() {
        val mapper = KotlinRowMapper.of<WithJson>()
        val result = mapper.map(row("1", """{"source":"web","priority":5}"""))
        assertEquals(WithJson(1, Meta("web", 5)), result)
    }

    @Test
    fun `mapper is reusable across rows`() {
        val mapper = KotlinRowMapper.of<User>()
        val r1 = mapper.map(row("1", "Alice", "alice@x.com"))
        val r2 = mapper.map(row("2", "Bob", "bob@y.com"))
        assertEquals(User(1, "Alice", "alice@x.com"), r1)
        assertEquals(User(2, "Bob", "bob@y.com"), r2)
    }
}
