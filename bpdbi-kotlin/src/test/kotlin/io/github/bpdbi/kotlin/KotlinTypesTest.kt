package io.github.bpdbi.kotlin

import io.github.bpdbi.core.TypeRegistry
import io.github.bpdbi.core.test.StubBinaryCodec
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.uuid.ExperimentalUuidApi

/** Tests that registerKotlinTypes() correctly registers encoders and decoders for all Kotlin types. */
class KotlinTypesTest {

    private val registry = TypeRegistry().registerKotlinTypes()

    // --- Encoding (domain → standard) ---

    @OptIn(ExperimentalUuidApi::class)
    @Test
    fun `encode kotlin Uuid to java UUID`() {
        val kotlinUuid = kotlin.uuid.Uuid.parse("550e8400-e29b-41d4-a716-446655440000")
        val encoded = registry.encode(kotlinUuid)
        assertEquals(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), encoded)
    }

    @Test
    fun `encode kotlin Instant to java Instant`() {
        val kotlinInstant = kotlin.time.Instant.fromEpochSeconds(1700000000, 123456789)
        val encoded = registry.encode(kotlinInstant)
        val javaInstant = encoded as java.time.Instant
        assertEquals(1700000000L, javaInstant.epochSecond)
        assertEquals(123456789, javaInstant.nano)
    }

    @Test
    fun `encode kotlinx LocalDate to java LocalDate`() {
        val kDate = LocalDate(2024, 6, 15)
        val encoded = registry.encode(kDate)
        assertEquals(java.time.LocalDate.of(2024, 6, 15), encoded)
    }

    @Test
    fun `encode kotlinx LocalDateTime to java LocalDateTime`() {
        val kdt = LocalDateTime(2024, 6, 15, 10, 30, 0)
        val encoded = registry.encode(kdt)
        assertEquals(java.time.LocalDateTime.of(2024, 6, 15, 10, 30, 0), encoded)
    }

    @Test
    fun `encode kotlinx LocalTime to java LocalTime`() {
        val kt = LocalTime(14, 30, 45)
        val encoded = registry.encode(kt)
        assertEquals(java.time.LocalTime.of(14, 30, 45), encoded)
    }

    @Test
    fun `encode UInt to Long`() {
        val v = 42u
        val encoded = registry.encode(v)
        assertEquals(42L, encoded)
    }

    @Test
    fun `encode ULong to Long`() {
        val v = 123uL
        val encoded = registry.encode(v)
        assertEquals(123L, encoded)
    }

    // --- Decoding (standard → domain) via TypeRegistry.decode ---

    @OptIn(ExperimentalUuidApi::class)
    @Test
    fun `decode java UUID to kotlin Uuid`() {
        val codec = StubBinaryCodec.INSTANCE
        assertTrue(registry.canDecode(kotlin.uuid.Uuid::class.java))
        val javaUuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        val bytes = javaUuid.toString().toByteArray()
        val result = registry.decode(kotlin.uuid.Uuid::class.java, codec, bytes, 0, bytes.size)
        assertEquals(kotlin.uuid.Uuid.parse("550e8400-e29b-41d4-a716-446655440000"), result)
    }

    @Test
    fun `decode java Instant to kotlin Instant`() {
        val codec = StubBinaryCodec.INSTANCE
        assertTrue(registry.canDecode(kotlin.time.Instant::class.java))
        // StubBinaryCodec decodes timestamptz from text: "2024-06-15T10:30:00Z"
        val text = "2024-06-15T10:30:00Z"
        val bytes = text.toByteArray()
        val result = registry.decode(kotlin.time.Instant::class.java, codec, bytes, 0, bytes.size)
        assertNotNull(result)
    }

    @Test
    fun `decode java LocalDate to kotlinx LocalDate`() {
        val codec = StubBinaryCodec.INSTANCE
        assertTrue(registry.canDecode(LocalDate::class.java))
        val text = "2024-06-15"
        val bytes = text.toByteArray()
        val result = registry.decode(LocalDate::class.java, codec, bytes, 0, bytes.size)
        assertEquals(LocalDate(2024, 6, 15), result)
    }

    @Test
    fun `decode java LocalDateTime to kotlinx LocalDateTime`() {
        val codec = StubBinaryCodec.INSTANCE
        assertTrue(registry.canDecode(LocalDateTime::class.java))
        val text = "2024-06-15T10:30:00"
        val bytes = text.toByteArray()
        val result = registry.decode(LocalDateTime::class.java, codec, bytes, 0, bytes.size)
        assertEquals(LocalDateTime(2024, 6, 15, 10, 30, 0), result)
    }

    @Test
    fun `decode java LocalTime to kotlinx LocalTime`() {
        val codec = StubBinaryCodec.INSTANCE
        assertTrue(registry.canDecode(LocalTime::class.java))
        val text = "14:30:45"
        val bytes = text.toByteArray()
        val result = registry.decode(LocalTime::class.java, codec, bytes, 0, bytes.size)
        assertEquals(LocalTime(14, 30, 45), result)
    }

    @Test
    fun `hasEncoders returns true after registration`() {
        assertTrue(registry.hasEncoders())
    }

    @Test
    fun `encode null returns null`() {
        assertEquals(null, registry.encode(null))
    }

    @Test
    fun `encode unregistered type returns unchanged`() {
        val value = "just a string"
        assertEquals(value, registry.encode(value))
    }
}
