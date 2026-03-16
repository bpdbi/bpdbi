package io.djb.kotlin

import io.djb.Row
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule

private val defaultJson = Json { ignoreUnknownKeys = true }

/**
 * A kotlinx.serialization [Decoder] that reads from djb's [Row].
 *
 * Since djb stores values as raw bytes (text or binary format) and [Row] already
 * provides typed getters, this is simpler than the JDBC-based version: no type
 * dispatch or `wasNull()` checks needed.
 *
 * Constructor parameters beyond [row] are internal — used when recursing into
 * nested data classes.
 */
@OptIn(ExperimentalSerializationApi::class)
class RowDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule = SerializersModule {},
    private val startColumn: Int = 0,
    private val json: Json = defaultJson,
    private val endCallback: (Int) -> Unit = {}
) : Decoder, CompositeDecoder {

    private var columnIndex = startColumn
    private var elementIndex = 0

    private fun isJsonFieldAnnotated(descriptor: SerialDescriptor, fieldIndex: Int): Boolean =
        descriptor.getElementAnnotations(fieldIndex).any { it is SqlJsonValue }

    // ---------------------------------------------------------
    // Structure
    // ---------------------------------------------------------
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder = this

    override fun endStructure(descriptor: SerialDescriptor) = endCallback(columnIndex)

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (elementIndex < descriptor.elementsCount) elementIndex++ else CompositeDecoder.DECODE_DONE

    // ---------------------------------------------------------
    // Null handling
    // ---------------------------------------------------------
    override fun decodeNotNullMark(): Boolean = !row.isNull(columnIndex)

    override fun decodeNull(): Nothing? {
        columnIndex++
        return null
    }

    private fun <T> handleNullableValue(descriptor: SerialDescriptor, index: Int, orElse: () -> T): T? {
        if (!row.isNull(columnIndex)) return orElse()
        if (descriptor.isElementOptional(index) || descriptor.getElementDescriptor(index).isNullable) {
            columnIndex++
            return null
        }
        throw SerializationException(
            "NULL value encountered for non-nullable field '${descriptor.getElementName(index)}' " +
                "at column $columnIndex"
        )
    }

    // ---------------------------------------------------------
    // Primitive decoders
    // ---------------------------------------------------------
    override fun decodeBoolean(): Boolean = row.getBoolean(columnIndex++) ?: throw nullError()
    override fun decodeByte(): Byte = (row.getShort(columnIndex++) ?: throw nullError()).toByte()
    override fun decodeShort(): Short = row.getShort(columnIndex++) ?: throw nullError()
    override fun decodeInt(): Int = row.getInteger(columnIndex++) ?: throw nullError()
    override fun decodeLong(): Long = row.getLong(columnIndex++) ?: throw nullError()
    override fun decodeFloat(): Float = row.getFloat(columnIndex++) ?: throw nullError()
    override fun decodeDouble(): Double = row.getDouble(columnIndex++) ?: throw nullError()
    override fun decodeChar(): Char = decodeString().singleOrNull()
        ?: throw SerializationException("Expected single character")
    override fun decodeString(): String = row.getString(columnIndex++) ?: throw nullError()
    override fun decodeInline(descriptor: SerialDescriptor): Decoder = this

    private fun nullError(): SerializationException =
        SerializationException("NULL value encountered for non-nullable primitive at column ${columnIndex - 1}")

    // ---------------------------------------------------------
    // Enum
    // ---------------------------------------------------------
    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val value = decodeString()
        val idx = enumDescriptor.getElementIndex(value)
        if (idx == CompositeDecoder.UNKNOWN_NAME)
            throw SerializationException("Unknown enum value '$value'")
        return idx
    }

    // ---------------------------------------------------------
    // CompositeDecoder element dispatch
    // ---------------------------------------------------------
    override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int): Boolean = decodeBoolean()
    override fun decodeByteElement(descriptor: SerialDescriptor, index: Int): Byte = decodeByte()
    override fun decodeShortElement(descriptor: SerialDescriptor, index: Int): Short = decodeShort()
    override fun decodeIntElement(descriptor: SerialDescriptor, index: Int): Int = decodeInt()
    override fun decodeLongElement(descriptor: SerialDescriptor, index: Int): Long = decodeLong()
    override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int): Float = decodeFloat()
    override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int): Double = decodeDouble()
    override fun decodeCharElement(descriptor: SerialDescriptor, index: Int): Char = decodeChar()
    override fun decodeStringElement(descriptor: SerialDescriptor, index: Int): String = decodeString()
    override fun decodeInlineElement(descriptor: SerialDescriptor, index: Int): Decoder = this

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ): T {
        val childDescriptor = descriptor.getElementDescriptor(index)

        // @SqlJsonValue: parse the column as JSON
        if (isJsonFieldAnnotated(descriptor, index)) {
            val jsonString = row.getString(columnIndex++)
                ?: throw SerializationException("JSON column cannot be null for non-nullable field at index $index")
            return json.decodeFromString(deserializer, jsonString)
        }

        // Arrays/lists: parse from PG array text format or JSON
        if (childDescriptor.kind == StructureKind.LIST) {
            val value = row.getString(columnIndex++)
                ?: throw SerializationException("Array column cannot be null for non-nullable field at index $index")
            // Try JSON first if it looks like JSON, otherwise parse PG array format
            return if (value.startsWith("[")) {
                json.decodeFromString(deserializer, value)
            } else {
                // PG text array format: {val1,val2,...}
                @Suppress("UNCHECKED_CAST")
                parsePgArray(value) as T
            }
        }

        // kotlin.time.Instant
        if (childDescriptor.serialName == "kotlin.time.Instant") {
            val value = row.getString(columnIndex++)
                ?: throw SerializationException("Instant column cannot be null at index $index")
            @Suppress("UNCHECKED_CAST")
            return parseInstant(value) as T
        }

        // Nested data classes: flatten columns into nested structure
        if (childDescriptor.kind == StructureKind.CLASS && !childDescriptor.isInline) {
            val nestedDecoder = RowDecoder(row, serializersModule, columnIndex, json) { newIndex ->
                columnIndex = newIndex
            }
            return deserializer.deserialize(nestedDecoder)
        }

        return deserializer.deserialize(this)
    }

    override fun <T : Any> decodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T?>,
        previousValue: T?
    ): T? {
        val childDesc = descriptor.getElementDescriptor(index)

        // @SqlJsonValue
        if (isJsonFieldAnnotated(descriptor, index)) {
            return handleNullableValue(descriptor, index) {
                row.getString(columnIndex++)?.let { json.decodeFromString(deserializer, it) }
            }
        }

        // Arrays/lists
        if (childDesc.kind == StructureKind.LIST) {
            return handleNullableValue(descriptor, index) {
                val value = row.getString(columnIndex++)
                if (value == null) null
                else if (value.startsWith("[")) json.decodeFromString(deserializer, value)
                else {
                    @Suppress("UNCHECKED_CAST")
                    parsePgArray(value) as T
                }
            }
        }

        // kotlin.time.Instant
        if (childDesc.serialName == "kotlin.time.Instant" || deserializer.descriptor.serialName == "kotlin.time.Instant") {
            return handleNullableValue(descriptor, index) {
                val value = row.getString(columnIndex++) ?: return@handleNullableValue null
                @Suppress("UNCHECKED_CAST")
                parseInstant(value) as T
            }
        }

        // Nested data classes
        if (childDesc.kind == StructureKind.CLASS && !childDesc.isInline) {
            val elementsCount = countElements(childDesc)
            val allNull = (columnIndex until (columnIndex + elementsCount)).all { row.isNull(it) }
            if (allNull && (descriptor.isElementOptional(index) || childDesc.isNullable)) {
                columnIndex += elementsCount
                return null
            }
            val nestedDecoder = RowDecoder(row, serializersModule, columnIndex, json) { newIndex ->
                columnIndex = newIndex
            }
            return deserializer.deserialize(nestedDecoder)
        }

        return handleNullableValue(descriptor, index) {
            deserializer.deserialize(this)
        }
    }

    private fun countElements(descriptor: SerialDescriptor): Int = when (descriptor.kind) {
        StructureKind.CLASS -> (0 until descriptor.elementsCount).sumOf { i ->
            val child = descriptor.getElementDescriptor(i)
            when {
                isJsonFieldAnnotated(descriptor, i) -> 1
                child.kind == StructureKind.LIST -> 1
                child.kind == StructureKind.CLASS && !child.isInline -> countElements(child)
                else -> 1
            }
        }
        else -> 1
    }
}

/**
 * Parse a PostgreSQL text-format timestamp string into a kotlin.time.Instant.
 * Handles PG format like "2026-02-04 04:31:16.935337+01" by normalizing to ISO-8601.
 */
internal fun parseInstant(value: String): kotlin.time.Instant {
    val normalized = value.replaceFirst(' ', 'T')
    val hasTimezone = normalized.endsWith('Z') || run {
        val tail = normalized.takeLast(8)
        tail.contains('+') || tail.contains('-')
    }
    val isoString = if (hasTimezone) normalized else "${normalized}Z"
    return kotlin.time.Instant.parse(isoString)
}

/**
 * Parse a PostgreSQL text-format array like `{val1,val2,val3}` into a List<String>.
 */
internal fun parsePgArray(value: String): List<String> {
    if (value == "{}" || value == "{}") return emptyList()
    val inner = value.removePrefix("{").removeSuffix("}")
    if (inner.isEmpty()) return emptyList()
    // Simple split — doesn't handle quoted/escaped values with commas
    return inner.split(",").map { it.trim().removeSurrounding("\"") }
}
