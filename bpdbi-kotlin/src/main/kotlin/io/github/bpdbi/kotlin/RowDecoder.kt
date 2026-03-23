package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Row
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.datetime.toKotlinLocalDateTime
import kotlinx.datetime.toKotlinLocalTime
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule

@PublishedApi internal val defaultJson = Json { ignoreUnknownKeys = true }

/**
 * A `kotlinx.serialization` [Decoder] that reads from bpdbi's [Row].
 *
 * Since bpdbi stores values as raw bytes (text or binary format) and [Row] already
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
    startColumn: Int = 0,
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

    private fun <T> handleNullableValue(
        descriptor: SerialDescriptor,
        index: Int,
        orElse: () -> T
    ): T? {
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
    override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int): Boolean =
        decodeBoolean()

    override fun decodeByteElement(descriptor: SerialDescriptor, index: Int): Byte = decodeByte()
    override fun decodeShortElement(descriptor: SerialDescriptor, index: Int): Short = decodeShort()
    override fun decodeIntElement(descriptor: SerialDescriptor, index: Int): Int = decodeInt()
    override fun decodeLongElement(descriptor: SerialDescriptor, index: Int): Long = decodeLong()
    override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int): Float = decodeFloat()
    override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int): Double =
        decodeDouble()

    override fun decodeCharElement(descriptor: SerialDescriptor, index: Int): Char = decodeChar()
    override fun decodeStringElement(descriptor: SerialDescriptor, index: Int): String =
        decodeString()

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

        // Arrays/lists: dispatch to typed Row array getters (binary: zero-copy, no string intermediary)
        if (childDescriptor.kind == StructureKind.LIST) {
            val list = readTypedArray(childDescriptor, columnIndex)
            if (list != null) {
                columnIndex++
                @Suppress("UNCHECKED_CAST")
                return (if (isSetDescriptor(deserializer.descriptor)) (list as List<*>).toSet() else list) as T
            }
            // Fallback: read as string (handles JSON arrays and non-PG databases)
            val value = row.getString(columnIndex++)
                ?: throw SerializationException("Array column cannot be null for non-nullable field at index $index")
            return json.decodeFromString(deserializer, value)
        }

        // Kotlin time types: read directly via Row typed getters (binary-aware)
        readTimeType(childDescriptor.serialName, row, columnIndex)?.let { value ->
            columnIndex++
            @Suppress("UNCHECKED_CAST")
            return value as T
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

        // Arrays/lists: binary-aware, nullable
        if (childDesc.kind == StructureKind.LIST) {
            return handleNullableValue(descriptor, index) {
                val list = readTypedArray(childDesc, columnIndex)
                if (list != null) {
                    columnIndex++
                    @Suppress("UNCHECKED_CAST")
                    return@handleNullableValue (if (isSetDescriptor(deserializer.descriptor))
                        (list as List<*>).toSet() else list) as T
                }
                // Fallback: read as string (JSON arrays and non-PG databases)
                val value = row.getString(columnIndex++)
                    ?: return@handleNullableValue null
                json.decodeFromString(deserializer, value)
            }
        }

        // Kotlin time types: read directly via Row typed getters (binary-aware, nullable)
        val timeSerialName = childDesc.serialName.takeIf { isTimeType(it) }
            ?: deserializer.descriptor.serialName.takeIf { isTimeType(it) }
        if (timeSerialName != null) {
            return handleNullableValue(descriptor, index) {
                @Suppress("UNCHECKED_CAST") // safe: timeSerialName matched a known time type
                readTimeType(timeSerialName, row, columnIndex++) as? T
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

    /**
     * Read a typed array from the Row using the appropriate typed getter based on the element kind.
     * Binary path: wire bytes → typed value directly, no string intermediary.
     * Returns null if the column is SQL NULL.
     */
    private fun readTypedArray(listDescriptor: SerialDescriptor, col: Int): Any? {
        val elementKind = listDescriptor.getElementDescriptor(0).kind
        return when (elementKind) {
            PrimitiveKind.INT -> row.getIntegerArray(col)
            PrimitiveKind.LONG -> row.getLongArray(col)
            PrimitiveKind.DOUBLE -> row.getDoubleArray(col)
            PrimitiveKind.FLOAT -> row.getFloatArray(col)
            PrimitiveKind.SHORT -> row.getShortArray(col)
            PrimitiveKind.BOOLEAN -> row.getBooleanArray(col)
            PrimitiveKind.STRING -> row.getStringArray(col)
            else -> row.getStringArray(col) // fallback: string elements for other types
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

/** Check if a serialName corresponds to a Kotlin time type we handle directly. */
private fun isTimeType(serialName: String): Boolean = serialName in TIME_TYPE_SERIAL_NAMES

private val TIME_TYPE_SERIAL_NAMES = setOf(
    "kotlin.time.Instant",
    "kotlinx.datetime.LocalDate",
    "kotlinx.datetime.LocalDateTime",
    "kotlinx.datetime.LocalTime"
)

/**
 * Read a Kotlin time type directly from [Row]'s typed getters (binary-aware).
 * Returns null if [serialName] is not a time type or if the column is SQL NULL.
 * Throws [SerializationException] only on unexpected errors.
 */
private fun readTimeType(serialName: String, row: Row, col: Int): Any? = when (serialName) {
    "kotlin.time.Instant" -> row.getInstant(col)?.let { javaInstant ->
        kotlin.time.Instant.fromEpochSeconds(javaInstant.epochSecond, javaInstant.nano.toLong())
    }
    "kotlinx.datetime.LocalDate" -> row.getLocalDate(col)?.toKotlinLocalDate()
    "kotlinx.datetime.LocalDateTime" -> row.getLocalDateTime(col)?.toKotlinLocalDateTime()
    "kotlinx.datetime.LocalTime" -> row.getLocalTime(col)?.toKotlinLocalTime()
    else -> null
}

/** Check if a descriptor represents a Set collection (vs List). */
private fun isSetDescriptor(descriptor: SerialDescriptor): Boolean =
    descriptor.serialName.startsWith("kotlin.collections.LinkedHashSet")
        || descriptor.serialName.startsWith("kotlin.collections.HashSet")

