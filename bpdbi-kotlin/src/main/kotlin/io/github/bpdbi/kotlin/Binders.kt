package io.github.bpdbi.kotlin

import io.github.bpdbi.core.BinderRegistry
import io.github.bpdbi.core.ColumnMapperRegistry
import io.github.bpdbi.core.Connection
import io.github.bpdbi.core.impl.BaseConnection
import kotlin.time.Instant

/** Register Binders for Kotlin-specific types. */
fun BinderRegistry.registerKotlinTypes(): BinderRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { it.toString() }
    register(kotlin.time.Instant::class.java) { it.toString() }
    register(UInt::class.java) { it.toLong().toString() }
    register(ULong::class.java) { it.toLong().toString() }
}

/** Register ColumnMappers (the parse from text) for Kotlin-specific types. */
fun ColumnMapperRegistry.registerKotlinTypes(): ColumnMapperRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { v, _ -> kotlin.uuid.Uuid.parse(v) }
    register(kotlin.time.Instant::class.java) { v, _ -> parseInstant(v) }
}

/** Register Kotlin type binders and mappers on this connection. */
fun Connection.useKotlinTypes() {
    if (this is BaseConnection) {
        binderRegistry().registerKotlinTypes()
        mapperRegistry().registerKotlinTypes()
    }
}

/**
 * Parse a Postgres text-format timestamp string into a kotlin.time.Instant.
 * Handles PG format like "2026-02-04 04:31:16.935337+01" by normalizing to ISO-8601.
 * Used by [registerKotlinTypes] for text-format column mapping.
 */
internal fun parseInstant(value: String): Instant {
    val normalized = value.replaceFirst(' ', 'T')
    val hasTimezone = normalized.endsWith('Z') || run {
        val tail = normalized.takeLast(8)
        tail.contains('+') || tail.contains('-')
    }
    val isoString = if (hasTimezone) normalized else "${normalized}Z"
    return Instant.parse(isoString)
}