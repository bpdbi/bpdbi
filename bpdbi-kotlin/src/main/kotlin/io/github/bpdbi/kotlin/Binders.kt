package io.github.bpdbi.kotlin

import io.github.bpdbi.core.BinderRegistry
import io.github.bpdbi.core.ColumnMapperRegistry
import io.github.bpdbi.core.Connection
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.uuid.toJavaUuid
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.datetime.toKotlinLocalDateTime
import kotlinx.datetime.toKotlinLocalTime

/**
 * Register Binders and ParamEncoders for Kotlin-specific types. ParamEncoders convert Kotlin types
 * to Java equivalents so the binary encoder can handle them (e.g. kotlin.uuid.Uuid → java.util.UUID).
 * Text Binders are kept as fallback for non-binary-capable drivers.
 */
fun BinderRegistry.registerKotlinTypes(): BinderRegistry = apply {
  // ParamEncoders: convert Kotlin types to Java equivalents for binary encoding
  registerEncoder(kotlin.uuid.Uuid::class.java) { it.toJavaUuid() }
  registerEncoder(kotlin.time.Instant::class.java) { it.toJavaInstant() }
  registerEncoder(kotlinx.datetime.LocalDate::class.java) { it.toJavaLocalDate() }
  registerEncoder(kotlinx.datetime.LocalDateTime::class.java) { it.toJavaLocalDateTime() }
  registerEncoder(kotlinx.datetime.LocalTime::class.java) { it.toJavaLocalTime() }
  registerEncoder(UInt::class.java) { it.toLong() }
  registerEncoder(ULong::class.java) { it.toLong() }

  // Text binders: fallback when binary encoding is not available
  register(kotlin.uuid.Uuid::class.java) { it.toString() }
  register(kotlin.time.Instant::class.java) { it.toString() }
  register(kotlinx.datetime.LocalDate::class.java) { it.toString() }
  register(kotlinx.datetime.LocalDateTime::class.java) { it.toString() }
  register(kotlinx.datetime.LocalTime::class.java) { it.toString() }
  register(UInt::class.java) { it.toLong().toString() }
  register(ULong::class.java) { it.toLong().toString() }
}

/** Register ColumnMappers (that parse from text) for Kotlin-specific types. */
fun ColumnMapperRegistry.registerKotlinTypes(): ColumnMapperRegistry = apply {
  register(kotlin.uuid.Uuid::class.java) { v, _ -> kotlin.uuid.Uuid.parse(v) }
  register(kotlin.time.Instant::class.java) { v, _ -> parseInstant(v) }
  register(kotlinx.datetime.LocalDate::class.java) { v, _ ->
    java.time.LocalDate.parse(v).toKotlinLocalDate()
  }
  register(kotlinx.datetime.LocalDateTime::class.java) { v, _ ->
    java.time.LocalDateTime.parse(v.replace(' ', 'T')).toKotlinLocalDateTime()
  }
  register(kotlinx.datetime.LocalTime::class.java) { v, _ ->
    java.time.LocalTime.parse(v).toKotlinLocalTime()
  }
}

/** Register Kotlin type binders and mappers on this connection. */
fun Connection.useKotlinTypes() {
  binderRegistry().registerKotlinTypes()
  mapperRegistry().registerKotlinTypes()
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
