package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Connection
import io.github.bpdbi.core.TypeRegistry
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.toJavaUuid
import kotlin.uuid.toKotlinUuid
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.datetime.toKotlinLocalDateTime
import kotlinx.datetime.toKotlinLocalTime

/**
 * Register type codecs for Kotlin-specific types. Each registration maps a Kotlin type to its
 * Java equivalent so the binary codec can handle it natively (e.g. kotlin.uuid.Uuid → java.util.UUID).
 */
@OptIn(ExperimentalUuidApi::class)
fun TypeRegistry.registerKotlinTypes(): TypeRegistry = apply {
  register(kotlin.uuid.Uuid::class.java, java.util.UUID::class.java,
    { it.toJavaUuid() },
    { it.toKotlinUuid() })
  register(kotlin.time.Instant::class.java, java.time.Instant::class.java,
    { it.toJavaInstant() },
    { Instant.fromEpochSeconds(it.epochSecond, it.nano) })
  register(kotlinx.datetime.LocalDate::class.java, java.time.LocalDate::class.java,
    { it.toJavaLocalDate() },
    { it.toKotlinLocalDate() })
  register(kotlinx.datetime.LocalDateTime::class.java, java.time.LocalDateTime::class.java,
    { it.toJavaLocalDateTime() },
    { it.toKotlinLocalDateTime() })
  register(kotlinx.datetime.LocalTime::class.java, java.time.LocalTime::class.java,
    { it.toJavaLocalTime() },
    { it.toKotlinLocalTime() })

  // UInt/ULong: encode-only (read back as Long/Int from result rows)
  register(UInt::class.java, Long::class.javaObjectType, { java.lang.Long.valueOf(it.toLong()) }, null)
  register(ULong::class.java, Long::class.javaObjectType, { java.lang.Long.valueOf(it.toLong()) }, null)
}

/** Register Kotlin type codecs on this connection. */
fun Connection.useKotlinTypes() {
  typeRegistry().registerKotlinTypes()
}
