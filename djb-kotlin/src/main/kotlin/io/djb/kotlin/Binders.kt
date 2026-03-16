package io.djb.kotlin

import io.djb.BinderRegistry
import io.djb.ColumnMapperRegistry
import io.djb.Connection
import io.djb.impl.BaseConnection

/** Register Binders for Kotlin-specific types. */
fun BinderRegistry.registerKotlinTypes(): BinderRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { it.toString() }
    register(kotlin.time.Instant::class.java) { it.toString() }
    register(UInt::class.java) { it.toLong().toString() }
    register(ULong::class.java) { it.toLong().toString() }
}

/** Register ColumnMappers for Kotlin-specific types. */
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
