package io.djb.kotlin

import io.djb.Connection
import io.djb.MapperRegistry
import io.djb.TypeRegistry
import io.djb.impl.BaseConnection

/** Register TypeBinders for Kotlin-specific types. */
fun TypeRegistry.registerKotlinTypes(): TypeRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { it.toString() }
    register(kotlin.time.Instant::class.java) { it.toString() }
    register(UInt::class.java) { it.toLong().toString() }
    register(ULong::class.java) { it.toLong().toString() }
}

/** Register ColumnMappers for Kotlin-specific types. */
fun MapperRegistry.registerKotlinTypes(): MapperRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { v, _ -> kotlin.uuid.Uuid.parse(v) }
    register(kotlin.time.Instant::class.java) { v, _ -> parseInstant(v) }
}

/** Register Kotlin type binders and mappers on this connection. */
fun Connection.useKotlinTypes() {
    if (this is BaseConnection) {
        typeRegistry().registerKotlinTypes()
        mapperRegistry().registerKotlinTypes()
    }
}
