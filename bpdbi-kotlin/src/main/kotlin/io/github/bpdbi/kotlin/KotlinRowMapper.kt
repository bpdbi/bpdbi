package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Row
import io.github.bpdbi.core.RowMapper
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializer

/**
 * A [RowMapper] that maps rows to Kotlin `@Serializable` data classes using `kotlinx.serialization`.
 *
 * Columns are consumed in declaration order, with nested data classes flattened into consecutive
 * columns — same as [RecordRowMapper][io.github.bpdbi.mapper.RecordRowMapper] and
 * [JavaBeanRowMapper][io.github.bpdbi.mapper.JavaBeanRowMapper].
 *
 * ```kotlin
 * @Serializable
 * data class User(val id: Int, val name: String, val email: String)
 *
 * val mapper = KotlinRowMapper.of<User>()
 * val users = conn.query("SELECT id, name, email FROM users").mapTo(mapper)
 * ```
 */
class KotlinRowMapper<T : Any>(
    private val deserializer: DeserializationStrategy<T>,
    private val serializersModule: SerializersModule = SerializersModule {},
    private val json: Json = Json { ignoreUnknownKeys = true }
) : RowMapper<T> {

    override fun map(row: Row): T {
        val decoder = RowDecoder(row, serializersModule, json = json)
        return deserializer.deserialize(decoder)
    }

    companion object {
        /** Create a mapper for the given `@Serializable` type. */
        inline fun <reified T : Any> of(
            serializersModule: SerializersModule = SerializersModule {},
            json: Json = Json { ignoreUnknownKeys = true }
        ): KotlinRowMapper<T> = KotlinRowMapper(serializer<T>(), serializersModule, json)
    }
}
