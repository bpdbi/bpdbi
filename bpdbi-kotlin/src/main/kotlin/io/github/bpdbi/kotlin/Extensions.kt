package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Connection
import io.github.bpdbi.core.RowSet
import kotlinx.serialization.serializer

/** Deserialize all rows to a list of [T]. */
inline fun <reified T : Any> RowSet.deserializeToList(): List<T> {
    val ser = serializer<T>()
    return mapTo { row -> ser.deserialize(RowDecoder(row)) }
}

/** Deserialize the first row to [T]. Throws if empty. */
inline fun <reified T : Any> RowSet.deserializeFirst(): T {
    val ser = serializer<T>()
    return mapFirst { row -> ser.deserialize(RowDecoder(row)) }
}

/** Deserialize the first row to [T], or null if empty. */
inline fun <reified T : Any> RowSet.deserializeFirstOrNull(): T? {
    if (isEmpty()) return null
    val ser = serializer<T>()
    return mapFirst { row -> ser.deserialize(RowDecoder(row)) }
}

/** Query and deserialize all rows to a list of [T]. */
inline fun <reified T : Any> Connection.queryAs(sql: String, vararg params: Any?): List<T> =
    query(sql, *params).deserializeToList<T>()

/** Query and deserialize the first row to [T], or null if empty. */
inline fun <reified T : Any> Connection.queryOneAs(sql: String, vararg params: Any?): T? =
    query(sql, *params).deserializeFirstOrNull<T>()

/** Query with named params and deserialize all rows to a list of [T]. */
inline fun <reified T : Any> Connection.queryAs(sql: String, params: Map<String, Any?>): List<T> =
    query(sql, params).deserializeToList<T>()

/** Query with named params and deserialize the first row to [T], or null if empty. */
inline fun <reified T : Any> Connection.queryOneAs(sql: String, params: Map<String, Any?>): T? =
    query(sql, params).deserializeFirstOrNull<T>()
