package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Connection
import io.github.bpdbi.core.RowSet
import io.github.bpdbi.core.Transaction
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

/**
 * Execute [block] within a transaction and return the result. Commits on normal return, rolls back
 * on exception.
 *
 * Named `inTx` (not `inTransaction`) to avoid shadowing the same-named Java default method on
 * [Connection]. Kotlin's overload resolution always prefers Java default methods over extensions
 * with identical signatures, which would make an identically named extension unreachable.
 */
inline fun <T> Connection.inTx(block: (Transaction) -> T): T {
  val tx = begin()
  try {
    val result = block(tx)
    tx.commit()
    return result
  } catch (e: Throwable) {
    tx.close()
    throw e
  }
}

/**
 * Execute [block] within a transaction. Commits on normal return, rolls back on exception.
 *
 * Named `useTx` (not `useTransaction`) to avoid shadowing the same-named Java default method on
 * [Connection]. See [inTx] for rationale.
 */
inline fun Connection.useTx(block: (Transaction) -> Unit) {
  val tx = begin()
  try {
    block(tx)
    tx.commit()
  } catch (e: Throwable) {
    tx.close()
    throw e
  }
}
