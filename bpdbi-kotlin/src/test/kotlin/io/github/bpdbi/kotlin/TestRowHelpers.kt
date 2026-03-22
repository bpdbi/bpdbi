package io.github.bpdbi.kotlin

import io.github.bpdbi.core.ColumnDescriptor
import io.github.bpdbi.core.Row

internal fun testCol(name: String) = ColumnDescriptor(name, 0, 0, 0, 0, 0)

internal fun testRow(vararg values: String?): Row {
    val columns = values.mapIndexed { i, _ -> testCol("col$i") }.toTypedArray()
    val byteValues: Array<ByteArray?> =
        values.map { it?.toByteArray(Charsets.UTF_8) }.toTypedArray()
    // JSpecify's `byte @Nullable [][]` puts nullability on the inner array, but Kotlin
    // reads it as the outer array being nullable — cast to satisfy the constructor.
    @Suppress("UNCHECKED_CAST")
    return Row(columns, byteValues as Array<ByteArray>, null, null)
}
