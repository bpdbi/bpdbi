package io.github.bpdbi.kotlin

import io.github.bpdbi.core.Row
import io.github.bpdbi.core.test.TestRows

internal fun testRow(vararg values: String?): Row {
    val columnNames = Array(values.size) { "col$it" }
    return TestRows.row(columnNames, values)
}
