package io.djb.kotlin

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo

/**
 * Marks that a property expects a JSON value from the database.
 * The column value will be deserialized from JSON using `kotlinx.serialization`.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.PROPERTY, AnnotationTarget.TYPE)
annotation class SqlJsonValue
