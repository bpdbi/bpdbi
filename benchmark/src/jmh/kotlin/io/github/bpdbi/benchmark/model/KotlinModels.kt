package io.github.bpdbi.benchmark.model

import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class UserKotlin(
    val id: Int,
    val username: String,
    val email: String,
    @SerialName("full_name") val fullName: String,
    val bio: String,
    val active: Boolean,
    @SerialName("created_at") val createdAt: LocalDateTime,
)

@Serializable
data class ProductKotlin(
    val id: Int,
    val name: String,
    val description: String,
    val price: Double,
    val category: String,
    val stock: Int,
)

@Serializable
data class OrderSummaryKotlin(
    val id: Int,
    val total: Double,
    val status: String,
    @SerialName("created_at") val createdAt: LocalDateTime,
    val username: String,
    @SerialName("item_count") val itemCount: Long,
)
