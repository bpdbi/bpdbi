# bpdbi-kotlin — Kotlin support for bpdbi

Idiomatic Kotlin extensions for bpdbi, built on **kotlinx.serialization**.

Deserialize query results directly into `@Serializable` data classes — no reflection,
no annotation processing, no code generation at runtime. The Kotlin compiler plugin
generates all serialization code at compile time.

## Why kotlinx.serialization?

Most JVM mapping libraries (Jackson, Gson, MapStruct, ModelMapper) rely on runtime reflection
to inspect classes and populate fields. This has real costs:

- **Startup overhead** — reflection-based mappers discover fields and constructors at runtime
- **No compile-time safety** — mismatches between SQL columns and class fields surface as runtime
  errors
- **GraalVM native-image pain** — reflection requires explicit configuration and breaks easily
- **Kotlin data class friction** — default values, nullable types, and `val` properties don't always
  play well with
  reflection

`kotlinx.serialization` avoids all of this. The Kotlin compiler plugin generates a serializer
for each `@Serializable` class, so deserialization is just calling generated code — fast,
predictable, and fully compatible with native image.

## Setup

Add to your `build.gradle.kts`:

```kotlin
plugins {
    kotlin("plugin.serialization") version "2.1.20"
}

dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-kotlin")
    implementation("io.github.bpdbi:bpdbi-pg-client")  // or bpdbi-mysql-client
}
```

## Usage

### Basic query deserialization

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

// Deserialize all rows
val users: List<User> = conn.query("SELECT id, name, email FROM users")
    .deserializeToList<User>()

// Deserialize first row (throws if empty)
val user: User = conn.query("SELECT id, name, email FROM users WHERE id = $1", 42)
    .deserializeFirst<User>()

// First row or null
val maybeUser: User? = conn.query("SELECT id, name, email FROM users WHERE id = $1", 999)
    .deserializeFirstOrNull<User>()
```

### Convenience extensions on Connection

```kotlin
// queryAs — query + deserialize in one call
val users: List<User> = conn.queryAs("SELECT id, name, email FROM users")

// queryOneAs — query + first-or-null
val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = $1", 1)

// Named parameters
val users: List<User> = conn.queryAs(
    "SELECT id, name, email FROM users WHERE name = :name",
    mapOf("name" to "Alice")
)
```

### Using KotlinRowMapper

`KotlinRowMapper` implements the `RowMapper<T>` and uses `kotlinx.serialization` internally:

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

// Create a reusable mapper
val mapper = KotlinRowMapper.of<User>()

// Use with mapTo / mapFirst — same API as RecordRowMapper and JavaBeanRowMapper
val users: List<User> = conn.query("SELECT id, name, email FROM users").mapTo(mapper)
val user: User = conn.query("SELECT id, name, email FROM users WHERE id = $1", 1)
    .mapFirst(mapper)
```

### Nullable fields

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String?)

val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = $1", 1)
// user.email is null if the column is NULL
```

### Nested data classes

Nested classes consume consecutive columns from the result set — no special SQL needed,
just make sure the column order matches.

```kotlin
@Serializable
data class Address(val street: String, val city: String)

@Serializable
data class UserWithAddress(val id: Int, val name: String, val address: Address)

// Columns: id, name, street, city — address consumes the last two
val results: List<UserWithAddress> = conn.queryAs(
    "SELECT u.id, u.name, a.street, a.city FROM users u JOIN addresses a ON u.id = a.user_id"
)
```

Nullable nested classes are supported — if all columns for the nested structure are NULL,
the field is set to `null`.

### JSON columns with `@SqlJsonValue`

Mark fields that contain JSON stored in the database. The column value (a JSON string)
is deserialized using `kotlinx.serialization.json`:

```kotlin
@Serializable
data class OrderMeta(val source: String, val priority: Int)

@Serializable
data class Order(
    val id: Int,
    val userId: Int,
    @SqlJsonValue val meta: OrderMeta
)

val orders: List<Order> = conn.queryAs("SELECT id, user_id, meta FROM orders")
// meta column contains '{"source":"web","priority":5}' → deserialized into OrderMeta
```

### Enums

Enum values are matched by name against the string value in the column:

```kotlin
@Serializable
enum class Status { ACTIVE, INACTIVE, SUSPENDED }

@Serializable
data class Account(val id: Int, val status: Status)

val accounts: List<Account> = conn.queryAs("SELECT id, status FROM accounts")
```

### Kotlin type binders

Register binders for Kotlin-specific types (`kotlin.uuid.Uuid`, `kotlin.time.Instant`,
`UInt`, `ULong`) so they can be used as query parameters and mapped back from results:

```kotlin
conn.useKotlinTypes()

val uuid = Uuid.random()
conn.query("INSERT INTO items (id, name) VALUES ($1, $2)", uuid, "widget")
```

Or register on the registries directly for more control:

```kotlin
conn.binderRegistry().registerKotlinTypes() // for parameter binding
conn.mapperRegistry().registerKotlinTypes() // for Row.get(col, KotlinType::class.java)
```
