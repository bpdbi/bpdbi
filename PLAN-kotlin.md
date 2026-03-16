# Plan: djb-kotlin — Kotlin support module

## Goal

A `djb-kotlin` module that brings idiomatic Kotlin support to djb, modeled after
the helpers in `jdbi/`. The centerpiece is a `kotlinx.serialization`-based
`RowMapper` that can deserialize query results directly into `@Serializable`
data classes — without reflection.

## What the jdbi/ folder provides (and we're porting)

1. **ResultSetRowDecoder** — a `kotlinx.serialization` `Decoder` that reads
   from a JDBC `ResultSet` column-by-column, supporting primitives, enums,
   nullability, nested data classes, `@SqlJsonValue` JSON columns, arrays,
   `kotlin.time.Instant`, and `UUID`.

2. **Extension functions** — `ResultBearing.deserialize<T>()`,
   `deserializeToList<T>()`, `deserializeToOne<T>()`.

3. **Custom argument factories** — `InstantArgumentFactory`, `UuidArgumentFactory`
   for binding Kotlin types as query parameters.

4. **`@SqlJsonValue` annotation** — marks fields that should be deserialized
   from JSON strings in the database.

## Key difference from jdbi version

The jdbi version decodes from a JDBC `ResultSet` (column-indexed, typed getters).
djb doesn't use JDBC — our `Row` has `byte[][]` values in text format. So our
decoder reads from `Row` instead of `ResultSet`, which is simpler (everything
is already a string, no type dispatch needed).

---

## Implementation plan

### Step 1 — Module setup

Create `djb-kotlin/` as a Kotlin module:

```
djb-kotlin/
├── build.gradle.kts
└── src/
    ├── main/kotlin/io/djb/kotlin/
    │   ├── RowDecoder.kt         — kotlinx.serialization Decoder for Row
    │   ├── SqlJsonValue.kt       — annotation for JSON columns
    │   ├── Extensions.kt         — extension functions on RowSet/Connection
    │   └── TypeBinders.kt        — TypeBinder registrations for Kotlin types
    └── test/kotlin/io/djb/kotlin/
        ├── RowDecoderTest.kt     — unit tests
        └── PgIntegrationTest.kt  — integration tests with Testcontainers
```

**build.gradle.kts:**
```kotlin
plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.20"
    `java-library`
}

dependencies {
    api(project(":djb-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")

    testImplementation(project(":djb-pg-client"))
    testImplementation("org.testcontainers:postgresql:1.20.4")
    // ... junit
}
```

### Step 2 — RowDecoder (the core)

A `kotlinx.serialization` `Decoder` + `CompositeDecoder` that reads from
djb's `Row`. Since djb uses text format, this is simpler than the jdbi
version (no JDBC type dispatch, no `rs.getInt()` vs `rs.getString()` etc.).

```kotlin
class RowDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule = defaultModule,
    private val startColumn: Int = 0,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val endCallback: (Int) -> Unit = {}
) : Decoder, CompositeDecoder {

    private var columnIndex = startColumn
    private var elementIndex = 0

    // Primitive decoders — all read from row's text values
    override fun decodeString(): String = row.getString(columnIndex++)
    override fun decodeInt(): Int = row.getInteger(columnIndex++)
    override fun decodeLong(): Long = row.getLong(columnIndex++)
    override fun decodeBoolean(): Boolean = row.getBoolean(columnIndex++)
    override fun decodeDouble(): Double = row.getDouble(columnIndex++)
    override fun decodeFloat(): Float = row.getFloat(columnIndex++)
    override fun decodeShort(): Short = row.getShort(columnIndex++)
    // ...

    // Null handling
    override fun decodeNotNullMark(): Boolean = !row.isNull(columnIndex)
    override fun decodeNull(): Nothing? { columnIndex++; return null }

    // Structure traversal
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder = this
    override fun endStructure(descriptor: SerialDescriptor) = endCallback(columnIndex)
    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (elementIndex < descriptor.elementsCount) elementIndex++ else DECODE_DONE

    // Complex types
    // - @SqlJsonValue → json.decodeFromString(row.getString(col))
    // - Nested classes → new RowDecoder with column offset + callback
    // - Enums → decodeString() → lookup
    // - UUID → row.getUUID(col) or UUID.fromString()
    // - Lists/arrays → parse PG array format or JSON
}
```

**Compared to the jdbi version:**
- No `ResultSet`/`ResultSetMetaData` — reads from `Row` directly
- No `rs.wasNull()` checks — we have `row.isNull()`
- No JDBC type dispatch (`getInt` vs `getLong`) — everything is text
- Same nested structure pattern with callback
- Same `@SqlJsonValue` handling
- Same `countElements()` for nullable nested classes

### Step 3 — `@SqlJsonValue` annotation

Identical to the jdbi version:

```kotlin
@SerialInfo
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.PROPERTY, AnnotationTarget.TYPE)
annotation class SqlJsonValue
```

### Step 4 — Extension functions

Idiomatic Kotlin extensions on djb types:

```kotlin
// Deserialize a RowSet to a list of objects
inline fun <reified T> RowSet.deserializeToList(): List<T> =
    mapTo { row -> serializer<T>().deserialize(RowDecoder(row)) }

// Deserialize first row
inline fun <reified T> RowSet.deserializeFirst(): T =
    mapFirst { row -> serializer<T>().deserialize(RowDecoder(row)) }

// Deserialize first row or null
inline fun <reified T> RowSet.deserializeFirstOrNull(): T? =
    if (size() == 0) null else deserializeFirst<T>()

// Direct query + deserialize
inline fun <reified T> Connection.queryAs(sql: String, vararg params: Any?): List<T> =
    query(sql, *params).deserializeToList<T>()

inline fun <reified T> Connection.queryOneAs(sql: String, vararg params: Any?): T? =
    query(sql, *params).deserializeFirstOrNull<T>()

// Named params
inline fun <reified T> Connection.queryAs(sql: String, params: Map<String, Any?>): List<T> =
    query(sql, params).deserializeToList<T>()
```

### Step 5 — TypeBinder registrations for Kotlin types

Register binders for Kotlin-specific types:

```kotlin
fun TypeRegistry.registerKotlinTypes(): TypeRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { it.toString() }
    register(kotlin.time.Instant::class.java) { it.toString() }
    // Kotlin unsigned types
    register(UInt::class.java) { it.toLong().toString() }
    register(ULong::class.java) { it.toLong().toString() }
}

// Convenience extension
fun Connection.useKotlinTypes() {
    if (this is BaseConnection) {
        typeRegistry().registerKotlinTypes()
    }
}
```

### Step 6 — ColumnMapper registrations for Kotlin types

```kotlin
fun MapperRegistry.registerKotlinTypes(): MapperRegistry = apply {
    register(kotlin.uuid.Uuid::class.java) { v, _ -> kotlin.uuid.Uuid.parse(v) }
    register(kotlin.time.Instant::class.java) { v, _ ->
        val normalized = v.replaceFirst(' ', 'T')
        val hasTimezone = normalized.endsWith('Z') ||
            normalized.takeLast(8).let { it.contains('+') || it.contains('-') }
        kotlin.time.Instant.parse(if (hasTimezone) normalized else "${normalized}Z")
    }
}
```

---

## Usage examples

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

@Serializable
data class Order(
    val id: Int,
    val userId: Int,
    @SqlJsonValue val metadata: OrderMetadata
)

@Serializable
data class OrderMetadata(val source: String, val priority: Int)

// Query and deserialize
val users: List<User> = conn.queryAs("SELECT id, name, email FROM users")
val user: User? = conn.queryOneAs("SELECT * FROM users WHERE id = :id", mapOf("id" to 1))

// Or step by step
val rs = conn.query("SELECT id, name, email FROM users")
val users = rs.deserializeToList<User>()

// Nested structures (columns flatten into parent)
@Serializable
data class UserWithAddress(
    val id: Int,
    val name: String,
    val address: Address  // reads next N columns as Address
)

@Serializable
data class Address(val street: String, val city: String)

// SELECT id, name, street, city FROM users JOIN addresses ...
val usersWithAddr: List<UserWithAddress> = conn.queryAs(
    "SELECT u.id, u.name, a.street, a.city FROM users u JOIN addresses a ON u.id = a.user_id"
)
```

---

## What's NOT included (and why)

- **Coroutines/Flow support** — djb is blocking by design. Virtual threads
  are the concurrency model. Adding `suspend` functions or `Flow` would go
  against the project philosophy.
- **Reflection-based mapping** — The whole point is using `kotlinx.serialization`
  (compile-time code generation, no reflection).
- **Value class support** — Requires `decodeInline()` handling. Can be added
  as a follow-up once the basics work.
- **Kotlin-specific DSL for queries** — Out of scope. The SQL-first philosophy
  means users write SQL directly.

## Test plan

**Unit tests (no DB):**
- RowDecoder with mock Row for each primitive type
- Null handling (nullable fields, non-nullable fields with null → error)
- Nested data class decoding
- `@SqlJsonValue` decoding
- Enum decoding
- UUID decoding
- Instant decoding (various PG timestamp formats)

**Integration tests (Testcontainers PG):**
- `deserializeToList<T>()` for simple data class
- `deserializeFirst<T>()` with parameters
- `queryAs<T>()` convenience
- Named params + deserialization
- Nested data class from JOIN query
- `@SqlJsonValue` from jsonb column
- Nullable fields
- Empty result set
- Kotlin types as query parameters (Uuid, Instant)
