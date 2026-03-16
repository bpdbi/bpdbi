# djb-record-mapper

A reflection-based `RowMapper` for [djb](../) that automatically maps query result rows to Java
records
using `java.lang.reflect`.

## Setup

```kotlin
dependencies {
    implementation(platform("io.djb:djb-bom:0.1.0"))
    implementation("io.djb:djb-record-mapper")
    implementation("io.djb:djb-pg-client")  // or djb-mysql-client
}
```

## Usage

### Basic mapping

Define a record whose component names match your column names:

```java
record User(int id, String name, String email) {}

RowMapper<User> mapper = RecordRowMapper.of(User.class);
List<User> users = conn.query("SELECT id, name, email FROM users").mapTo(mapper);
```

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

With `djb-kotlin`, use `@Serializable` data classes instead of reflection:

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

val users: List<User> = conn.queryAs("SELECT id, name, email FROM users")
```

</details>

### Column name override with `@ColumnName`

When column names don't match component names (e.g. snake_case columns), use `@ColumnName`:

```java
record User(
    int id,
    @ColumnName("first_name") String firstName,
    @ColumnName("last_name") String lastName,
    String email  // no annotation needed — matches column "email" exactly
) {}

RowMapper<User> mapper = RecordRowMapper.of(User.class);
List<User> users = conn.query("SELECT id, first_name, last_name, email FROM users")
    .mapTo(mapper);
```

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

With `djb-kotlin`, use `@SerialName` from kotlinx.serialization:

```kotlin
@Serializable
data class User(
    val id: Int,
    @SerialName("first_name") val firstName: String,
    @SerialName("last_name") val lastName: String,
    val email: String
)

val users: List<User> = conn.queryAs("SELECT id, first_name, last_name, email FROM users")
```

</details>

### Single row

```java
User user = conn.query("SELECT id, name, email FROM users WHERE id = 1")
    .mapFirst(RecordRowMapper.of(User.class));
```

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

```kotlin
val user: User = conn.query("SELECT id, name, email FROM users WHERE id = 1")
    .deserializeFirst<User>()

// Or as a one-liner:
val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = \$1", 1)
```

</details>

### Supported types

| Java type             | Row getter used       |
|-----------------------|-----------------------|
| `String`              | `getString()`         |
| `int` / `Integer`     | `getInteger()`        |
| `long` / `Long`       | `getLong()`           |
| `short` / `Short`     | `getShort()`          |
| `float` / `Float`     | `getFloat()`          |
| `double` / `Double`   | `getDouble()`         |
| `boolean` / `Boolean` | `getBoolean()`        |
| `BigDecimal`          | `getBigDecimal()`     |
| `UUID`                | `getUUID()`           |
| `byte[]`              | `getBytes()`          |
| `LocalDate`           | `getLocalDate()`      |
| `LocalTime`           | `getLocalTime()`      |
| `LocalDateTime`       | `getLocalDateTime()`  |
| `OffsetDateTime`      | `getOffsetDateTime()` |

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

`djb-kotlin` supports all the above types plus:

- Nullable types (`Int?`, `String?`, etc.) — SQL NULL maps to `null`
- `kotlin.uuid.Uuid` and `kotlin.time.Instant` (via `conn.useKotlinTypes()`)
- Nested data classes (flattened from consecutive columns)
- Enums (matched by name)
- JSON fields via `@SqlJsonValue`
- Postgres arrays and lists

</details>

### Null handling

- **Boxed types** (`Integer`, `String`, etc.) are set to `null` when the column is SQL NULL.
- **Primitive types** (`int`, `long`, etc.) default to their zero value (`0`, `0L`, `false`, etc.)
  when the column is
  SQL NULL.

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

With `djb-kotlin`, null handling is type-safe at compile time:

- **Non-nullable types** (`Int`, `String`) throw `SerializationException` if the column is SQL NULL
- **Nullable types** (`Int?`, `String?`) are set to `null`

</details>

### Error handling

The mapper fails fast with clear messages when:

- The class is not a record
- A record component has an unsupported type
- A column name is missing from the result row
