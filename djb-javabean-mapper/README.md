# djb-javabean-mapper

A reflection-based `RowMapper` for [djb](../) that automatically maps query result rows to JavaBeans
(POJOs with a no-arg constructor and setter methods) using `java.beans.Introspector` (reflection).

## Setup

```kotlin
dependencies {
    implementation(platform("io.djb:djb-bom:0.1.0"))
    implementation("io.djb:djb-javabean-mapper")
    implementation("io.djb:djb-pg-client")  // or djb-mysql-client
}
```

## Usage

### Basic mapping

Define a bean whose property names match your column names:

```java
public class User {
    private int id;
    private String name;
    private String email;
    public User() {}
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}

RowMapper<User> mapper = JavaBeanRowMapper.of(User.class);
List<User> users = conn.query("SELECT id, name, email FROM users").mapTo(mapper);
```

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

With `djb-kotlin`, use `@Serializable` data classes instead of JavaBeans:

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

val users: List<User> = conn.queryAs("SELECT id, name, email FROM users")
```

</details>

### Single row

```java
User user = conn.query("SELECT id, name, email FROM users WHERE id = 1")
    .mapFirst(JavaBeanRowMapper.of(User.class));
```

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

```kotlin
val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = \$1", 1)
```

</details>

### How it works

The mapper uses `java.beans.Introspector` to discover writable properties (those with a setter
method). Property names
are matched to column names. Properties without a setter are silently skipped.

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

### Null handling

- **Boxed types** (`Integer`, `String`, etc.) are set to `null` when the column is SQL NULL.
- **Primitive types** (`int`, `long`, etc.) default to their zero value (`0`, `0L`, `false`, etc.)
  when the column is
  SQL NULL.

### When to use this vs. other mappers

- **Java records** → use `djb-record-mapper` (simpler, immutable, reflection-based)
- **Kotlin data classes** → use `djb-kotlin` (compile-time, no reflection)
- **Existing JavaBean POJOs** → use this module
- **One-off mapping** → use a `RowMapper` lambda directly:

```java
List<User> users = conn.query("SELECT id, name, email FROM users")
    .mapTo(row -> {
        var u = new User();
        u.setId(row.getInteger("id"));
        u.setName(row.getString("name"));
        u.setEmail(row.getString("email"));
        return u;
    });
```
