# Plans: Value Binding and Result Mapping

## Overview

Add Jdbi-style value binding (Java objects → SQL parameters) and result mapping
(SQL rows → Java objects) to djb. Unlike Jdbi, the mapping system is fully
pluggable from the start — no built-in reflection-based mapper. The core provides
interfaces and primitives; users (or extension libraries) bring the actual mapping
logic for their types.

This is split into 4 independent, incremental plans. Each delivers standalone
value and can be shipped separately.

---

## Plan 1: RowMapper interface + mapTo()

**Goal:** Let users convert rows to typed objects without manual getter calls.

**What changes:**

Add to `djb-core`:

```java
@FunctionalInterface
public interface RowMapper<T> {
    T map(Row row);
}
```

Add convenience methods to `RowSet`:

```java
public <T> List<T> mapTo(RowMapper<T> mapper) {
    return stream().map(mapper::map).toList();
}

public <T> T mapFirst(RowMapper<T> mapper) {
    return mapper.map(first());
}
```

**Usage:**

```java
// Inline lambda
List<String> names = conn.query("SELECT name FROM users")
    .mapTo(row -> row.getString("name"));

// Reusable mapper
RowMapper<User> userMapper = row -> new User(
    row.getInteger("id"),
    row.getString("name"),
    row.getString("email")
);
List<User> users = conn.query("SELECT * FROM users").mapTo(userMapper);
User alice = conn.query("SELECT * FROM users WHERE id = $1", 1).mapFirst(userMapper);
```

**Why no reflection-based mapper built in?**
- Records and data classes vary across Java/Kotlin
- Constructor parameter names require `-parameters` compiler flag
- kotlinx.serialization uses its own mechanism entirely
- A reflection mapper is easy to add as an extension — the interface is what matters

**Tests:**
- mapTo with lambda
- mapTo with record
- mapFirst
- mapTo on empty result
- mapTo preserves order

**Files changed:** `RowSet.java` (add methods), new `RowMapper.java`

---

## Plan 2: TypeBinder registry for parameter encoding

**Goal:** Let users register custom Java-type-to-SQL-value converters, so
`conn.query("...", myCustomObject)` works without manual `.toString()` calls.

**What changes:**

Add to `djb-core`:

```java
@FunctionalInterface
public interface TypeBinder<T> {
    String bind(T value);
}
```

```java
public class TypeRegistry {
    private final Map<Class<?>, TypeBinder<?>> binders = new LinkedHashMap<>();

    // Register a binder for a type
    public <T> TypeRegistry register(Class<T> type, TypeBinder<T> binder) { ... }

    // Look up how to convert a value to a SQL string
    public String bind(Object value) { ... }

    // Built-in defaults
    public static TypeRegistry defaults() {
        var reg = new TypeRegistry();
        reg.register(String.class, v -> v);
        reg.register(Integer.class, Object::toString);
        reg.register(Long.class, Object::toString);
        reg.register(Double.class, Object::toString);
        reg.register(Float.class, Object::toString);
        reg.register(Boolean.class, Object::toString);
        reg.register(java.math.BigDecimal.class, Object::toString);
        reg.register(java.util.UUID.class, Object::toString);
        reg.register(java.time.LocalDate.class, Object::toString);
        reg.register(java.time.LocalTime.class, Object::toString);
        reg.register(java.time.LocalDateTime.class, Object::toString);
        reg.register(java.time.OffsetDateTime.class, Object::toString);
        reg.register(byte[].class, v -> "\\x" + hexEncode(v));
        return reg;
    }
}
```

The `TypeRegistry` is set on the connection (or passed at connect time) and
used by `BaseConnection` when converting `Object... params` to `String[]`
for the wire protocol. This replaces the current hard-coded `params[i].toString()`.

**Usage:**

```java
// Custom type
record Money(BigDecimal amount, String currency) {}

var registry = TypeRegistry.defaults();
registry.register(Money.class, m -> m.amount().toPlainString());

var conn = PgConnection.connect(config);
conn.setTypeRegistry(registry);
conn.query("INSERT INTO prices VALUES ($1)", new Money(new BigDecimal("9.99"), "USD"));
```

**Why a registry instead of just toString()?**
- `toString()` is not always SQL-safe (e.g., `byte[]`, enum values, custom types)
- Lets users control serialization without wrapping every value
- Foundation for kotlinx.serialization integration

**Tests:**
- Default binders for all primitive types
- Custom binder registration
- Binder lookup with inheritance (e.g., `Integer` extends `Number`)
- null handling
- Unknown type falls back to toString()

**Files changed:** New `TypeBinder.java`, new `TypeRegistry.java`, `BaseConnection.java`
(use registry in param conversion)

---

## Plan 3: Named parameters

**Goal:** Support `:name` style parameters for readability, mapped from a
`Map<String, Object>` instead of positional `Object...`.

**What changes:**

Add to `Connection` interface:

```java
RowSet query(String sql, Map<String, Object> params);
int enqueue(String sql, Map<String, Object> params);
```

The implementation:
1. Parse SQL, find all `:name` tokens (outside quotes)
2. Replace them with positional placeholders (`$1`, `$2` for PG; `?` for MySQL)
3. Build the `Object[]` array in the order the placeholders appear
4. Delegate to the existing positional `query(sql, params...)`

```java
// Parsing ":name" → "$1" and collecting param values
record ParsedQuery(String sql, Object[] params) {}

static ParsedQuery parseNamed(String sql, Map<String, Object> params,
                               String placeholderPrefix) { ... }
```

This is done in `BaseConnection` so it works for both PG and MySQL. The
placeholder format (`$N` vs `?`) is provided by each driver via an abstract method.

**Usage:**

```java
conn.query(
    "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
    Map.of("name", "Alice", "email", "alice@example.com", "age", 30)
);

conn.query(
    "SELECT * FROM users WHERE name = :name AND age > :minAge",
    Map.of("name", "Bob", "minAge", 21)
);
```

**Tests:**
- Single named param
- Multiple named params
- Repeated param name (same value used twice)
- Named param inside quoted string (should not be replaced)
- Mixed with TypeRegistry (named params use custom binders)
- PG placeholder format ($1, $2)
- MySQL placeholder format (?, ?)

**Files changed:** `Connection.java` (new method signatures), `BaseConnection.java`
(parsing + delegation), each driver provides placeholder format

---

## Plan 4: ColumnMapper registry for result decoding

**Goal:** Let users register custom SQL-value-to-Java-type converters, so
`row.get("price", Money.class)` works with custom types.

**What changes:**

Add to `djb-core`:

```java
@FunctionalInterface
public interface ColumnMapper<T> {
    T map(String value, String columnName);  // value is the raw text from the DB
}
```

```java
public class MapperRegistry {
    private final Map<Class<?>, ColumnMapper<?>> mappers = new LinkedHashMap<>();

    public <T> MapperRegistry register(Class<T> type, ColumnMapper<T> mapper) { ... }
    public <T> T map(Class<T> type, String value, String columnName) { ... }

    public static MapperRegistry defaults() {
        var reg = new MapperRegistry();
        reg.register(String.class, (v, c) -> v);
        reg.register(Integer.class, (v, c) -> Integer.parseInt(v));
        reg.register(Long.class, (v, c) -> Long.parseLong(v));
        // ... all primitives, dates, UUID, etc.
        return reg;
    }
}
```

Add generic getter to `Row`:

```java
public <T> T get(int index, Class<T> type) {
    if (values[index] == null) return null;
    return mapperRegistry.map(type, textValue(index), columns[index].name());
}

public <T> T get(String columnName, Class<T> type) {
    return get(columnIndex(columnName), type);
}
```

**Usage:**

```java
// Custom column type
record Money(BigDecimal amount) {}

var registry = MapperRegistry.defaults();
registry.register(Money.class, (v, col) -> new Money(new BigDecimal(v)));

conn.setMapperRegistry(registry);
Money price = conn.query("SELECT price FROM products WHERE id = 1")
    .first().get("price", Money.class);
```

**Interaction with RowMapper (Plan 1):**

A `ColumnMapper` handles individual columns. A `RowMapper` handles entire rows.
They complement each other:

```java
// ColumnMapper: value → typed object (one column)
registry.register(Money.class, (v, col) -> new Money(new BigDecimal(v)));

// RowMapper: row → domain object (all columns)
RowMapper<Product> productMapper = row -> new Product(
    row.getInteger("id"),
    row.getString("name"),
    row.get("price", Money.class)  // uses ColumnMapper
);
```

**This is also the JSON integration point:**

```java
// Jackson
registry.register(JsonNode.class, (v, col) -> objectMapper.readTree(v));

// kotlinx.serialization (from Kotlin code)
registry.register(MyData.class, (v, col) -> Json.decodeFromString(v));
```

**Tests:**
- Default column mappers for all types
- Custom column mapper
- row.get(index, Class) and row.get(name, Class)
- null handling
- Unknown type throws clear error
- JSON column mapper

**Files changed:** New `ColumnMapper.java`, new `MapperRegistry.java`, `Row.java`
(add `get(int, Class)` and `get(String, Class)`), `Connection` / `BaseConnection`
(carry registry reference)

---

## Implementation order

```
Plan 1 (RowMapper)       — no dependencies, pure addition
    ↓
Plan 2 (TypeBinder)      — no dependencies, pure addition
    ↓
Plan 3 (Named params)    — benefits from Plan 2 (uses TypeRegistry for binding)
    ↓
Plan 4 (ColumnMapper)    — benefits from Plan 1 (ColumnMapper feeds into RowMapper)
```

Plans 1 and 2 are independent and can be done in either order.
Plan 3 builds on Plan 2.
Plan 4 builds on Plan 1.

Each plan is small enough to implement, test, and ship in isolation.
