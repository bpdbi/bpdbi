# Bpdbi — Blocking Pipelined Database Interface for the JVM

A blocking database library the JVM that treats **pipelining** as a first-class concept.

Ported from the battle-tested [Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client)
(the foundation of Quarkus), but stripped of all async/reactive machinery; thus "blocking".
Since it's blocking, `java.net.Socket` is used for I/O (no Netty dependencies, event loop, `Uni<T>`,
Kotlin coroutines or other implementations of the "future" pattern).

## Why?

JDBC is [showing its age](docs/is-jdbc-showing-its-age.md) and (hence) does not allow pipelining
which is available
in [Postgres 14+](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)
and [MySQL 5.7.12+](https://dev.mysql.com/blog-archive/mysql-5-7-12-part-2-improving-the-mysql-protocol/).

Vert.x (`vertx-sql-client`) does support pipelines (for these databases) —it does not use JDBC—
but forces [reactive/async programming](docs/why-not-write-all-code-reactive.md).

Bpdbi gives you pipelining with straightforward blocking code — ideal for **Java 21+ virtual threads,
where blocking is inexpensive and readability of the code matters more than maximum throughput.

Bpdbi provides a better developer experience than JDBC alone, it can be compared to Jdbi's DX.

The whole library is a lot smaller that the typical JVM db stack, where a db driver, JDBC, and
something like Jdbi are needed (easily several MB of libraries), where Bpdbi is under 200kB.

## Why pipelining?

Pipelining sends multiple statements to the database in a single network write and reads all
responses back at once.
Reducing the number of db roundtrips per HTTP request cycle significantly improves performance:

```java
// 4 statements, 1 roundtrip
conn.enqueue("SET search_path TO myschema");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)");
RowSet result = conn.query("SELECT * FROM my_table WHERE id = $1", 42);
```

Without pipelining each of those would be a separate db roundtrip.
Over a network with 1ms latency, that's 4ms saved on every request — and it adds up.

**Dependency minimalism.** Bpdbi incurs a tiny dependency (<100k) compared to Vert.x/Netty (5MB+),
the Postgres JDBC driver (~1.1MB), or MySQL Connector/J (~2.5MB). It also provides named parameters,
row mapping, and type binding commonly found in libraries like Jdbi (~1MB) or Spring Data JDBC (~
3MB) —
without pulling in those dependencies. Bpdbi does not use the JVM reflection API out of the box
(some optional mapper modules use it).

## Design Principles

**Pipelining first** — The `enqueue()`/`flush()` API is not an afterthought.
Simple `query()` calls participate in the same pipeline machinery,
so you get batched I/O without restructuring your code.
Bpdbi has no separate "batch" API — pipelining is a strict superset of batching.
A batch sends N copies of the same statement with different parameters;
a pipeline sends N arbitrary statements (different SQL, different parameter counts)
in a single network write. Anything you'd do with batch, you can do with pipeline —
plus mix in SETs, DDL, transactions, and different queries in the same roundtrip.

**Binary protocol for parameterized queries** — Both the Postgres and MySQL drivers use the
database's binary wire protocol for parameterized query results. Binary encoding is more compact
on the wire, avoids text parsing overhead for numeric and temporal types, and simplifies decoding
(no locale-dependent formatting). Parameterless queries use the simple query protocol (text format)
because neither database supports all SQL commands via their prepared-statement protocol:
MySQL's `COM_STMT_PREPARE` rejects `BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, and other session
commands; Postgres's extended query protocol forbids multi-statement strings (`SELECT 1; SELECT 2`).
Text format also makes `getString()` work naturally for types that lack a dedicated binary codec
(geometric, network, array, and interval types in Postgres).

**Lazy decoding with column-oriented storage** — `Row` stores raw bytes from the wire and decodes
them only when you call a typed getter (`getInteger`, `getString`, etc.).
Columns you never read are never decoded, keeping CPU overhead minimal.
Buffered result sets use column-oriented storage internally: all values for a given column
are packed into a single contiguous `byte[]` buffer, and each `Row` is a lightweight view
(a column-buffer reference + a row index) with no per-row allocation.
For a 100K-row, 10-column result, this means 10 byte arrays instead of 1 million —
dramatically reducing GC pressure for large results.

**One connection per thread** — Connections are not thread-safe, same as JDBC and virtually every
SQL client in any language. The underlying wire protocol is a stateful, ordered byte stream —
concurrent writes from multiple threads would corrupt it. Transactions are also connection-scoped,
so sharing a connection between threads would mix transaction boundaries.
The standard pattern is: borrow a connection from a [pool](#connection-pooling), use it, return it.
With Java 21+ virtual threads, blocking on a pooled connection is cheap, so thousands of
concurrent requests each get their own connection naturally.

**Pluggable type system** — `BinderRegistry` (Java → SQL) and `ColumnMapperRegistry` (SQL → Java)
let you register custom converters for your domain types without forking the library or relying on
reflection.

**Null-safe API** — The entire public API is annotated with [JSpecify](https://jspecify.dev/)
(e.g.: `@NonNull` and `@Nullable`).
IDEs and tools like NullAway can statically verify correct null handling at compile time.
AI Coding Agents also like them.

**No compile-time SQL validation** — Bpdbi does not attempt to type-check the boundary between
your JVM code and the database. SQL strings are opaque at compile time, just like in JDBC.
Tools that do validate SQL at compile time (e.g. jOOQ, SQLC, SqlDelight) add significant
complexity and code generation steps. We believe a simpler policy works well in practice:
write one integration test per SQL query that exercises it against a real database instance.
These tests are straightforward to generate with AI assistance and catch schema mismatches,
typos, and type errors at test time rather than compile time — with far less machinery.

**Minimal dependencies** — Just `java.net.Socket`, a tiny SCRAM library (Postgres only),
and JSpecify annotations (a single 3KB jar).
No Netty, no reactive runtime, no reflection by default — ideal for GraalVM native images.

**GraalVM native-image ready** — The core library and drivers (`bpdbi-core`, `bpdbi-pg-client`,
`bpdbi-mysql-client`, `bpdbi-pool`) use zero reflection and work out of the box with
`native-image`. The optional mapper modules (`bpdbi-record-mapper`,
`bpdbi-javabean-mapper`) ship with GraalVM `reflect-config.json` metadata for their own
classes; you only need to register your application's record/bean types. The Kotlin module
uses compile-time code generation (kotlinx.serialization) and needs no reflection at all.

## Quick Start

### Dependencies

```kotlin
// build.gradle.kts
dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-pg-client")                // Postgres driver
    // implementation("io.github.bpdbi:bpdbi-mysql-client")          // MySQL driver
    // implementation("io.github.bpdbi:bpdbi-pool")                  // Connection pool
    // implementation("io.github.bpdbi:bpdbi-javabean-mapper")       // JavaBean mapping per row (reflection-based)
    // implementation("io.github.bpdbi:bpdbi-record-mapper")         // Java record mapping per row (reflection-based)
    // implementation("io.github.bpdbi:bpdbi-kotlin")                // Kotlin extensions + kotlinx.serialization based row mapper
}
```

### Connecting

```java
// Direct
try (var conn = PgConnection.connect("localhost", 5432, "mydb", "user", "pass")) {
    conn.query("SELECT 1");
}

// From URI
var config = ConnectionConfig.fromUri("postgresql://user:pass@localhost:5432/mydb");
try (var conn = PgConnection.connect(config)) {
    conn.query("SELECT 1");
}

// MySQL
try (var conn = MysqlConnection.connect("localhost", 3306, "mydb", "user", "pass")) {
    conn.query("SELECT 1");
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
// Direct
PgConnection.connect("localhost", 5432, "mydb", "user", "pass").use { conn ->
    conn.query("SELECT 1")
}

// From URI
val config = ConnectionConfig.fromUri("postgresql://user:pass@localhost:5432/mydb")
PgConnection.connect(config).use { conn ->
    conn.query("SELECT 1")
}

// MySQL
MysqlConnection.connect("localhost", 3306, "mydb", "user", "pass").use { conn ->
    conn.query("SELECT 1")
}
```

</details>

URI parsing supports `postgresql://`, `postgres://`, and `mysql://` schemes with
optional query parameters: `?sslmode=require&application_name=myapp`.

#### SSL/TLS

Five SSL modes are supported: `disable`, `prefer`, `require`, `verify-ca`, `verify-full`.

```java
// Via URI
var config = ConnectionConfig.fromUri(
    "postgresql://user:pass@host/db?sslmode=verify-full");

// Programmatic with PEM certificate
var config = new ConnectionConfig("host", 5432, "db", "user", "pass")
    .sslMode(SslMode.VERIFY_CA)
    .pemCertPath("/path/to/server-ca.pem");

// Programmatic with JKS trust store
var config = new ConnectionConfig("host", 5432, "db", "user", "pass")
    .sslMode(SslMode.VERIFY_CA)
    .trustStorePath("/path/to/truststore.jks")
    .trustStorePassword("changeit");

// Bring your own SSLContext (full control)
SSLContext ctx = SSLContext.getInstance("TLS");
ctx.init(keyManagers, trustManagers, null);
var config = new ConnectionConfig("host", 5432, "db", "user", "pass")
    .sslMode(SslMode.REQUIRE)
    .sslContext(ctx);

try (var conn = PgConnection.connect(config)) {
    conn.query("SELECT 1");
}
```

| Mode          | Encryption   | CA verification | Hostname verification |
|---------------|--------------|-----------------|-----------------------|
| `disable`     | No           | No              | No                    |
| `prefer`      | If available | No              | No                    |
| `require`     | Yes          | No              | No                    |
| `verify-ca`   | Yes          | Yes             | No                    |
| `verify-full` | Yes          | Yes             | Yes                   |

For `verify-ca` and `verify-full` without an explicit certificate/trust store,
the system default trust store is used (Java's `cacerts`).

### Queries

```java
// Simple query
RowSet users = conn.query("SELECT * FROM users WHERE active");
for (Row row : users) {
    System.out.println(row.getString("name") + ": " + row.getInteger("age"));
}

// Parameterized query (PG uses $1, $2; MySQL uses ?)
RowSet rs = conn.query("SELECT * FROM users WHERE id = $1", 42);
Row row = rs.first();
```

<details><summary>Kotlin equivalent</summary>

```kotlin
// Simple query
val users = conn.query("SELECT * FROM users WHERE active")
for (row in users) {
    println("${row.getString("name")}: ${row.getInteger("age")}")
}

// Parameterized query (PG uses $1, $2; MySQL uses ?)
val rs = conn.query("SELECT * FROM users WHERE id = $1", 42)
val row = rs.first()
```

</details>

### Named Parameters

Use `:name` style named parameters with a `Map` instead of positional placeholders:

```java
RowSet rs = conn.query(
    "SELECT * FROM users WHERE name = :name AND age > :age",
    Map.of("name", "Alice", "age", 21));
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val rs = conn.query(
    "SELECT * FROM users WHERE name = :name AND age > :age",
    mapOf("name" to "Alice", "age" to 21))
```

</details>

Named parameters are rewritten to positional placeholders (`$1`, `$2` for Postgres, `?` for MySQL)
before execution. They work with `query()`, `enqueue()`, and `prepare()`. The `::` cast operator
(e.g. `$1::int`) is correctly handled and not treated as a named parameter.

Collections and arrays are automatically expanded — see [IN-list Expansion](#in-list-expansion).

### Pipelining

Queue statements with `enqueue()`, send them all in one TCP write with `flush()`:

```java
// 4 statements, 1 network roundtrip
conn.enqueue("SET search_path TO myschema");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)");
RowSet result = conn.query("SELECT * FROM my_table WHERE id = $1", 42);
```

<details><summary>Kotlin equivalent</summary>

```kotlin
// 4 statements, 1 network roundtrip
conn.enqueue("SET search_path TO myschema")
conn.enqueue("SET statement_timeout TO '5s'")
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)")
val result = conn.query("SELECT * FROM my_table WHERE id = \$1", 42)
```

</details>

`query()` implicitly flushes any previously enqueued statements.

For explicit control, `enqueue()` returns an index and `flush()` returns all results:

```java
conn.enqueue("BEGIN");
int alice = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Alice");
int bob   = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Bob");
conn.enqueue("COMMIT");
List<RowSet> results = conn.flush();

long aliceId = results.get(alice).first().getLong("id");
long bobId   = results.get(bob).first().getLong("id");
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.enqueue("BEGIN")
val alice = conn.enqueue("INSERT INTO users (name) VALUES (\$1) RETURNING id", "Alice")
val bob   = conn.enqueue("INSERT INTO users (name) VALUES (\$1) RETURNING id", "Bob")
conn.enqueue("COMMIT")
val results = conn.flush()

val aliceId = results[alice].first().getLong("id")
val bobId   = results[bob].first().getLong("id")
```

</details>

Errors in one pipelined statement don't poison the others:

```java
conn.enqueue("SELECT 1");
conn.enqueue("SELECT * FROM nonexistent_table");  // fails
conn.enqueue("SELECT 3");
List<RowSet> results = conn.flush();

results.get(0).first().getInteger(0);  // 1
results.get(1).hasError();              // true
results.get(2).first().getInteger(0);  // 3
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.enqueue("SELECT 1")
conn.enqueue("SELECT * FROM nonexistent_table")  // fails
conn.enqueue("SELECT 3")
val results = conn.flush()

results[0].first().getInteger(0)  // 1
results[1].hasError()              // true
results[2].first().getInteger(0)  // 3
```

</details>

### Prepared Statements

Parse once, execute many times:

```java
try (var stmt = conn.prepare("SELECT * FROM users WHERE id = $1")) {
    RowSet alice = stmt.query(1);
    RowSet bob   = stmt.query(2);
    RowSet carol = stmt.query(3);
}  // closes the prepared statement on the server
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.prepare("SELECT * FROM users WHERE id = \$1").use { stmt ->
    val alice = stmt.query(1)
    val bob   = stmt.query(2)
    val carol = stmt.query(3)
}  // closes the prepared statement on the server
```

</details>

Named parameters work with prepared statements too:

```java
try (var stmt = conn.prepare("SELECT * FROM users WHERE name = :name AND age = :age")) {
    RowSet rs1 = stmt.query(Map.of("name", "Alice", "age", 30));
    RowSet rs2 = stmt.query(Map.of("name", "Bob", "age", 25));
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.prepare("SELECT * FROM users WHERE name = :name AND age = :age").use { stmt ->
    val rs1 = stmt.query(mapOf("name" to "Alice", "age" to 30))
    val rs2 = stmt.query(mapOf("name" to "Bob", "age" to 25))
}
```

</details>

The named parameter SQL is parsed at prepare time and the `:name` → position mapping is stored
with the statement. At execution time, the `Map` values are resolved to positional parameters.

IN-list expansion (`WHERE id IN (:ids)` with a collection) is not supported in prepared statements
because the SQL changes with collection size. However, **Postgres** supports an equivalent pattern
using `= ANY()` with an array parameter:

```java
// Postgres: use = ANY() instead of IN() for prepared statements with collections
try (var stmt = conn.prepare("SELECT * FROM users WHERE id = ANY(:ids::int[])")) {
    RowSet rs1 = stmt.query(Map.of("ids", List.of(1, 2, 3)));
    RowSet rs2 = stmt.query(Map.of("ids", Set.of(10, 20)));
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.prepare("SELECT * FROM users WHERE id = ANY(:ids::int[])").use { stmt ->
    val rs1 = stmt.query(mapOf("ids" to listOf(1, 2, 3)))
    val rs2 = stmt.query(mapOf("ids" to setOf(10, 20)))
}
```

</details>

Collection and array values are automatically formatted as Postgres array literals (`{1,2,3}`).
This works regardless of collection size without re-preparing the statement.
MySQL does not support array types — use `query(String, Map)` with IN-list expansion instead.

### Transactions

Use `begin()` for a transaction with automatic rollback on failure:

```java
try (var tx = conn.begin()) {
    tx.query("INSERT INTO orders VALUES ($1, $2)", 1, "widget");
    tx.query("INSERT INTO audit_log VALUES ($1)", "created order");
    tx.commit();
}  // auto-rollback if commit() not called
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.begin().use { tx ->
    tx.query("INSERT INTO orders VALUES (\$1, \$2)", 1, "widget")
    tx.query("INSERT INTO audit_log VALUES (\$1)", "created order")
    tx.commit()
}  // auto-rollback if commit() not called
```

</details>

Transactions compose with pipelining — entire transaction in one roundtrip:

```java
try (var tx = conn.begin()) {
    tx.enqueue("INSERT INTO orders VALUES ($1, $2)", 1, "widget");
    tx.enqueue("INSERT INTO orders VALUES ($1, $2)", 2, "gadget");
    tx.enqueue("INSERT INTO audit_log VALUES ($1)", "batch insert");
    tx.flush();
    tx.commit();
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.begin().use { tx ->
    tx.enqueue("INSERT INTO orders VALUES (\$1, \$2)", 1, "widget")
    tx.enqueue("INSERT INTO orders VALUES (\$1, \$2)", 2, "gadget")
    tx.enqueue("INSERT INTO audit_log VALUES (\$1)", "batch insert")
    tx.flush()
    tx.commit()
}
```

</details>

#### Rollback-only connections for testing

Since `Transaction` implements `Connection`, you can use it as a rollback-only connection
in tests. Pass the transaction to your code under test, and let it auto-rollback on close —
no test data is ever committed:

```java
try (var tx = conn.begin()) {
    // pass tx anywhere that expects a Connection
    var repo = new UserRepository(tx);
    repo.createUser("Alice", 30);

    // verify
    RowSet rs = tx.query("SELECT * FROM users WHERE name = $1", "Alice");
    assertEquals(1, rs.size());

    // no tx.commit() — auto-rollback on close, database is unchanged
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.begin().use { tx ->
    val repo = UserRepository(tx)
    repo.createUser("Alice", 30)

    val rs = tx.query("SELECT * FROM users WHERE name = \$1", "Alice")
    assertEquals(1, rs.size())

    // no tx.commit() — auto-rollback on close, database is unchanged
}
```

</details>

This keeps tests isolated without requiring database cleanup or schema resets between runs.

### Cursors

For large result sets, use a cursor to read rows in batches (requires a transaction):

```java
conn.query("BEGIN");
try (var cursor = conn.cursor("SELECT * FROM big_table WHERE category = $1", "active")) {
    while (cursor.hasMore()) {
        RowSet batch = cursor.read(100);  // fetch 100 rows at a time
        for (Row row : batch) {
            process(row);
        }
    }
}
conn.query("COMMIT");
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.query("BEGIN")
conn.cursor("SELECT * FROM big_table WHERE category = \$1", "active").use { cursor ->
    while (cursor.hasMore()) {
        val batch = cursor.read(100)  // fetch 100 rows at a time
        for (row in batch) {
            process(row)
        }
    }
}
conn.query("COMMIT")
```

</details>

### Streaming

For large result sets where you don't need all rows in memory at once, use the streaming API.
Rows are read from the wire one at a time — constant memory regardless of result size.

**Callback style** — simplest, no resource management:

```java
conn.queryStream("SELECT * FROM big_table", row -> {
    process(row.getString("name"), row.getInteger("id"));
});

// With parameters
conn.queryStream("SELECT * FROM orders WHERE status = $1", row -> {
    process(row);
}, "pending");
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.queryStream("SELECT * FROM big_table") { row ->
    process(row.getString("name"), row.getInteger("id"))
}

conn.queryStream("SELECT * FROM orders WHERE status = \$1", { row ->
    process(row)
}, "pending")
```

</details>

**Iterable/Stream style** — for composition with `for` loops, iterators, or `java.util.stream`:

```java
// Iterable — use with for-each
try (var rows = conn.stream("SELECT * FROM big_table")) {
    for (Row row : rows) {
        process(row);
    }
}

// java.util.stream.Stream — filter, map, collect
try (var rows = conn.stream("SELECT * FROM big_table WHERE active")) {
    List<String> names = rows.stream()
        .filter(row -> row.getInteger("age") > 21)
        .map(row -> row.getString("name"))
        .toList();
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
conn.stream("SELECT * FROM big_table").use { rows ->
    for (row in rows) {
        process(row)
    }
}

conn.stream("SELECT * FROM big_table WHERE active").use { rows ->
    val names = rows.stream()
        .filter { it.getInteger("age")!! > 21 }
        .map { it.getString("name") }
        .toList()
}
```

</details>

`RowStream` implements `AutoCloseable` and `Iterable<Row>`. Always close it (via try-with-resources)
to drain remaining server messages and keep the connection usable. Closing early is safe — unread
rows are discarded.

Unlike cursors, streaming does not require a transaction. Compared to cursors, streaming is simpler
(no batch size management) but only supports a single forward pass.

### Data Types

Row provides typed getters for common types:

```java
row.getString("name");           // text, varchar, char
row.getInteger("id");            // int2, int4
row.getLong("big_id");           // int8
row.getDouble("price");          // float8
row.getBigDecimal("amount");     // numeric, decimal
row.getBoolean("active");        // boolean
row.getLocalDate("created");     // date
row.getLocalTime("start_time");  // time
row.getLocalDateTime("updated"); // timestamp
row.getOffsetDateTime("ts");     // timestamptz
row.getInstant("ts");            // timestamptz → java.time.Instant
row.getOffsetTime("t");          // timetz (Postgres only)
row.getUUID("ref");              // uuid
row.getBytes("data");            // bytea
```

All getters accept either a column index (`int`) or column name (`String`).
NULL values return `null` — check with `row.isNull("col")`.

#### Postgres-specific types

The `io.github.bpdbi.pg.data` package provides types for Postgres geometric, network,
and temporal types. They are decoded directly from binary wire format via parameterized queries:

```java
// Geometric types
Point p = row.get("location", Point.class);        // (x,y)
Circle c = row.get("area", Circle.class);          // <(x,y),r>
Box b = row.get("bounds", Box.class);              // (x1,y1),(x2,y2)
Polygon poly = row.get("region", Polygon.class);   // ((x1,y1),(x2,y2),...)

// Network types
Inet addr = row.get("ip", Inet.class);             // 192.168.1.1/24

// Interval
Interval iv = row.get("duration", Interval.class); // 1 year 2 mons 3 days
Duration d = iv.toDuration();
```

<details><summary>Kotlin equivalent</summary>

```kotlin
// Geometric types
val p = row.get("location", Point::class.java)        // (x,y)
val c = row.get("area", Circle::class.java)           // <(x,y),r>
val b = row.get("bounds", Box::class.java)            // (x1,y1),(x2,y2)
val poly = row.get("region", Polygon::class.java)     // ((x1,y1),(x2,y2),...)

// Network types
val addr = row.get("ip", Inet::class.java)            // 192.168.1.1/24

// Interval
val iv = row.get("duration", Interval::class.java)    // 1 year 2 mons 3 days
val d = iv.toDuration()
```

</details>

#### JSON columns

Bpdbi has pluggable JSON support via the `JsonMapper` interface. Provide your own implementation
backed by Jackson, Gson, Moshi, or any other JSON library:

```java
// Wire up your JSON library (Jackson example)
var objectMapper = new ObjectMapper();
conn.setJsonMapper(new JsonMapper() {
    public <T> T fromJson(String json, Class<T> type) {
        return objectMapper.readValue(json, type);
    }
    public String toJson(Object value) {
        return objectMapper.writeValueAsString(value);
    }
});
```

**Auto-detection for JSON/JSONB columns** — if the database column type is `json`, `jsonb` (
Postgres),
or `JSON` (MySQL), `row.get()` automatically deserializes:

```java
// metadata is a jsonb column — auto-detected, no extra config needed
OrderMeta meta = conn.query("SELECT metadata FROM orders WHERE id = $1", 1)
    .first()
    .get("metadata", OrderMeta.class);
```

**Opt-in for JSON in text columns** — if your JSON lives in a `text`/`varchar` column,
register the type explicitly:

```java
conn.binderRegistry().registerAsJson(OrderMeta.class);

// Now works even though config_text is a text column
OrderMeta meta = conn.query("SELECT config_text FROM orders WHERE id = $1", 1)
    .first()
    .get("config_text", OrderMeta.class);
```

**Parameter binding** — registered JSON types are automatically serialized when used as query
parameters:

```java
conn.binderRegistry().registerAsJson(OrderMeta.class);
conn.query("INSERT INTO orders (id, metadata) VALUES ($1, $2::jsonb)",
    1, new OrderMeta("rush", 3));
// OrderMeta is serialized to JSON via jsonMapper.toJson()
```

Without a `JsonMapper` set, JSON columns are still available as raw strings via `row.getString()`.

<details><summary>Kotlin equivalent (using bpdbi-kotlin)</summary>

With `bpdbi-kotlin`, JSON support uses `kotlinx.serialization` instead of `JsonMapper`.
Mark JSON fields with `@SqlJsonValue` — no setup needed:

```kotlin
@Serializable
data class OrderMeta(val source: String, val priority: Int)

@Serializable
data class Order(
    val id: Int,
    val userId: Int,
    @SqlJsonValue val meta: OrderMeta  // jsonb or text column containing JSON
)

// Auto-deserializes the meta column from JSON
val orders: List<Order> = conn.queryAs("SELECT id, user_id, meta FROM orders")

// Works for jsonb columns and text columns alike — @SqlJsonValue handles both
val order: Order? = conn.queryOneAs(
    "SELECT id, user_id, meta FROM orders WHERE id = \$1", 1)
```

</details>

### Batch Execution

Execute the same statement with multiple parameter sets efficiently:

```java
List<RowSet> results = conn.executeMany(
    "INSERT INTO users (name, age) VALUES ($1, $2)",
    List.of(
        new Object[]{"Alice", 30},
        new Object[]{"Bob", 25},
        new Object[]{"Carol", 35}
    ));

// Each result corresponds to one parameter set
for (RowSet rs : results) {
    System.out.println("Rows affected: " + rs.rowsAffected());
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val results = conn.executeMany(
    "INSERT INTO users (name, age) VALUES (\$1, \$2)",
    listOf(
        arrayOf("Alice", 30),
        arrayOf("Bob", 25),
        arrayOf("Carol", 35)
    ))

for (rs in results) {
    println("Rows affected: ${rs.rowsAffected()}")
}
```

</details>

`executeMany` composes with pipelining — all executions are sent in one network roundtrip.

Works with transactions too:

```java
try (var tx = conn.begin()) {
    tx.executeMany(
        "INSERT INTO ledger (account, amount) VALUES ($1, $2)",
        List.of(new Object[]{"A", 100}, new Object[]{"B", -100}));
    tx.commit();
}
```

### IN-list Expansion

Named parameters support automatic expansion for collections and arrays:

```java
// List of IDs — :ids expands to $1, $2, $3
RowSet rs = conn.query(
    "SELECT * FROM users WHERE id IN (:ids)",
    Map.of("ids", List.of(1, 2, 3)));

// Mixed scalar and collection parameters
RowSet rs2 = conn.query(
    "SELECT * FROM users WHERE status = :status AND id IN (:ids)",
    Map.of("status", "active", "ids", List.of(1, 2, 3)));
// Becomes: SELECT * FROM users WHERE status = $1 AND id IN ($2, $3, $4)
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val rs = conn.query(
    "SELECT * FROM users WHERE id IN (:ids)",
    mapOf("ids" to listOf(1, 2, 3)))

val rs2 = conn.query(
    "SELECT * FROM users WHERE status = :status AND id IN (:ids)",
    mapOf("status" to "active", "ids" to listOf(1, 2, 3)))
```

</details>

Supports `Collection` (List, Set) and arrays (including primitive arrays like `int[]`).
Empty collections produce `NULL` to avoid SQL syntax errors.

### LISTEN/NOTIFY (Postgres)

Subscribe to Postgres asynchronous notifications:

```java
try (var conn = PgConnection.connect("localhost", 5432, "mydb", "user", "pass")) {
    conn.listen("order_events");

    // ... do other work, or poll in a loop ...

    // Notifications are buffered as they arrive during query processing.
    // Trigger a roundtrip to collect any pending notifications:
    conn.query("SELECT 1");

    for (PgNotification n : conn.getNotifications()) {
        System.out.println("Channel: " + n.channel() + ", Payload: " + n.payload());
    }

    conn.unlisten("order_events");
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
PgConnection.connect("localhost", 5432, "mydb", "user", "pass").use { conn ->
    conn.listen("order_events")

    conn.query("SELECT 1")

    for (n in conn.getNotifications()) {
        println("Channel: ${n.channel()}, Payload: ${n.payload()}")
    }

    conn.unlisten("order_events")
}
```

</details>

Send notifications with `notify(channel, payload)`. Use `unlistenAll()` to unsubscribe from
everything.
Notifications arrive as `PgNotification` records with `processId()`, `channel()`, and `payload()`.

### Cancel Request (Postgres)

Cancel a long-running query from another thread:

```java
var conn = PgConnection.connect("localhost", 5432, "mydb", "user", "pass");

// Start a long-running query on a virtual thread
var queryThread = Thread.startVirtualThread(() -> {
    try {
        conn.query("SELECT pg_sleep(300)");
    } catch (DbException e) {
        System.out.println("Query cancelled: " + e.getMessage());
    }
});

// Cancel it after a timeout
Thread.sleep(1000);
conn.cancelRequest();
queryThread.join();
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val conn = PgConnection.connect("localhost", 5432, "mydb", "user", "pass")

// Start a long-running query in a coroutine
val job = scope.launch(Dispatchers.IO) {
    try {
        conn.query("SELECT pg_sleep(300)")
    } catch (e: DbException) {
        println("Query cancelled: ${e.message}")
    }
}

// Cancel it after a timeout
delay(1000)
conn.cancelRequest()
job.join()
```

</details>

## Connection Pooling

Bpdbi ships its own lightweight connection pool in `bpdbi-pool`:

```java
var pool = new ConnectionPool(
    () -> PgConnection.connect("localhost", 5432, "mydb", "user", "pass"),
    new PoolConfig()
        .maxSize(10)
        .maxIdleTimeMillis(300_000)     // 5 minutes
        .maxLifetimeMillis(1_800_000)   // 30 minutes
        .connectionTimeoutMillis(5000)  // 5 seconds
        .validateOnBorrow(true)         // ping idle connections before use
        .leakDetectionThresholdMillis(60_000)); // warn after 60s unreturned

// Borrow, use, return automatically
pool.withConnection(conn -> {
    conn.query("SELECT 1");
});

// Or use try-with-resources — close() returns the connection to the pool
try (Connection conn = pool.acquire()) {
    conn.query("SELECT 1");
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val pool = ConnectionPool(
    { PgConnection.connect("localhost", 5432, "mydb", "user", "pass") },
    PoolConfig()
        .maxSize(10)
        .maxIdleTimeMillis(300_000)
        .maxLifetimeMillis(1_800_000)
        .connectionTimeoutMillis(5000)
        .validateOnBorrow(true)
        .leakDetectionThresholdMillis(60_000))

pool.withConnection { conn ->
    conn.query("SELECT 1")
}

// Or use Kotlin's .use { } — close() returns the connection to the pool
pool.acquire().use { conn ->
    conn.query("SELECT 1")
}
```

</details>

**Pool configuration options:**

| Option                         | Default          | Description                                              |
|--------------------------------|------------------|----------------------------------------------------------|
| `maxSize`                      | 10               | Maximum number of connections                            |
| `maxIdleTimeMillis`            | 600,000 (10 min) | Evict idle connections after this time. 0 = disabled     |
| `maxLifetimeMillis`            | 0 (disabled)     | Evict connections after this total lifetime              |
| `connectionTimeoutMillis`      | 30,000 (30s)     | Max wait time when pool is exhausted                     |
| `maxWaitQueueSize`             | -1 (unbounded)   | Max threads waiting for a connection. -1 = unbounded     |
| `poolCleanerPeriodMillis`      | 1,000 (1s)       | Background eviction check interval                       |
| `validateOnBorrow`             | false            | Ping connections before handing them out                 |
| `leakDetectionThresholdMillis` | 0 (disabled)     | Log a warning when a connection is held longer than this |

Connections returned via `close()` go back to the pool (not closed). The pool tracks
active connections and runs a background cleaner for eviction and leak detection.

For heavier requirements, any generic object pool library
(like [Apache Commons Pool 2](https://commons.apache.org/proper/commons-pool/)) would also work
well.
Note that HikariCP is JDBC-specific and thus not compatible.

This works with any driver — just swap `PgConnection.connect(...)` for
`MysqlConnection.connect(...)`.

## Recommended HTTP frameworks

Bpdbi uses blocking I/O and is designed for virtual threads — it pairs well with HTTP frameworks that
are not mandatorily reactive/async:

- **[http4k](https://www.http4k.org/)** — Functional, zero-reflection, tiny. The philosophical twin
  of bpdbi on the HTTP side.
- **[Javalin](https://javalin.io/)** — Minimal Jetty wrapper with built-in virtual thread support.
  Very popular in both Java and Kotlin.
- **[Helidon SE](https://helidon.io/) 4+** — Oracle's lightweight framework. Versions 1–3 were
  reactive (Reactive Streams); 4.x was rewritten around virtual threads and blocking I/O.
- **[Undertow](https://undertow.io/)** — Embedded, low-level. Blocking handlers run on a worker
  thread pool (or virtual threads).
- **[Micronaut](https://micronaut.io/)** — Compile-time DI, GraalVM-first. Supports both reactive
  and imperative — controller methods can simply return values.
- **[Spark](https://sparkjava.com/)** — Dead-simple Java micro-framework with the same "just enough" philosophy.
- **[Jooby](https://jooby.io/)** — Modular micro-framework, explicit about dependencies, virtual
  thread support.
- **`com.sun.net.httpserver`** — The JDK's built-in HTTP server. Zero dependencies, pairs naturally
  with bpdbi's minimalism.

Frameworks like Spring Boot are opinionated about their own data stacks (Spring Data, Hibernate) and
assume a JDBC `DataSource` integration for transactions, health checks, and connection management.

Quarkus and the underlying Vert.x are reactive/async frameworks, so not a good fit for Bpdbi.
That said, Bpdbi started as a port of the `vertx-sql-client` package!

## Modules

* `bpdbi-bom`                       BOM (Bill of Materials) for version alignment
* `bpdbi-core`                      Database-agnostic API (Connection, Row, RowSet, pipelining logic)
* `bpdbi-pg-client`                 Postgres driver (wire protocol, auth, PG types)
* `bpdbi-mysql-client`              MySQL driver (wire protocol, auth)
* `bpdbi-pool`                      Simple connection pool with idle/lifetime eviction
* `bpdbi-kotlin`                    Kotlin extensions (kotlinx.serialization-based row decoding)
* `bpdbi-record-mapper`             Reflection-based Java record mapping (GraalVM metadata included)
* `bpdbi-javabean-mapper`           Reflection-based JavaBean (POJO) mapping (GraalVM metadata
  included)

Each driver implements its own wire protocol by extending `BaseConnection`.
The pipelining machinery, result types, and public API are shared.

Use the BOM to align versions across modules:

```kotlin
dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-pg-client")    // no version needed
    implementation("io.github.bpdbi:bpdbi-pool")          // no version needed
}
```

See [`examples/`](examples/) for runnable examples.

## Requirements and dependencies

- JDK 21+
- JSpecify (3kB)
- Only for `bpdbi-pg-client`: `scram-client` a small (70kB) encryption library for SCRAM
  authentication
- Only for `bpdbi-pool`: Apache `commons-pool2` a battle tested pool that's not JDBC/DataSource
  specific
- Only for `bpdbi-kotlin`: `kotlin-stdlib` (for `kotlin.time` and `kotlin.uuid`) and
  `kotlinx-serialization`

## Develop

**Code formatting** — The project uses [Google Java Format](https://github.com/google/google-java-format)
via [Spotless](https://github.com/diffplug/spotless). To check and fix formatting:

```bash
./gradlew spotlessCheck   # check for violations
./gradlew spotlessApply   # auto-fix all violations
```

Your IDE will likely have a plugin for Google Java Format —
see https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides
for IntelliJ/Android Studio, or search your IDE's plugin marketplace.

**Build & test** — see [`CLAUDE.md`](CLAUDE.md) for build commands and project structure.

## Status

Early development. Not yet published to Maven Central — the dependency coordinates above are
*placeholders* for now.

## Todo

* Look into the testcases of our lib/jdbi RowMapper -- see if we have parity in Bpdbi
* Test for nested records
* Double check we have all kotlin's time types implemented
* Make explicit when it's positional mapping and when ist' "by name"
* Consider abstracting over pg/mysql parameter injection syntax ($1 vs ?): may be a bad idea (what
  doe Jdbi do?)
* allow nested transactions
* Publish to Maven Central

## License

Apache 2.0, same as **Vert.x**.
Massive respects to Vert.x, Claude Code and some of my internal library code for help and
inspiration.
