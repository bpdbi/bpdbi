# Djb — Pipelined blocking Postgres and MySQL on the JVM

A blocking SQL client for the JVM that treats **pipelining** as a first-class concept.

Ported from the battle-tested [Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client)
(the foundation of Quarkus), but stripped of all async/reactive machinery.
Plain `java.net.Socket` I/O. No Netty dependencies. No event loop. No `Uni<T>` (or other implementations of futures).

## Why?

JDBC is showing its age and (hence) does not allow [pipelining](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)
which is available in Postgres 13+ and MySQL 8.0.3+.

Vert.x (`vertx-sql-client`) does support pipelines (for these databases), but forces reactive/async programming.

Djb gives you pipelining with straightforward blocking code — ideal for **Java 21+ virtual threads**,
where blocking is inexpensive and readability of the code matters more than a few percent better throughput.

Pipelining sends multiple statements to the database in a single network write and reads all responses back at once.
Reducing the number db roundtrips per HTTP request cycle significantly improves performance:

```java
// 4 statements, 1 roundtrip
conn.enqueue("SET search_path TO myschema");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)");
RowSet result = conn.query("SELECT * FROM my_table WHERE id = $1", 42);
```

Without pipelining each of those would be a separate db roundtrip.
Over a network with 1ms latency, that's 4ms saved on every request — and it adds up.

Finally: dependency minimalism. Using Djb incurs a tiny dependency (<100k) compared to Vert.x/Netty (5MB+)
or JDBC alternatives (the postgres is ~1.1MB and MySQL Connector/J is ~2.5MB).
Djb also provides functionality commonly found libraries like Jdbi (which clocks in at ~1MB), or Spring Data Code (~3MB).
Djb does not use any JVM reflection API out of the box (some mapper implementations may use it though).

## Design Principles

**Pipelining first** — The `enqueue()`/`flush()` API is not an afterthought.
Simple `query()` calls participate in the same pipeline machinery,
so you get batched I/O without restructuring your code.
Djb has no separate "batch" API — pipelining is a strict superset of batching.
A batch sends N copies of the same statement with different parameters;
a pipeline sends N arbitrary statements (different SQL, different parameter counts)
in a single network write. Anything you'd do with batch, you can do with pipeline —
plus mix in SETs, DDL, transactions, and different queries in the same roundtrip.

**Lazy decoding** — `Row` stores raw bytes from the wire and decodes them
only when you call a typed getter (`getInteger`, `getString`, etc.).
Columns you never read are never decoded, keeping CPU overhead minimal.

**Pluggable type system** — `TypeRegistry` (Java → SQL) and `MapperRegistry` (SQL → Java)
let you register custom converters for your domain types without forking the library or relying on reflection.

**Null-safe API** — The entire public API is annotated with [JSpecify](https://jspecify.dev/) (`@NullMarked` / `@Nullable`).
IDEs and tools like NullAway can statically verify correct null handling at compile time.

**No compile-time SQL validation** — Djb does not attempt to type-check the boundary between
your JVM code and the database. SQL strings are opaque at compile time, just like in JDBC.
Tools that do validate SQL at compile time (e.g. jOOQ, SQLC, SqlDelight) add significant
complexity and code generation steps. We believe a simpler policy works well in practice:
write one integration test per SQL query that exercises it against a real database instance.
These tests are straightforward to generate with AI assistance and catch schema mismatches,
typos, and type errors at test time rather than compile time — with far less machinery.

**Minimal dependencies** — Just `java.net.Socket`, a tiny SCRAM library (Postgres only),
and JSpecify annotations (a single 3KB jar).
No Netty, no reactive runtime, no reflection by default — ideal for GraalVM native images.

## Quick Start

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
row.getUUID("ref");              // uuid
row.getBytes("data");            // bytea
```

All getters accept either a column index (`int`) or column name (`String`).
NULL values return `null` — check with `row.isNull("col")`.

#### Postgres-specific types

The `io.djb.pg.data` package provides types for Postgres geometric, network,
and temporal types. They are read as strings and parsed:

```java
// Geometric types
Point p = Point.parse(row.getString("location"));        // (x,y)
Circle c = Circle.parse(row.getString("area"));          // <(x,y),r>
Box b = Box.parse(row.getString("bounds"));              // (x1,y1),(x2,y2)
Polygon poly = Polygon.parse(row.getString("region"));   // ((x1,y1),(x2,y2),...)

// Network types
Inet addr = Inet.parse(row.getString("ip"));             // 192.168.1.1/24

// Interval
Interval iv = Interval.parse(row.getString("duration")); // 1 year 2 mons 3 days
Duration d = iv.toDuration();
```

<details><summary>Kotlin equivalent</summary>

```kotlin
// Geometric types
val p = Point.parse(row.getString("location"))        // (x,y)
val c = Circle.parse(row.getString("area"))           // <(x,y),r>
val b = Box.parse(row.getString("bounds"))            // (x1,y1),(x2,y2)
val poly = Polygon.parse(row.getString("region"))     // ((x1,y1),(x2,y2),...)

// Network types
val addr = Inet.parse(row.getString("ip"))            // 192.168.1.1/24

// Interval
val iv = Interval.parse(row.getString("duration"))    // 1 year 2 mons 3 days
val d = iv.toDuration()
```

</details>

#### JSON columns

Djb has pluggable JSON support via the `JsonMapper` interface. Provide your own implementation
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

**Auto-detection for JSON/JSONB columns** — if the database column type is `json`, `jsonb` (Postgres),
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
conn.typeRegistry().registerAsJson(OrderMeta.class);

// Now works even though config_text is a text column
OrderMeta meta = conn.query("SELECT config_text FROM orders WHERE id = $1", 1)
    .first()
    .get("config_text", OrderMeta.class);
```

**Parameter binding** — registered JSON types are automatically serialized when used as query parameters:

```java
conn.typeRegistry().registerAsJson(OrderMeta.class);
conn.query("INSERT INTO orders (id, metadata) VALUES ($1, $2::jsonb)",
    1, new OrderMeta("rush", 3));
// OrderMeta is serialized to JSON via jsonMapper.toJson()
```

Without a `JsonMapper` set, JSON columns are still available as raw strings via `row.getString()`.

<details><summary>Kotlin equivalent (using djb-kotlin)</summary>

With `djb-kotlin`, JSON support uses `kotlinx.serialization` instead of `JsonMapper`.
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

Djb doesn't bundle a connection pool — use any generic object pool library.
Djb is not a JDBC DataSource provider, so the popular [HikariCP](https://github.com/brettwooldridge/HikariCP) is not a good fit.

Here an example with the solid [Apache Commons Pool 2](https://commons.apache.org/proper/commons-pool/) library:

```java
var factory = new BasePooledObjectFactory<Connection>() {
    @Override
    public Connection create() {
        return PgConnection.connect("localhost", 5432, "mydb", "user", "pass");
    }
    @Override
    public PooledObject<Connection> wrap(Connection conn) {
        return new DefaultPooledObject<>(conn);
    }
    @Override
    public void destroyObject(PooledObject<Connection> p) {
        p.getObject().close();
    }
};

var pool = new GenericObjectPool<>(factory);
pool.setMaxTotal(10);
pool.setMaxIdle(5);
pool.setMaxWaitMillis(30_000);

var conn = pool.borrowObject();
try {
    conn.query("SELECT 1");
} finally {
    pool.returnObject(conn);
}
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val factory = object : BasePooledObjectFactory<Connection>() {
    override fun create(): Connection =
        PgConnection.connect("localhost", 5432, "mydb", "user", "pass")
    override fun wrap(conn: Connection): PooledObject<Connection> =
        DefaultPooledObject(conn)
    override fun destroyObject(p: PooledObject<Connection>) =
        p.`object`.close()
}

val pool = GenericObjectPool(factory).apply {
    maxTotal = 10
    maxIdle = 5
    maxWaitMillis = 30_000
}

val conn = pool.borrowObject()
try {
    conn.query("SELECT 1")
} finally {
    pool.returnObject(conn)
}
```

</details>

This works with any driver — just swap `PgConnection.connect(...)` for `MysqlConnection.connect(...)`.

## Recommended HTTP frameworks

Djb uses blocking I/O and is designed for virtual threads — it pairs well with HTTP frameworks that are not mandatorily reactive/async:

- **[http4k](https://www.http4k.org/)** — Functional, zero-reflection, tiny. The philosophical twin of djb on the HTTP side.
- **[Javalin](https://javalin.io/)** — Minimal Jetty wrapper with built-in virtual thread support. Very popular in both Java and Kotlin.
- **[Helidon SE](https://helidon.io/) 4+** — Oracle's lightweight framework. Versions 1–3 were reactive (Reactive Streams); 4.x was rewritten around virtual threads and blocking I/O.
- **[Undertow](https://undertow.io/)** — Embedded, low-level. Blocking handlers run on a worker thread pool (or virtual threads).
- **[Micronaut](https://micronaut.io/)** — Compile-time DI, GraalVM-first. Supports both reactive and imperative — controller methods can simply return values.
- **[Spark](https://sparkjava.com/)** — Dead-simple Java micro-framework with the same "just enough" philosophy.
- **[Jooby](https://jooby.io/)** — Modular micro-framework, explicit about dependencies, virtual thread support.
- **`com.sun.net.httpserver`** — The JDK's built-in HTTP server. Zero dependencies, pairs naturally with djb's minimalism.

Frameworks like Spring Boot is opinionated about its own data stacks (Spring Data, Hibernate) and assumes a JDBC
`DataSource` integration for transactions, health checks, and connection management.

Quarkus and the underlying Vert.x are reactive/async frameworks, so not a good fit for Djb:
the Djb implementation is based on the `vertx-sql-client` package!

## Modules

```
djb-core                      Database-agnostic API (Connection, Row, RowSet, pipelining logic)
djb-pg-client                 Postgres driver (wire protocol, auth, PG types)
djb-mysql-client              MySQL driver (wire protocol, auth)
djb-kotlin                    Kotlin extensions (kotlinx.serialization-based row decoding)
djb-reflective-record-mapper  Reflection-based Java record mapping
djb-javabean-mapper           Reflection-based JavaBean (POJO) mapping
```

Each driver implements its own wire protocol by extending `BaseConnection`.
The pipelining machinery, result types, and public API are shared.

See [`examples/`](examples/) for runnable examples.

## Requirements and dependencies

- JDK 21+
- JSpecify
- Only for `djb-pg-client`: `scram-client` (a tiny encryption library for SCRAM authentication)

## Status

Early development.
Not yet published to Maven Central.

## Todo

* Kotlin support
* Document Value-binding like in Jdbi (does this need to be pluggable?)
* Document Result-mapping like in Jdbi (but fully pluggable: kotlin implements with kotlinx.serialization)
* Add MapStruct implementation
* Add ModelMapper implementation
* Install original copyright notices for files that are very similar to their Vertx counterparts

## License

Apache 2.0, same as **Vert.x**.
