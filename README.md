bpdbi
=====
### Blocking Pipelined Database Interface for Postgres on the JVM

[![CI](https://github.com/bpdbi/bpdbi/actions/workflows/ci.yml/badge.svg)](https://github.com/bpdbi/bpdbi/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bpdbi/bpdbi/graph/badge.svg)](https://codecov.io/gh/bpdbi/bpdbi)
[![License](https://img.shields.io/badge/License-BSD--2--Clause-blue.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.bpdbi/bpdbi-core)](https://maven-badges.herokuapp.com/maven-central/io.github.bpdbi/bpdbi-core)
[![Javadocs](http://javadoc.io/badge/io.github.bpdbi/bpdbi-core.svg)](http://javadoc.io/doc/io.github.bpdbi/bpdbi-core)

A blocking database library the JVM that treats **pipelining** as a first-class concept.

Initially ported from the battle-tested [Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client)
(Vert.x is the foundation of Quarkus), but stripped of all async/reactive machinery; thus "blocking".
Since it's blocking, `java.net.Socket` is used for I/O (no Netty dependencies, event loop, `Uni<T>`,
Kotlin coroutines or other implementations of the "future" pattern).

This library —like Vert.x SQL— does not use JDBC, as JDBC does not expose pipelining.
Since Bpdbi is not constrained by JDBC, a developer experience similar to [Jdbi](https://jdbi.org),
[Spring JDBC Template](https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/jdbc/core/JdbcTemplate.html)
or [Sql2o](http://sql2o.org) is provided out-of-the-box.

## Why?

JDBC is [showing its age](docs/is-jdbc-showing-its-age.md): it does not allow pipelining, which is available since
[Postgres 14+](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html);
and only provides a text-based API (while most databases support binary protocols nowadays).

Vert.x (`vertx-sql-client`) does support pipelines (for Postgres) —it does not use JDBC—
but forces reactive/async programming and
[this style of programming comes at a cost](docs/why-not-write-all-code-reactive.md).

Bpdbi provides pipelining for straightforward blocking code — ideal for **Java 21+ virtual threads**,
which made blocking a lot more performant.

This library is a lot smaller than the typical JVM db stack, where a db driver, JDBC, and
something like Jdbi are needed (easily several MB of libraries), where Bpdbi is under 200kB.

## Why pipelining?

Pipelining sends multiple statements to the database in a single network write and reads all
responses back at once.

It can be used to reduce the number of db roundtrips: this is a great way to improve the performance
of, say, an HTTP request cycle.

The need for pipelining is more urgent when using certain features like Postgres' RLS
(row-level security): an important technique to prevent data leaks in typical SaaS applications.

For example, queries for which we are not interested in the result (like starting a transaction,
setting the JWT which is needed if you want to use Supabase's RLS, or other settings)
can usually be sent alongside the first query that we **are** interested in the result of.

```java
// 4 statements, 1 roundtrip
conn.enqueue("begin");
conn.enqueue("set statement_timeout to '5s'");
conn.enqueue("set local role authenticated");
conn.enqueue("select set_config('request.jwt.claims', json_build_object('sub', ...)::text, true)");
RowSet result = conn.query("select * from my_table where id = $1", 42);
```

Without pipelining each of those would be a separate db roundtrip.
Over a network with 1ms roundtrip latency, that means at least 4ms is saved on every HTTP request
as you likely do more than just one query.

Multiple db queries for which you **do** want results can also be combined in a pipeline.
`enqueue()` returns an index, and `flush()` returns a `List<RowSet>` — see [Pipelining](#pipelining).

## Benchmarks

JMH benchmarks with 1ms Toxiproxy-simulated network latency per direction (2ms roundtrip),
JDK 21, single-threaded. All numbers are **ops/s** (higher is better).

| Scenario | bpdbi | JDBC (pgjdbc) |  Speedup |
|---|--:|--:|---------:|
| **Pipelined lookups** (10 SELECTs) | 310 | 18 |  **17x** |
| **Pipelined read-only tx** (BEGIN+SELECT+COMMIT) | 360 | 185 |   **2x** |
| **Pipelined inserts in tx** (10 INSERTs) | 116 | 18 | **6.5x** |
| **Bulk insert** (100 rows) | 313 | 171 | **1.8x** |
| **Cursor fetch** (1000 rows) | 281 | 30 | **9.3x** |
| **Large value streaming** | 152 | 82 | **1.9x** |
| Single row lookup | 370 | 370 |   on par |
| Multi-row fetch (10 rows) | 358 | 358 |   on par |
| Join query | 271 | 271 |   on par |

Anything that touches the network more than once — transactions, batches, sequential lookups —
gets a **5-17x** speedup from pipelining. Single-query performance is on par with raw `pgjdbc`:
the overhead is zero and results are purely network-bound.

For the full breakdown, including mapper comparisons and Hibernate numbers, see
[docs/benchmark-interpretation.md](docs/benchmark-interpretation.md).

## Design Principles

**Pipelining first** — The `enqueue()`/`flush()` API is not an afterthought.
Simple `query()` calls participate in the same pipeline machinery,
so you get batched I/O without restructuring your code.
Bpdbi has no separate "batch" API — pipelining is a strict superset of batching.
A batch sends N copies of the same statement with different parameters;
a pipeline sends N arbitrary statements (different SQL, different parameter counts)
in a single network write. Anything you'd do with batch, you can do with pipeline —
plus mix in SETs, DDL, transactions, and different queries in the same roundtrip.

**Binary protocol everywhere** — Bpdbi's Postgres driver uses the extended query protocol
(Parse/Bind/Execute) with binary result format for **all** queries, including parameterless ones
like `BEGIN`, `COMMIT`, `SET`, and plain `SELECT`s. This is a deliberate departure from
`pgjdbc` and most other drivers, which use the *simple query protocol* (text format) for
parameterless statements. The always-binary design has three significant consequences:

1. **Uniform pipelining.** Because every statement uses the same wire protocol,
   `enqueue("BEGIN")` + `enqueue("SELECT ... WHERE id = $1", 42)` + `enqueue("COMMIT")`
   + `flush()` goes through a single `executePipelinedBatch` → one TCP write → one Sync →
   **one roundtrip**. Drivers that use the simple protocol for `BEGIN`/`COMMIT` cannot
   pipeline them with extended-protocol queries — they need separate roundtrips.
   This is why `conn.begin()` enqueues `BEGIN` lazily instead of flushing it immediately,
   saving one roundtrip on every transaction.

2. **Binary results for all queries.** Even `SELECT count(*) FROM t` (no parameters)
   returns integers in binary (4 raw bytes) rather than text (`"12345"`), avoiding
   text parsing overhead. Numeric, temporal, and UUID types benefit most.

3. **No multi-statement strings.** Postgres's extended protocol rejects
   `"SELECT 1; SELECT 2"` — use `enqueue()`/`flush()` instead, which is both
   more explicit and faster.

4. **Simpler internals.** A lot of code we do not need to maintain, thus a smaller footprint.

**Lazy decoding with column-oriented storage** — `Row` stores raw bytes from the wire and decodes
them only when you call a typed getter (`getInteger`, `getString`, etc.).
Columns you never read are never decoded, keeping CPU overhead minimal.
Buffered result sets use column-oriented storage internally: all values for a given column
are packed into a single contiguous `byte[]` buffer, and each `Row` is a lightweight view
(a column-buffer reference + a row index) with no per-row allocation.
For a 100K-row, 10-column result, this means 10 byte arrays instead of 1 million —
dramatically reducing GC pressure for large results.

**One connection per thread** — Connections are not thread-safe, like JDBC and virtually every
SQL client in any language. The underlying wire protocol is a stateful, ordered byte stream —
concurrent writes from multiple threads would corrupt it. Transactions are also connection-scoped,
so sharing a connection between threads would mix transaction boundaries.
The standard pattern is: borrow a connection from a [pool](#connection-pooling), use it, return it.
With Java 21+ virtual threads, blocking on a pooled connection is cheap, so thousands of
concurrent requests each get their own connection naturally.

**Pluggable type system** — `TypeRegistry` lets you register custom type mappings (both JVM → SQL
and SQL → JVM) for your domain types without forking the library or relying on reflection.

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

**Dependency minimalism** — Bpdbi incurs a tiny dependency (<100k) compared to Vert.x/Netty (5MB+),
the Postgres JDBC driver (&tilde;1.1MB). It also provides named parameters,
row mapping, and type binding commonly found in libraries like Jdbi (&tilde;1MB) or Spring Data JDBC
(&tilde;3MB). Bpdbi can be used without the JVM reflection API, some optional mapper modules do use it.
Mind you that libraries like Hibernate and jOOQ weigh in at about 15MB each.

**GraalVM native-image ready** — The core library and drivers (`bpdbi-core`, `bpdbi-pg-client`,
`bpdbi-pool`) make no use of reflection and work out of the box with
`native-image`. The optional mapper modules (`bpdbi-record-mapper`,
`bpdbi-javabean-mapper`) ship with GraalVM `reflect-config.json` metadata for their own
classes; you only need to register your application's record/bean types. The Kotlin module
uses compile-time code generation (kotlinx.serialization) and needs no reflection at all.

**JVM-based** — The library is written in Java (JDK 21+) to ensure maximum compatibility
with JVM-based projects. The `bpdbi-kotlin` module provides a Kotlin-friendly API, compatibility
with Kotlin types (`kotlin.time`, `kotlinx.datetime`) and a row mapper that's based on
`kotlinx.serialization` (which does **not** use reflection!).

## Quick Start

### Dependencies

```kotlin
// build.gradle.kts
dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-pg-client")                // Postgres driver
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
```

</details>

URI parsing supports `postgresql://` and `postgres://` schemes with
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

// Parameterized query ($1, $2, ...)
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

// Parameterized query ($1, $2, ...)
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

Named parameters are rewritten to positional placeholders (`$1`, `$2`)
before execution. They work with `query()`, `enqueue()`, and `prepare()`. The `::` cast operator
(e.g. `$1::int`) is correctly handled and not treated as a named parameter.

Collections and arrays are automatically expanded — see [IN-list Expansion](#in-list-expansion).

### Fluent Parameter Binding

For queries with many named parameters, the fluent `sql()` builder is more readable
than constructing a `Map`:

```java
List<User> users = conn.sql("SELECT * FROM users WHERE name = :name AND age > :age")
    .bind("name", "Alice")
    .bind("age", 30)
    .mapTo(userMapper);

// Pipelining with fluent binding
int idx = conn.sql("INSERT INTO log (msg, level) VALUES (:msg, :level)")
    .bind("msg", "hello")
    .bind("level", "INFO")
    .enqueue();
List<RowSet> results = conn.flush();

// Streaming
conn.sql("SELECT * FROM big_table WHERE status = :status")
    .bind("status", "active")
    .queryStream(row -> process(row));
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val users = conn.sql("SELECT * FROM users WHERE name = :name AND age > :age")
    .bind("name", "Alice")
    .bind("age", 30)
    .mapTo(userMapper)

// With kotlinx.serialization
val user = conn.sql("SELECT id, name, email FROM users WHERE id = :id")
    .bind("id", 42)
    .query()
    .deserializeFirstOrNull<User>()
```

</details>

The builder supports all execution modes: `query()`, `enqueue()`, `stream()`, `queryStream()`,
and convenience methods `mapTo()`, `mapFirst()`, `mapFirstOrNull()`.

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

Errors in one pipelined statement don't poison the others (each statement gets its own result):

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

### Cursors

Cursors read rows in batches via server-side portals (requires a transaction):

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

### Streaming vs Cursors

Both streaming and cursors avoid loading an entire result set into memory at once.
Use streaming by default — it is simpler and significantly faster. Cursors exist for the
few cases where streaming doesn't fit.

**Use streaming** (`stream()` / `queryStream()`) when:
- You want constant-memory iteration over a large result — this is the common case.
- You process rows in a single forward pass (filter, transform, aggregate, write to a file, etc.).
- You don't need a transaction for anything else.

**Use cursors** (`cursor()`) when:
- You need to **pause and resume** reading — e.g. read 100 rows, do external I/O, read the next 100.
  With streaming, the server sends all rows immediately and the client must consume them.
- You need **bounded memory per batch** with access to complete `RowSet` objects (column-oriented
  storage, random access within the batch, `mapTo()`, etc.). Streaming yields one `Row` at a time.
- You are already in a transaction and the cursor is a natural fit.

**Why the performance difference?** Streaming reads all rows from a single server response — one
network round-trip for the entire result. Cursors issue a `FETCH <n>` command per batch, each
requiring its own round-trip. With 1ms network latency and a fetch size of 100, reading 1000 rows
via a cursor adds &tilde;20ms of round-trip overhead (10 fetches x 2ms) that streaming avoids entirely.
In benchmarks, streaming is roughly **8x faster** than cursors for the same 1000-row result.

For most workloads — ETL pipelines, report generation, data export — streaming is the right choice.
Cursors are a specialized tool for interactive or externally-paced consumption patterns.

### Data Types

Row provides typed getters for common types:

```java
row.getString("name");           // text, varchar, char
row.getInteger("id");            // int2, int4
row.getLong("big_id");           // int8
row.getDouble("price");          // float8
row.getBigDecimal("amount");     // numeric, decimal
row.getBoolean("active");        // boolean
row.getInstant("created_at");    // timestamptz → java.time.Instant
row.getLocalDate("issued_on");   // date
row.getLocalTime("start_time");  // time
row.getLocalDateTime("start");   // timestamp
row.getOffsetTime("from_time");  // timetz (Postgres only)
row.getOffsetDateTime("t");      // timestamptz
row.getUUID("user_uuid");        // uuid
row.getBytes("data");            // bytea
```

All getters accept either a column index (`int`) or column name (`String`).
NULL values return `null` — check  `row.isNull(columnIndex)` or `row.isNull("columnName")`.

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
`row.get()` automatically deserializes:

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

**Parameter binding** — registered JSON types are automatically serialized when used as query
parameters:

```java
conn.typeRegistry().registerAsJson(OrderMeta.class);
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

### High-Throughput Bulk Inserts

For maximum INSERT throughput, construct a multi-row `VALUES` clause yourself. This sends
all rows in a single statement — one parse, one plan, one WAL entry — which is significantly
faster than executing N separate INSERTs, even with pipelining:

```java
// Build a multi-row INSERT for N rows with 3 columns each
int cols = 3;
var sql = new StringBuilder("INSERT INTO users (name, age, active) VALUES ");
var params = new ArrayList<>();
for (int i = 0; i < rows.size(); i++) {
    if (i > 0) sql.append(", ");
    int base = i * cols;
    sql.append("($").append(base + 1)
       .append(", $").append(base + 2)
       .append(", $").append(base + 3).append(")");
    params.add(rows.get(i).name());
    params.add(rows.get(i).age());
    params.add(rows.get(i).active());
}
conn.query(sql.toString(), params.toArray());
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val cols = 3
val sql = buildString {
    append("INSERT INTO users (name, age, active) VALUES ")
    rows.forEachIndexed { i, row ->
        if (i > 0) append(", ")
        val base = i * cols
        append("(\$${base + 1}, \$${base + 2}, \$${base + 3})")
    }
}
val params = rows.flatMap { listOf(it.name, it.age, it.active) }
conn.query(sql, *params.toTypedArray())
```

</details>

For very large batches (1000+ rows), split into blocks to stay within Postgres's parameter
limit of 65535:

```java
int maxRowsPerBlock = 65535 / cols;
for (int offset = 0; offset < rows.size(); offset += maxRowsPerBlock) {
    var block = rows.subList(offset, Math.min(offset + maxRowsPerBlock, rows.size()));
    // ... build and execute multi-row INSERT for this block
}
```

> **Tip**: Use `executeMany()` when you need per-row results (e.g. with `RETURNING`) or
> per-row error handling. Use multi-row `VALUES` when you want raw insert speed and only
> need the total rows-affected count.

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
- Only for `bpdbi-kotlin`: `kotlin-stdlib` (for `kotlin.time` and `kotlin.uuid`) and
  `kotlinx-serialization`

## Limitations

- **Arrays**: Only one-dimensional Postgres arrays are supported. Multi-dimensional arrays
  (e.g., `int[][]`) are not decoded and will return an empty list. This is a deliberate
  trade-off — multi-dimensional arrays are rarely used in practice, and supporting them adds
  significant complexity to the binary array codec.
- **Exception hierarchy**: `DbException` is the base for all database errors.
  `PoolException` (with subtypes `PoolExhaustedException` and `PoolTimeoutException`)
  is a separate hierarchy for pool-specific errors. When using pooled connections,
  catch both if you want to handle all bpdbi-related errors.

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

* Double-check we have all kotlin's time types implemented
* Look into in-list expansion (it is almost never a good idea, as it breaks preparing: right?)
* Document all extension points

## License

Apache 2.0.

Massive respects to [Vert.x](https://github.com/eclipse-vertx/vertx-sql-client),
[pgjdbc](https://github.com/pgjdbc/pgjdbc), and [Jdbi](https://jdbi.org).
This project derives from their work.
