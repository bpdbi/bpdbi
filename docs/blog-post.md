# Bpdbi: What If Your Database Client Just Sent Everything at Once?

Every time your Java or Kotlin app talks to Postgres, something wasteful happens.
Your code sends a query, waits for the response, sends the next query, waits again.
Each wait is a full network round-trip with a typical latency of 0.5-2ms in a cloud environment.
For a simple transaction with a few queries, that's 8-10ms of just... waiting.

This problem is exacerbated by the additional queries that are commonly added when
using Supabase's Row-Level Security (RLS).
Roles and JWT claims need to be set before querying.
This usually needs to happen more than once for each HTTP request,
as some db queries need to be executed with different roles than others.

Bpdbi is a new JVM database library for the Postgres database.
It exposes **pipelining**, a feature that's been part of the Postgres wire protocol since version 14.

JDBC can't do it. Bpdbi can.

## Where this came from

Bpdbi started as a port of the [Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client)
— the database layer behind Quarkus and one of the fastest JVM database drivers.
Vert.x already speaks the Postgres wire protocol directly (no JDBC), and exposes the pipelines feature.

The problem is that Vert.x is fully reactive (async/non-blocking).
And reactive code is not for everyone: it adds
[significant](https://rajendravardi.medium.com/the-hidden-dangers-of-reactive-programming-what-every-developer-must-know-9ce467552fd3)
[complexity](https://www.techyourchance.com/reactive-programming-considered-harmful).

Reactive code looks like this:

```java
// Reactive — same logic, now good luck
public Uni<Invoice> generateInvoice(long orderId) {
  return orderRepo.findById(orderId)
      .flatMap(order ->
          Uni.combine().all()
              .unis(
                  customerRepo.findById(order.getCustomerId()),
                  itemRepo.findByOrderId(orderId))
              .asTuple()
              .map(tuple -> invoiceService.build(
                  order, tuple.getItem1(), tuple.getItem2())));
}
```

The blocking equivalent look like this:

```java
public Invoice generateInvoice(long orderId) {
  Order order = orderRepo.findById(orderId);
  Customer customer = customerRepo.findById(order.getCustomerId());
  List<Item> items = itemRepo.findByOrderId(orderId);
  return invoiceService.build(order, customer, items);
}
```

Reactive allows for higher throughput in high-traffic scenarios, but comes at a cost:
code is less readable, useless stack traces, and a paradigm that infects your entire codebase.
With Java 21's virtual threads, blocking I/O became much cheaper —
you get thousands of concurrent connections without platform threads.

Bpdbi started as a port of *Vert.x SQL Client*, stripped of all its async/reactive machinery.
Where Vert.x SQL uses [Netty](https://netty.io) to connect with the database,
Bpdbi uses a good old `java.net.Socket`.

Bpdbi employs Postgres' binary protocol and pipelines for all db queries,
even single queries without parameters.
This results in a library with a much smaller footprint,
while being very performant (as shown in the benchmarks).

**TL;DR**:
Bpdbi provides blocking, pipelined, small-footprint and performant Postgres access for the JVM.

## Pipelining in practice

Here's the core idea. Say you need to start a transaction, set some config, and run a query.
With JDBC, that's four separate round-trips:

```java
// JDBC: 4 round-trips (8ms+ at 1ms latency)
conn.createStatement().execute("BEGIN");
conn.createStatement().execute("SET statement_timeout TO '5s'");
conn.createStatement().execute("SET LOCAL role TO 'authenticated'");
PreparedStatement ps = conn.prepareStatement("SELECT * FROM orders WHERE id = ?");
ps.setInt(1, 42);
ResultSet rs = ps.executeQuery();
```

With Bpdbi, it's one:

```java
// Bpdbi: 1 round-trip (2ms at 1ms latency)
conn.enqueue("BEGIN");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("SET LOCAL role TO 'authenticated'");
RowSet result = conn.query("SELECT * FROM orders WHERE id = $1", 42);
```

`enqueue()` buffers statements locally. `query()` flushes everything
— all the enqueued statements plus itself — in a single TCP write.
The Postgres instance processes them all and sends all responses back at once.

### When you need all the results

Sometimes you want results from multiple pipelined queries. `flush()` returns them all:

```java
conn.enqueue("BEGIN");
int aliceId = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Alice");
int bobId   = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Bob");
conn.enqueue("COMMIT");
List<RowSet> results = conn.flush();

long aliceId = results.get(aliceId).first().getLong("id");
long bobId   = results.get(bobId).first().getLong("id");
```

Four statements, one round-trip.
Each `enqueue()` returns an index so you know which result is which.

### The benchmark numbers

We ran JMH benchmarks using Toxiproxy to simulate 1ms network latency per direction (2ms round-trip).
This simulates what you'd see talking to a database in the same cloud region.

| Scenario | Bpdbi | JDBC (`pgjdbc`) | Speedup  |
|---|--:|--:|----------|
| 10 SELECTs (pipelined vs sequential) | 310 ops/s | 18 ops/s | **17x**  |
| Transaction (BEGIN+SELECT+COMMIT) | 360 ops/s | 185 ops/s | **~2x**  |
| 10 INSERTs in a transaction | 116 ops/s | 18 ops/s | **6.5x** |
| Cursor fetch (1000 rows) | 281 ops/s | 30 ops/s | **9.3x** |
| Bulk insert (100 rows) | 313 ops/s | 171 ops/s | **1.8x** |
| Single row lookup | 370 ops/s | 370 ops/s | op par   |
| Multi-row fetch (10 rows) | 358 ops/s | 358 ops/s | on par   |

The pattern is clear: anything that touches the network more than once gets a massive speedup.
While single-query performance is on par with JDBC + `pgjdbc`.

## Postgres-only, and that's the point

Bpdbi only supports Postgres, the only open source database that truly supports pipelining.
This is intentional, and it buys us a lot.

### Binary protocol everywhere
The Postgres extended query protocol lets you request results in binary format.
An integer comes back as four raw bytes instead of the text string `"12345"`.
A UUID is 16 bytes instead of 36 characters. No string allocation and string parsing.

Most JDBC drivers use the text format for simple queries (the "simple query protocol")
and only switch to binary for prepared statements.
Bpdbi uses the extended query protocol with binary format for _everything_ —
even `BEGIN`, `COMMIT`, and `SET`.
This is what makes uniform pipelining possible:
because every statement uses the same wire protocol, they can all be batched in a pipeline.

### Small footprint
Bpdbi's Postgres driver is about 1,400 lines of Java.
The whole library is under 200KB and that includes a connection pool.

Compare that to a typical Postgres/Jdbi/HikariCP stack:

| Component | Size |
|---|---|
| `pgjdbc` (JDBC driver) | ~1.1 MB |
| Jdbi (developer experience) | ~1 MB |
| HikariCP (connection pool) | ~160 KB |
| **Total** | **~2.3 MB** |
| **Bpdbi (everything)** | **< 200 KB** |

And that's the modest stack.
Hibernate is ~15MB, jOOQ is ~15MB.
*Vert.x SQL Client* with Netty is 5MB+.
Bpdbi has no transitive dependencies beyond SCRAM-client (a small crypt lib for connection auth).

### No Netty, no event loops
Plain `java.net.Socket` with unsynchronized buffered I/O streams.
No channel pipelines, no allocator frameworks, no thread pools.
This works because Bpdbi connections are single-threaded by design — just like JDBC connections.
With virtual threads, blocking on a socket is cheap.
The simplicity pays off in readability, debugging, and startup time.

### GraalVM native-image ready
The core library uses zero reflection.
`native-image` just works, no configuration needed.

## Developer experience: Jdbi-level, not JDBC-level

Directly using the JDBC API in your application code is low-level and verbose.
That's why libraries like [Jdbi](https://jdbi.org) exist. Jdbi provides:
named parameters, row mapping, pluggable data type binders/ row mappers/ JSON mappers.
It also makes use of reflection for its built-in JavaBean row mapper.

Bpdbi has all these features built-in: you don't need an additional library.
It comes with add-on modules: `bpdbi-record-mapper` for mapping rows to Java records,
and `bpdbi-bean-mapper` for mapping rows to JavaBeans.
The `bpdbi-kotlin` add-on contains a mapper for mapping rows to Kotlin data classes
using `kotlinx.serialization` which, unlike the other row mappers, does **not** use reflection.

## Under the hood

The performance doesn't just come from pipelining.
Bpdbi borrows optimization ideas from `pgjdbc`, Vert.x, and Jdbi, and adds some of its own:

- **Column-oriented storage.** A 100K-row, 10-column result creates 10 byte arrays instead of 1,000,000. Each `Row` is a lightweight view (buffer reference + row index) with no per-row allocation.
- **Lazy decoding.** Rows store raw wire bytes. Columns you never read are never decoded. Your `SELECT *` that only reads 3 columns out of 20? Only those 3 get decoded.
- **Binary parameter encoding.** Sending an `int4` as 4 raw bytes instead of the ASCII string `"12345"` saves wire bandwidth and server CPU.
- **Unsynchronized I/O.** Java's `BufferedOutputStream` acquires a lock on every write. Since Bpdbi connections are single-threaded, the buffered streams skip all synchronization.
- **Prepared statement cache.** An LRU cache avoids re-parsing the same SQL. Oversized queries that would flush the cache are rejected outright.
- **Deadlock prevention.** Large pipelined batches can deadlock if both TCP buffers fill up. Bpdbi estimates response sizes and inserts mid-pipeline syncs to prevent this — transparently.

## Who is Bpdbi for?

Requirements for using Bpdbi:

- Use **Postgres** (it's the only supported database)
- Use **Java 21+** (virtual threads make blocking I/O practical at scale)

Bpdbi makes a lot of sense if you:

- Want to use Postgres' **pipelining**
- Use RLS (most Supabase users) or otherwise make a lot of "prefix" queries
- Prefer **simple, blocking code** over reactive/async/non-blocking code
- Care about **small dependencies** and fast startup (GraalVM, serverless, CLI tools)
- Want to **write SQL by hand** (not ORM)

If you're already happy with Hibernate or jOOQ and their compile-time SQL validation,
Bpdbi is probably not what you need.

### Web application libraries/frameworks that work well with Bpdbi

Bpdbi uses blocking I/O and is designed for virtual threads.
It pairs well with HTTP frameworks that are not mandatorily reactive/async and do not dictate JDBC:

- **[http4k](https://www.http4k.org/)** — Functional, zero-reflection, tiny. The philosophical twin
  of Bpdbi on the HTTP side.
- **[Javalin](https://javalin.io/)** — Minimal Jetty wrapper with built-in virtual thread support.
  Very popular in both Java and Kotlin.
- **[Helidon SE](https://helidon.io/) 4+** — Oracle's lightweight framework. Versions 1–3 were
  reactive (Reactive Streams); 4.x was rewritten around virtual threads and blocking I/O.
- **[Undertow](https://undertow.io/)** — Embedded, low-level. Blocking handlers run on a worker
  thread pool (or virtual threads).
- **[Micronaut](https://micronaut.io/)** — Compile-time DI, GraalVM-first. Supports both reactive
  and imperative, controller methods can simply return values.
- **[Spark](https://sparkjava.com/)** — Dead-simple Java micro-framework with the same "just enough" philosophy.
- **[Jooby](https://jooby.io/)** — Modular micro-framework, explicit about dependencies, virtual
  thread support.
- **`com.sun.net.httpserver`** — The JDK's built-in HTTP server. Zero dependencies, pairs naturally
  with Bpdbi's minimalism.

Frameworks like Spring Boot are opinionated about their own data stacks (Spring Data, Hibernate) and
assume a JDBC `DataSource` integration for transactions, health checks, and connection management.







------------------


### Named parameters

Similar to Jdbi, you can use named parameters with Bpdbi:

```java
RowSet rs = conn.query(
    "SELECT * FROM users WHERE name = :name AND age > :age",
    Map.of("name", "Alice", "age", 21));
```

### Row mapping with Java records

```java
record User(int id, String name, int age) {}

List<User> users = conn.query("SELECT id, name, age FROM users")
    .mapTo(User.class);
```

The record mapper uses reflection, but it's in an optional module (`bpdbi-record-mapper`).
The core library stays reflection-free.

Bpdbi also has an optional module for a JavaBean mapper (`bpdbi-bean-mapper`).

### Kotlin: compile-time mapping with `kotlinx.serialization`

If you're using Kotlin, the `bpdbi-kotlin` module maps rows using `kotlinx.serialization`
— it works at compile time, no reflection at all:

```kotlin
@Serializable
data class User(val id: Int, val name: String, val age: Int)

val users: List<User> = conn.queryAs("SELECT id, name, age FROM users")

val alice: User? = conn.queryOneAs("SELECT * FROM users WHERE id = $1", 42)
```

### Transactions

```java
try (var tx = conn.begin()) {
    tx.query("INSERT INTO orders VALUES ($1, $2)", 1, "widget");
    tx.query("INSERT INTO audit_log VALUES ($1)", "created order");
    tx.commit();
}  // auto-rollback if commit() not called
```

`Transaction` implements `Connection`, so you can pass it to any code that expects a connection.
This makes rollback-only test connections trivial:

```java
try (var tx = conn.begin()) {
    var repo = new UserRepository(tx);  // tx is a Connection
    repo.createUser("Alice", 30);
    assertEquals(1, tx.query("SELECT count(*) FROM users").first().getInteger(0));
    // no commit — auto-rollback, database unchanged
}
```

### Streaming

Large results don't need to fit in memory:

```java
conn.queryStream("SELECT * FROM big_table", row -> {
    process(row.getString("name"), row.getInteger("id"));
});
```

Internally, Bpdbi recycles a single `Row` object across all rows
(inspired by Vert.x's `RowBase.tryRecycle()` pattern).
No per-row allocation.

### Custom types

Register converters for your domain types:

```java
// Writing: Java → SQL
conn.binderRegistry().register(Money.class, m -> m.amount().toPlainString());

// Reading: SQL → Java
conn.mapperRegistry().register(Money.class,
    (value, column) -> new Money(new BigDecimal(value)));

// Now just use them
conn.query("INSERT INTO prices VALUES ($1)", new Money("9.99"));
Money price = conn.query("SELECT price FROM products WHERE id = $1", 1)
    .first().get("price", Money.class);
```

### JSON columns

Plug in your JSON library (Jackson, Gson, whatever):

```java
conn.setJsonMapper(new JsonMapper() {
    public <T> T fromJson(String json, Class<T> type) {
        return objectMapper.readValue(json, type);
    }
    public String toJson(Object value) {
        return objectMapper.writeValueAsString(value);
    }
});

// jsonb columns are auto-detected
OrderMeta meta = conn.query("SELECT metadata FROM orders WHERE id = $1", 1)
    .first()
    .get("metadata", OrderMeta.class);
```