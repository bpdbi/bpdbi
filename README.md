# Djb — Pipelined blocking Postgres and MySQL on the JVM

A blocking SQL client for the JVM that treats **pipelining** as a first-class concept.
Ported from the battle-tested [Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client)
(the foundation of Quarkus), but stripped of all async/reactive machinery.
Plain `java.net.Socket` I/O. No Netty dependencies. No event loop. No `Uni<T>`.

## Why?

JDBC is showing its age and (hence) does not allow [pipelining](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html)
which is available in Postgres 13+ and MySQL 8.0.3+.

Vert.x (`vertx-sql-client`) does support pipelines (for these databases), but forces reactive/async programming.

Djb gives you pipelining with straightforward blocking code — ideal for
**Java 21+ virtual threads**, where blocking is inexpensive and clarity matters.

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

### Pipelining

Queue statements with `enqueue()`, send them all in one TCP write with `flush()`:

```java
// 4 statements, 1 network roundtrip
conn.enqueue("SET search_path TO myschema");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)");
RowSet result = conn.query("SELECT * FROM my_table WHERE id = $1", 42);
```

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

### Prepared Statements

Parse once, execute many times:

```java
try (var stmt = conn.prepare("SELECT * FROM users WHERE id = $1")) {
    RowSet alice = stmt.query(1);
    RowSet bob   = stmt.query(2);
    RowSet carol = stmt.query(3);
}  // closes the prepared statement on the server
```

### Transactions

Use `begin()` for a transaction with automatic rollback on failure:

```java
try (var tx = conn.begin()) {
    tx.query("INSERT INTO orders VALUES ($1, $2)", 1, "widget");
    tx.query("INSERT INTO audit_log VALUES ($1)", "created order");
    tx.commit();
}  // auto-rollback if commit() not called
```

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

JSON/JSONB columns are returned as strings — parse with your preferred JSON library:

```java
String json = row.getString("data");  // {"key": "value"}
```

### Cancel Request (Postgres)

Cancel a long-running query from another thread:

```java
PgConnection conn = PgConnection.connect(...);
// In another thread:
conn.cancelRequest();
```

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

This works with any driver — just swap `PgConnection.connect(...)` for
`MysqlConnection.connect(...)`.

## Modules

```
djb-core          Database-agnostic API (Connection, Row, RowSet, pipelining logic)
djb-pg-client     Postgres driver (wire protocol, auth, PG types)
djb-mysql-client  MySQL driver (wire protocol, auth)
```

Each driver implements its own wire protocol by extending `BaseConnection`.
The pipelining machinery, result types, and public API are shared.

See [`examples/`](examples/) for runnable examples.

## Requirements

- JDK 21+
- Only for `djb-pg-client`: `scram-client` (a tiny encryption library for SCRAM authentication)

## Status

Early development.
Not yet published to Maven Central.

## Todo

* Kotlin support
* Pluggable Json handling
* Value-binding like in Jdbi (does this need to be pluggable?)
* Result-mapping like in Jdbi (but fully pluggable: kotlin implements with kotlinx.serialization)

## License

Apache 2.0, same as **Vert.x**.
