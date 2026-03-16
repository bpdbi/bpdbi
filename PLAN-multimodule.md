# Plan: Multi-Module Restructure (vertx-sql-client style)

## Goal

Restructure djb from a single flat module into a multi-module Gradle project that cleanly separates the database-agnostic SQL client API from the PostgreSQL-specific implementation. A future MySQL/SQLite/etc. driver should be addable as a new module without touching the core.

## Current State

Everything lives in one module under `io.djb`:

```
io.djb.Connection           — PG connection + pipelining + auth + query (all in one class)
io.djb.Row                  — row with text-format getters
io.djb.RowSet               — result container
io.djb.ColumnDescriptor     — column metadata
io.djb.PgException          — PG error
io.djb.impl.buffer.PgBuffer — byte buffer (PG-specific name, but generic logic)
io.djb.impl.codec.*         — PG wire protocol encoder/decoder/messages
io.djb.impl.auth.*          — MD5 auth (PG-specific)
```

The problem: `Connection` is a god class that mixes generic SQL client concerns (pipeline state, query/enqueue/flush API, RowSet building) with PG-specific concerns (wire protocol, SCRAM auth, startup handshake).

## Target Structure

Three Gradle modules, mirroring vertx-sql-client:

```
djb/
├── build.gradle.kts              (root, declares subprojects)
├── settings.gradle.kts           (includes submodules)
├── djb-core/               (database-agnostic API + base impl)
│   ├── build.gradle.kts
│   └── src/main/java/io/djb/
│       ├── Connection.java            — interface
│       ├── Row.java                   — interface or class (kept as-is)
│       ├── RowSet.java                — kept as-is
│       ├── ColumnDescriptor.java      — kept as-is
│       ├── DbException.java           — generic (renamed from PgException)
│       ├── ConnectionConfig.java      — base config (host, port, db, user, pass, properties)
│       ├── Driver.java                — SPI interface for driver discovery
│       ├── spi/
│       │   └── ConnectionFactory.java — creates connections for a given driver
│       └── impl/
│           ├── BaseConnection.java    — shared pipeline logic (enqueue/flush/query)
│           └── ByteBuffer.java        — renamed from PgBuffer (nothing PG-specific about it)
│
├── djb-pg-client/                (PostgreSQL implementation)
│   ├── build.gradle.kts          (depends on djb-core)
│   └── src/main/java/io/djb/pg/
│       ├── PgConnection.java         — PG-specific connection (extends BaseConnection)
│       ├── PgConnectOptions.java      — PG-specific config (ssl mode, etc.)
│       ├── PgException.java           — PG-specific error with extra fields
│       ├── PgDriver.java             — implements Driver SPI
│       ├── data/                      — PG-specific types (Point, Interval, Inet, etc.)
│       └── impl/
│           ├── PgConnectionFactory.java  — creates PgConnection (socket, startup, auth)
│           ├── codec/
│           │   ├── PgEncoder.java
│           │   ├── PgDecoder.java
│           │   ├── PgProtocolConstants.java
│           │   └── BackendMessage.java
│           └── auth/
│               ├── MD5Authentication.java
│               └── ScramAuthentication.java
│
└── vertx-sql-client/             (reference code, unchanged)
```

## What Changes

### Step 1 — Convert to multi-module Gradle project

**Root `build.gradle.kts`:**
- Define shared config (Java 21 toolchain, repositories)
- No source code at root level

**Root `settings.gradle.kts`:**
```kotlin
include("djb-core", "djb-pg-client")
```

**`djb-core/build.gradle.kts`:**
- No database-specific dependencies
- Only JUnit 5 for tests

**`djb-pg-client/build.gradle.kts`:**
- `implementation(project(":djb-core"))`
- `implementation("com.ongres.scram:scram-client:3.1")`
- `testImplementation("org.testcontainers:postgresql:1.20.4")`

### Step 2 — Extract the database-agnostic API into `djb-core`

**Move and rename:**

| Current | New location | Changes |
|---------|-------------|---------|
| `io.djb.Row` | `io.djb.Row` (djb-core) | No change |
| `io.djb.RowSet` | `io.djb.RowSet` (djb-core) | No change |
| `io.djb.ColumnDescriptor` | `io.djb.ColumnDescriptor` (djb-core) | No change |
| `io.djb.PgException` | `io.djb.DbException` (djb-core) | Rename, keep generic fields (message, sqlState, severity) |
| `io.djb.impl.buffer.PgBuffer` | `io.djb.impl.ByteBuffer` (djb-core) | Rename (nothing PG-specific) |

**New interfaces/classes in `djb-core`:**

`Connection` becomes an **interface** defining the public API:

```java
public interface Connection extends AutoCloseable {
    RowSet query(String sql);
    RowSet query(String sql, Object... params);
    int enqueue(String sql);
    int enqueue(String sql, Object... params);
    List<RowSet> flush();
    Map<String, String> parameters();
    void close();
}
```

`BaseConnection` is an **abstract class** that implements the pipeline logic (enqueue/flush/query), delegating the actual encoding/sending/receiving to abstract methods:

```java
public abstract class BaseConnection implements Connection {
    private final List<PendingStatement> pending = new ArrayList<>();

    // Pipeline logic (shared across all drivers)
    public int enqueue(String sql) { ... }
    public int enqueue(String sql, Object... params) { ... }
    public List<RowSet> flush() { ... }
    public RowSet query(String sql) { ... }
    public RowSet query(String sql, Object... params) { ... }

    // Abstract — each driver implements its own protocol
    protected abstract void encodeSimpleQuery(String sql);
    protected abstract void encodeExtendedQuery(String sql, String[] params);
    protected abstract void flushToNetwork();
    protected abstract RowSet readSimpleQueryResponse();
    protected abstract RowSet readExtendedQueryResponse();
    protected abstract void sendTerminate();
}
```

**Driver SPI** for programmatic or ServiceLoader-based discovery:

```java
public interface Driver {
    String name();  // "postgresql", "mysql", etc.
    Connection connect(ConnectionConfig config);
}
```

```java
public record ConnectionConfig(
    String host, int port, String database,
    String username, String password,
    Map<String, String> properties
) {}
```

### Step 3 — Move PG-specific code into `djb-pg-client`

**`PgConnection`** extends `BaseConnection`:
- Implements the abstract encode/read/flush methods using `PgEncoder` and `PgDecoder`
- Owns the `Socket`, streams, encoder, decoder
- Handles PG-specific startup (auth, parameter status, backend key data)

**`PgDriver`** implements `Driver`:
- `name()` → `"postgresql"`
- `connect(config)` → creates Socket, wraps in PgConnection, performs startup

**Everything else** moves with minimal changes:
- `PgEncoder`, `PgDecoder`, `BackendMessage`, `PgProtocolConstants` → `io.djb.pg.impl.codec`
- `MD5Authentication`, `ScramAuthentication` → `io.djb.pg.impl.auth`
- `PgException` stays PG-specific (has extra fields like schema, table, constraint that other DBs may not have) — but extends `DbException`

### Step 4 — Move tests

**`djb-core` tests:**
- `ByteBufferTest` (renamed from `PgBufferTest`)
- Any future shared test base classes

**`djb-pg-client` tests:**
- `PgEncoderTest`, `PgDecoderTest`, `MD5AuthenticationTest` — unit tests
- `ConnectionTest` → `PgConnectionTest` — integration tests (Testcontainers)

### Step 5 — ServiceLoader registration

`djb-pg-client/src/main/resources/META-INF/services/io.djb.Driver`:
```
io.djb.pg.PgDriver
```

This enables:
```java
// Explicit
Connection conn = new PgDriver().connect(config);

// Or via ServiceLoader (future convenience)
Connection conn = Driver.connect(config); // finds PgDriver on classpath
```

## What Stays The Same

- The pipelining API design (enqueue/flush/query)
- The wire protocol implementation (PgEncoder, PgDecoder, BackendMessage)
- Row, RowSet, ColumnDescriptor APIs
- All 96 tests (just moved to the right module)
- Blocking I/O model
- ByteBuffer (née PgBuffer) implementation

## What A Future MySQL Driver Would Look Like

```
djb-mysql-client/
├── build.gradle.kts          (depends on djb-core)
└── src/main/java/io/djb/mysql/
    ├── MysqlConnection.java       — extends BaseConnection
    ├── MysqlDriver.java           — implements Driver
    └── impl/
        ├── codec/
        │   ├── MysqlEncoder.java
        │   ├── MysqlDecoder.java
        │   └── MysqlProtocolConstants.java
        └── auth/
            └── NativeAuthentication.java
```

It would only need to implement the 6 abstract methods from `BaseConnection` + its own codec. The pipeline logic, RowSet building, and public API come for free.

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| `Connection` as interface | Yes | Allows PG/MySQL/etc. to add DB-specific methods |
| Pipeline logic in `BaseConnection` | Yes | Prevents each driver from reimplementing enqueue/flush |
| `DbException` base class | Yes | Each DB extends with its own fields (PG has schema/table/constraint, MySQL has errno) |
| ServiceLoader SPI | Yes but optional | Nice for framework integration, but direct instantiation works too |
| ByteBuffer generic name | Yes | The buffer has nothing PG-specific; MySQL encoder would use the same class |
| Separate `PgException extends DbException` | Yes | Preserves the PG-specific error fields without polluting the generic API |
