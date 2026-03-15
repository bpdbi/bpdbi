# djb — Blocking PostgreSQL Client with First-Class Pipelining

## Context

The JVM ecosystem lacks a blocking PostgreSQL client that exposes protocol-level pipelining. pgjdbc only pipelines `executeBatch()` (DML, no SELECTs). Vert.x's pg-client has excellent pipelining but requires the Vert.x runtime and async programming model. djb fills this gap: a zero-dependency, blocking-IO PostgreSQL client where pipelining is a first-class API concept — you can queue fire-and-forget statements (SET, CREATE TEMP TABLE, etc.) and have them sent in a single TCP write alongside the next real query, eliminating extra roundtrips.

The implementation is informed by the vertx-sql-client source code (downloaded at `vertx-sql-client/`), reusing its protocol knowledge (wire format, type mappings, auth flows) but replacing all Netty/Vert.x infrastructure with plain `java.net.Socket` I/O.

## Decisions

- **Build:** Gradle Kotlin DSL, single module
- **Package:** `io.djb`
- **Java:** 21+ (records, sealed types, pattern matching switch)
- **JSON:** Pluggable `JsonCodec` interface, defaults to raw `String`. Designed for easy kotlinx.serialization integration.
- **Only runtime dependency:** `com.ongres.scram:scram-client` (for SCRAM-SHA-256 auth)
- **Not thread-safe per connection** — one Connection = one TCP socket, designed for virtual-thread-per-connection usage

---

## Implementation Steps

### Step 1 — Project scaffold

Create `build.gradle.kts` with Java 21 toolchain, `scram-client` dependency, JUnit 5 + Testcontainers for tests. Set up `module-info.java` exporting `io.djb` and `io.djb.data`, keeping `io.djb.impl.*` internal.

Package structure:
```
io.djb                      — public API (Connection, Row, RowSet, Transaction, etc.)
io.djb.data                 — PG-specific value types (Point, Interval, Inet, etc.)
io.djb.impl                 — internal
io.djb.impl.codec           — wire protocol encoder/decoder
io.djb.impl.codec.types     — DataType enum + DataTypeCodec
io.djb.impl.auth            — SCRAM, MD5
io.djb.impl.buffer          — PgBuffer
```

### Step 2 — PgBuffer (replaces Netty ByteBuf)

A growable `byte[]` with separate read/write cursors. Supports:
- Write: `writeByte`, `writeInt`, `writeCString`, `writeBytes`, `setInt` (backpatch length fields)
- Read: `readByte`, `readInt`, `readLong`, `readFloat`, `readDouble`, `readBytes`, `skipBytes`
- Lifecycle: `toByteArray()` for flushing to OutputStream, `wrap(byte[])` for reading from InputStream

~200 lines. Unit-testable immediately.

**Ref:** The vertx encoder/decoder use ByteBuf's dual-index API extensively; PgBuffer mirrors this to make porting mechanical.

### Step 3 — Protocol constants + message encoding

Port `PgProtocolConstants.java` (message type IDs, auth types — pure constants, no deps).

Build `PgEncoder` — a plain class (not a Netty handler) that accumulates protocol messages into a PgBuffer:
- `encodeStartupMessage(user, database, properties)`
- `encodePasswordMessage(password)`
- `encodeQuery(sql)` — simple query protocol "Q" message
- `encodeParse(sql, paramTypes)` + `encodeBind(params)` + `encodeExecute()` + `encodeSync()` — extended query protocol
- `flush(OutputStream)` — renders all pending messages, writes as single `out.write()`, clears buffer

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/codec/PgEncoder.java` — mechanical port replacing `ByteBuf` → `PgBuffer` and `ctx.writeAndFlush()` → `flush(OutputStream)`.

### Step 4 — Message decoding + framing

Build `PgDecoder` — reads from `InputStream`, returns typed backend messages:
- Framing loop: read 1-byte type + 4-byte length + payload
- Dispatch to decode methods per message type (30+ types)
- Returns sealed interface `BackendMessage` with record variants:
  ```java
  sealed interface BackendMessage {
      record AuthenticationOk() implements BackendMessage {}
      record AuthenticationMD5Password(byte[] salt) implements BackendMessage {}
      record ParameterStatus(String name, String value) implements BackendMessage {}
      record ReadyForQuery(char txStatus) implements BackendMessage {}
      record RowDescription(ColumnDescriptor[] columns) implements BackendMessage {}
      record DataRow(byte[][] values) implements BackendMessage {}
      record CommandComplete(String tag) implements BackendMessage {}
      record ErrorResponse(Map<Character,String> fields) implements BackendMessage {}
      // ... etc
  }
  ```

**Key simplification over vertx:** Blocking reads mean no partial-message buffering. Each `readMessage()` call blocks until one complete message is available.

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/codec/PgDecoder.java`

### Step 5 — Authentication (MD5 first, then SCRAM)

- **MD5:** Direct port of `MD5Authentication.java` — pure Java, no changes needed beyond package rename.
- **SCRAM-SHA-256:** Port `ScramAuthentication`/`ScramSession` using the `scram-client` library, removing Netty references.
- **Cleartext:** Trivial — send password as PasswordMessage.

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/auth/`

### Step 6 — Connection lifecycle

Build `Connection` class with `connect()` that:
1. Opens `Socket`, wraps in BufferedOutputStream/BufferedInputStream
2. Optionally negotiates SSL (SSLRequest → upgrade to SSLSocket)
3. Sends StartupMessage
4. Runs auth handshake loop (blocking sequential reads)
5. Collects ParameterStatus, BackendKeyData
6. Waits for ReadyForQuery

```java
public final class Connection implements AutoCloseable {
    public static Connection connect(String host, int port, String db, String user, String pass);
    public static Connection connect(ConnectionConfig config);
    public void close(); // sends Terminate message
}
```

**First integration test milestone:** connect to Testcontainers PostgreSQL, authenticate, disconnect.

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/codec/InitPgCommandMessage.java`

### Step 7 — Simple query protocol

Add `Connection.query(String sql)` returning `RowSet`:
1. Encode Query message
2. Flush to socket
3. Read responses: RowDescription → DataRow* → CommandComplete → ReadyForQuery
4. Decode rows, build RowSet

This is the "Q" message — supports multi-statement SQL, no parameters.

### Step 8 — DataType enum + DataTypeCodec (scalar types)

Port the type system in two phases:

**Phase A (MVP):** bool, int2/4/8, float4/8, numeric, text/varchar/bpchar, bytea, uuid, date, time, timetz, timestamp, timestamptz, json/jsonb (as String via JsonCodec).

**Phase B (later):** geometric types (Point, Line, Box, etc.), network types (Inet, Cidr), Interval, Money, array types.

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/codec/DataType.java` and `DataTypeCodec.java` (1827 lines — largest porting effort, but mechanical ByteBuf→PgBuffer substitution).

### Step 9 — Result types

```java
public record ColumnDescriptor(String name, int tableOID, DataType dataType, ...) {}

public final class Row {
    // Typed getters by index and by column name
    String getString(int i);  Integer getInteger(int i);  // etc.
    <T> T get(int i, Class<T> type);  // generic
}

public final class RowSet implements Iterable<Row> {
    int size();
    int rowsAffected();
    List<ColumnDescriptor> columnDescriptors();
    Row first();
    Stream<Row> stream();
}
```

No Tuple type in the public API — parameters are `Object... params`.

### Step 10 — Extended query protocol (parameterized queries)

Add `Connection.query(String sql, Object... params)`:
1. Encode: Parse (unnamed stmt) + Bind (params) + Describe (portal) + Execute + Sync — all into one buffer
2. Single flush
3. Read: ParseComplete, BindComplete, RowDescription/NoData, DataRow*, CommandComplete, ReadyForQuery

Type inference: `DataType.lookup(param.getClass())` determines the OID and binary encoding for each parameter. NULL → OID 0 (UNKNOWN).

### Step 11 — Pipelining API (the key feature)

`enqueue()` adds a statement to the pending pipeline and returns its `int` index. `flush()` sends all pending statements in a single TCP write, reads all responses, and returns `List<RowSet>`. `query()` is sugar for enqueue + flush + return last result.

**Core API:**

```java
// enqueue() returns the index into the results list
public int enqueue(String sql) { ... }
public int enqueue(String sql, Object... params) { ... }

// flush() sends all pending statements in one TCP write, returns all results
public List<RowSet> flush() { ... }
```

**query() is sugar:**

```java
public RowSet query(String sql) {
    enqueue(sql);
    return flush().getLast();
}

public RowSet query(String sql, Object... params) {
    enqueue(sql, params);
    return flush().getLast();
}
```

This means `query()` implicitly flushes any previously enqueued statements too — so the simple one-off usage still works without thinking about pipelining.

**Example — fire-and-forget setup + query (4 statements, 1 roundtrip):**

```java
conn.enqueue("SET search_path TO myschema");
conn.enqueue("SET statement_timeout TO '5s'");
conn.enqueue("CREATE TEMP TABLE IF NOT EXISTS _cache (id int, data text)");
RowSet result = conn.query("SELECT * FROM my_table WHERE id = $1", 42);
```

**Example — pipeline with results (5 statements, 1 roundtrip):**

```java
conn.enqueue("BEGIN");
int alice = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Alice");
int bob   = conn.enqueue("INSERT INTO users (name) VALUES ($1) RETURNING id", "Bob");
conn.enqueue("INSERT INTO audit_log (msg) VALUES ($1)", "added 2 users");
conn.enqueue("COMMIT");
List<RowSet> results = conn.flush();

long aliceId = results.get(alice).first().getLong("id");
long bobId   = results.get(bob).first().getLong("id");
```

**Internal data structure:**

The pending list is `List<PendingStatement>`, where each entry holds the SQL, params, and protocol type (simple/extended):

```
pending = [
  0: PendingStatement("BEGIN",                    simple)
  1: PendingStatement("INSERT ... RETURNING id",  extended, params=["Alice"])
  2: PendingStatement("INSERT ... RETURNING id",  extended, params=["Bob"])
  3: PendingStatement("INSERT INTO audit_log..",   extended, params=["added 2 users"])
  4: PendingStatement("COMMIT",                   simple)
]
```

On `flush()`:
1. Encode all pending statements into one PgBuffer
2. Single `out.write()` + `out.flush()`
3. Read responses in order, building `List<RowSet>` (indices 0–4)
4. Clear the pending list, return the results list

Each enqueued simple query uses the "Q" protocol (gets its own ReadyForQuery). Each enqueued parameterized query uses Parse/Bind/Execute/Sync (gets its own ReadyForQuery via Sync). Pipeline errors don't poison subsequent statements — each is independent.

**Error handling:** If a pipelined statement fails, the corresponding `RowSet` in the results list captures the error. Calling `rows()` or `first()` on it throws `PgException`. Other statements in the pipeline still execute and get their results normally.

### Step 12 — Transaction support

Transactions are just syntactic sugar over BEGIN/COMMIT/ROLLBACK. They compose naturally with the pipelining API:

```java
// Ergonomic wrapper with auto-rollback:
try (var tx = conn.begin()) {
    tx.query("INSERT INTO t VALUES ($1)", 1);
    tx.query("INSERT INTO t VALUES ($1)", 2);
    tx.commit();
}  // auto-rollback if commit() not called
```

Since `begin()` / `commit()` / `rollback()` are just SQL statements, they can be pipelined too — there's nothing special about them:

```java
// Entire transaction in one roundtrip:
conn.enqueue("BEGIN");
int r1 = conn.enqueue("INSERT INTO t VALUES (1) RETURNING id");
int r2 = conn.enqueue("INSERT INTO t VALUES (2) RETURNING id");
conn.enqueue("COMMIT");
List<RowSet> results = conn.flush();
```

### Step 13 — SSL/TLS

1. Send SSLRequest (8 bytes)
2. Read single-byte response: `S` (upgrade) or `N` (no SSL)
3. If `S`, wrap Socket → SSLSocket via SSLSocketFactory, re-wrap streams
4. SslMode enum: `DISABLE`, `ALLOW`, `PREFER`, `REQUIRE`

**Ref:** `vertx-pg-client/src/main/java/io/vertx/pgclient/impl/codec/InitiateSslHandler.java`

### Step 14 — JsonCodec SPI

```java
public interface JsonCodec<T> {
    T decode(String json, Class<T> type);
    String encode(T value);
}
```

Default implementation returns raw String. Users register a kotlinx.serialization or Jackson implementation via `ConnectionConfig.jsonCodec(myCodec)`. The codec is consulted when reading json/jsonb columns via `Row.get(i, MyClass.class)`.

---

## Testing Strategy

**Unit tests** (no DB): PgBuffer ops, encoder output bytes, decoder parsing, DataTypeCodec roundtrips, MD5 hash, type inference.

**Integration tests** (Testcontainers PostgreSQL):
- Connect + auth (MD5, SCRAM, cleartext)
- Simple & parameterized queries for all supported types
- **Pipelining: enqueue N + query, verify single-roundtrip behavior**
- Transactions (commit, rollback, auto-rollback)
- SSL connections
- Error handling (bad SQL, pipeline errors, connection loss)
- NULL handling (parameters and results)

---

## Key Reference Files in vertx-sql-client/

| File | What to extract |
|------|----------------|
| `vertx-pg-client/.../codec/PgEncoder.java` | All message encoding logic |
| `vertx-pg-client/.../codec/PgDecoder.java` | Message framing + dispatch |
| `vertx-pg-client/.../codec/DataType.java` | OID→type mappings |
| `vertx-pg-client/.../codec/DataTypeCodec.java` | Binary/text type encoding (1827 lines) |
| `vertx-pg-client/.../codec/PgProtocolConstants.java` | Message type + auth type constants |
| `vertx-pg-client/.../codec/InitPgCommandMessage.java` | Auth handshake flow |
| `vertx-pg-client/.../auth/scram/` | SCRAM-SHA-256 implementation |
| `vertx-pg-client/.../util/MD5Authentication.java` | MD5 auth hash |
| `vertx-pg-client/.../data/*.java` | Geometric/network value types |
