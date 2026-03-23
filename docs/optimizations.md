# Performance optimizations

This document catalogs the performance optimizations in bpdbi, organized by
category. Some were inspired by studying the internals of
[pgjdbc](https://github.com/pgjdbc/pgjdbc),
[Vert.x SQL Client](https://github.com/eclipse-vertx/vertx-sql-client), and
[Jdbi](https://github.com/jdbi/jdbi).

---

## Memory & GC

### Column-oriented storage (`ColumnBuffer`)

Instead of allocating N rows x M columns individual `byte[]` arrays, all values
for a single column are packed into one contiguous `byte[]` with per-row
offset/length metadata. A 100K-row, 10-column result creates 10 buffers instead
of 1,000,000 `byte[]` allocations — dramatically reducing GC pressure and heap
fragmentation.

`bpdbi-core/…/impl/ColumnBuffer.java`

### Lazy decoding in `Row`

`Row` stores raw wire bytes and decodes only when a typed getter is called
(`getString`, `getInteger`, etc.). Columns you never read are never decoded.
This avoids allocating objects for unused columns, which is common in
`SELECT *` queries where application code reads only a few fields.

Supports two backing modes: per-row `byte[][]` (streaming) and column-buffer
views (buffered result sets). Both defer decoding identically.

`bpdbi-core/…/Row.java`

### Row object recycling for streaming

*Inspired by Vert.x SQL Client's `RowBase.tryRecycle()` pattern.*

The `queryStream()` callback path reuses a single `Row` object across all rows
instead of allocating a new one per `DataRow` message. The backing `byte[][]` is
swapped in-place via `Row.resetForStreaming()`. Since the consumer processes one
row at a time, the previous row's data is no longer needed. This eliminates
per-row object allocation on streaming paths entirely.

`bpdbi-core/…/Row.java` — `resetForStreaming()`
`bpdbi-pg-client/…/PgConnection.java` — `readExtendedQueryStreaming()`, `readSimpleQueryStreaming()`

### Buffer reuse in cursors

The `PgCursor` retains column buffers from the previous batch and uses
`ColumnBuffer.averageValueSize()` to size the next batch's buffers based on
observed data. This avoids oversizing (wasted memory) and undersizing (resize
copies) across successive `cursor.read()` calls.

`bpdbi-pg-client/…/PgConnection.java` — `PgCursor.createOrReuseBuffers()`

### `ByteBuffer` shrink threshold

The protocol encoder/decoder buffer (`ByteBuffer`) shrinks back to 1 KB when
`clear()` is called on a buffer larger than 64 KB. This prevents holding large
allocations indefinitely after processing an unusually large batch.

`bpdbi-core/…/impl/ByteBuffer.java` — `clear()`

---

## CPU

### Direct byte-to-number parsing

`Row` parses integers and longs directly from ASCII bytes without creating an
intermediate `String`. `parseIntFromBytes()` and `parseLongFromBytes()` scan the
byte buffer in-place. `parseBoolFromBytes()` checks a single byte for `'t'`/`'1'`.
This matters for text-format results where every row would otherwise allocate a
throwaway `String` per numeric column.

`bpdbi-core/…/Row.java` — `parseIntFromBytes()`, `parseLongFromBytes()`, `parseBoolFromBytes()`

### Binary protocol for parameterized queries

All queries use the Postgres extended query protocol (Parse/Bind/Execute) with binary
result format. Binary format is more compact on the wire and avoids text parsing
overhead for numerics, timestamps, and UUIDs. Multi-statement strings are not
supported since the extended protocol forbids them.

`bpdbi-core/…/impl/BaseConnection.java` (class javadoc)

### Binary parameter encoding

*Inspired by pgjdbc's binary Bind path.*

In addition to requesting binary *results*, bpdbi supports binary *parameter*
encoding via `PgEncoder.writeBindBinary()`. Sending an `int4` as 4 raw bytes
instead of ASCII digits (`"12345"` = 5 bytes + server-side parsing) saves both
wire bandwidth and server CPU. Encode helpers (`encodeInt4`, `encodeInt8`,
`encodeFloat8`, `encodeBool`, `encodeUuid`) produce the big-endian byte arrays
that Postgres expects.

`bpdbi-pg-client/…/codec/PgEncoder.java` — `writeBindBinary()`
`bpdbi-pg-client/…/codec/PgBinaryCodec.java` — `encodeInt4()`, `encodeInt8()`, etc.

### Optimized NUMERIC binary codec

*Inspired by pgjdbc's `ByteConverter.numeric()` fast path.*

Postgres NUMERIC uses a base-10000 representation with 2-byte digit groups. The
original codec built a `StringBuilder`, appended each digit, then parsed the
result with `new BigDecimal(string)` — double work. The optimized version stays
in `long` arithmetic for up to 4 base-10000 groups (16 decimal digits, covering
the vast majority of real-world numeric values) and calls
`BigDecimal.valueOf(long, scale)` directly. Only numbers exceeding `long` range
fall back to `BigInteger` arithmetic.

`bpdbi-pg-client/…/codec/PgBinaryCodec.java` — `decodeNumeric()`

---

## I/O & network

### Unsynchronized buffered I/O streams

*Inspired by pgjdbc's `PgBufferedOutputStream` and `VisibleBufferedInputStream`.*

Java's `BufferedOutputStream` and `BufferedInputStream` acquire a lock on every
read/write call. Since each bpdbi connection is single-threaded, both
`UnsyncBufferedOutputStream` and `UnsyncBufferedInputStream` remove all
synchronization. Additionally, data transfers larger than the buffer size bypass
the buffer entirely and go directly to/from the underlying stream, avoiding a
redundant copy for large payloads. `UnsyncBufferedInputStream` also provides a
`peek()` method for protocol lookahead without consuming the byte.

`bpdbi-core/…/impl/UnsyncBufferedOutputStream.java`
`bpdbi-core/…/impl/UnsyncBufferedInputStream.java`

### `TCP_NODELAY`

`PgConnection` sets `socket.setTcpNoDelay(true)` to disable Nagle's algorithm.
Without this, the OS might delay small writes waiting for more data, adding
artificial latency to pipelined roundtrips.

`PgConnection.java` line 133

### Single TCP write for pipelined batches

`executePipelinedBatch()` writes all Parse/Bind/Describe/Execute messages into
the encoder buffer, appends a single Sync, and flushes once. The server receives
the full batch in one TCP write and can start processing immediately, hiding
network latency across all statements.

`bpdbi-pg-client/…/PgConnection.java` — `executePipelinedBatch()`

### Encoder buffer pre-sizing

*Inspired by Vert.x SQL Client's `DataTypeEstimator` pattern.*

Before encoding a pipelined batch, `estimateExtendedQuerySize()` computes the
expected byte size per statement (accounting for SQL length, parameter count, and
per-message overhead). The encoder buffer is pre-allocated to the total estimate,
avoiding incremental resizing during batch encoding.

`bpdbi-pg-client/…/codec/PgEncoder.java` — `estimateExtendedQuerySize()`, `ensureCapacity()`

### Pipeline deadlock prevention

*Inspired by pgjdbc's `flushIfDeadlockRisk()` heuristic.*

A large pipelined batch can deadlock: the client blocks writing requests while
the server blocks sending responses (TCP buffers full in both directions). bpdbi
estimates server response size (~250 bytes/query) and inserts a mid-pipeline
Sync + drain when the cumulative estimate exceeds 64 KB (conservative TCP
receive buffer size). This splits large batches into safe chunks transparently.

`bpdbi-pg-client/…/PgConnection.java` — `MAX_BUFFERED_RECV_BYTES`, `ESTIMATED_RESPONSE_BYTES_PER_QUERY`

### Parse deduplication in pipelines

When a pipelined batch contains multiple executions of the same SQL (common with
`executeMany`), the Parse message is sent only once for consecutive identical
SQL strings. Subsequent statements skip directly to Bind/Execute, reducing wire
bytes and server parse overhead.

`bpdbi-pg-client/…/PgConnection.java` — `lastParsedSql` tracking in `executePipelinedBatch()`

---

## Caching

### Prepared statement cache (LRU)

*Design based on Vert.x SQL Client's `LruCache`.*

A per-connection LRU cache backed by `LinkedHashMap` with access-order. Avoids
re-parsing the same SQL on repeated executions. Evicted entries are collected via
`removeEldestEntry()` and closed server-side in batch.

`bpdbi-core/…/impl/PreparedStatementCache.java`

### Size-aware cache rejection

*Inspired by pgjdbc's `LruCache` dual-constraint design.*

The prepared statement cache tracks total SQL bytes across all entries. A single
SQL string that would consume more than 50% of the total byte budget is rejected
outright, preventing one huge dynamic query from evicting many smaller frequently
used statements.

`PreparedStatementCache.java` — `isOversized()`, `maxTotalSqlBytes`

### Statement invalidation via epoch

*Inspired by pgjdbc's `deallocateEpoch` mechanism.*

When `SET search_path`, `DEALLOCATE ALL`, or `DISCARD ALL` is detected in a
query, bpdbi increments an epoch counter and closes all cached prepared
statements server-side. Postgres does not send `ParameterStatus` for
`search_path` changes, so detection is done by inspecting the SQL text — the
same approach pgjdbc uses.

`bpdbi-pg-client/…/PgConnection.java` — `invalidatePreparedStatementCache()`, `deallocateEpoch`

### Stale-plan detection and targeted re-preparation

*Inspired by Vert.x SQL Client's `InvalidCachedStatementEvent` pattern.*

When a cached prepared statement fails with Postgres error `0A000` ("cached plan
must not change result type") or `26000` ("prepared statement does not exist"),
bpdbi evicts only that specific statement from the cache and retries the query
transparently. This is more surgical than the epoch-based full-cache
invalidation — other warm cache entries are preserved. Common trigger: `ALTER
TABLE` between cached queries.

`bpdbi-pg-client/…/PgConnection.java` — `isStalePlanError()`, cache-hit retry path

### `ParameterStatus` tracking during queries

Postgres `ParameterStatus` messages (server_encoding, TimeZone, DateStyle, etc.)
can arrive during any query response, not just at startup. All response-reading
loops now capture these messages and update the connection's `parameters()` map,
keeping the client-side view in sync with the server.

`bpdbi-pg-client/…/PgConnection.java` — `handleParameterStatus()` in all response loops

---

## Architectural

### Adaptive cursor fetch size

*Inspired by pgjdbc's `AdaptiveFetchCache`.*

When `ConnectionConfig.maxResultBufferBytes()` is set, cursor `read()` calls
auto-adjust the fetch count based on the average row size observed in the
previous batch: `adjustedCount = maxResultBufferBytes / avgRowSize`. This
prevents memory spikes for queries with unexpectedly wide rows while still
fetching many rows for narrow-row queries. The user-provided `count` acts as an
upper bound.

`bpdbi-core/…/ConnectionConfig.java` — `maxResultBufferBytes`
`bpdbi-pg-client/…/PgConnection.java` — `PgCursor.read()`

### No Netty, no event loops

Plain `java.net.Socket` with `UnsyncBufferedInputStream` and
`UnsyncBufferedOutputStream`. No channel pipelines, no thread pools, no
allocator framework. This keeps the library under 200 KB (vs. 5 MB+ for
Netty-based drivers), simplifies debugging, and works naturally with Java 21+
virtual threads where blocking I/O is cheap.

### Single-threaded connections, no locks

Connections are not thread-safe by design — one connection per (virtual) thread.
This eliminates all lock contention, atomic operations, and volatile reads on
the hot path. Concurrent access is handled at the pool level, where each thread
borrows its own connection.


