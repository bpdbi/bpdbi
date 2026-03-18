# Plan: ColumnBuffer Integration for Large Result Sets

## Context

When reading a result set with N rows × M columns, bpdbi currently allocates a `byte[][]` per row
and stores each column value as an individual `byte[]`. This creates N×M small arrays, putting
significant pressure on the GC for large result sets (10K+ rows).

The `ColumnBuffer` class already exists (`bpdbi-core/src/main/java/.../impl/ColumnBuffer.java`)
and is already used by `BaseConnection.buildRowSet()`. However, the current integration only
applies to the **buffered result set reading path** — it is not yet used in the streaming/cursor
paths, and the potential for zero-copy row access (reading directly from the shared buffer without
copying) is not yet exploited.

### Current state

`ColumnBuffer` provides column-oriented contiguous byte storage:
- M ColumnBuffers (one per column) instead of N×M `byte[]` arrays
- Append-only during result reading, random-access after
- Already implements `ColumnData` interface for Row access
- `Row` already supports ColumnBuffer-backed construction via `createBufferedRow()`

The `ColumnBuffer.get(rowIndex)` method currently **copies** bytes out. The zero-copy path is
available via `buffer(rowIndex)` + `offset(rowIndex)` + `length(rowIndex)`, but the binary codec
and type decoders don't yet use it.

---

## Design

### Goal

Make large result sets allocate O(M) arrays instead of O(N×M), and enable zero-copy decoding
where possible.

### Phase 1: Zero-copy binary decoding (high impact, moderate effort)

Add offset/length-aware decode methods to `PgBinaryCodec` and `MysqlBinaryCodec` that read
directly from the shared ColumnBuffer backing array without copying.

Current flow:
```
ColumnBuffer.get(row) → copies bytes → new byte[] → BinaryCodec.decodeInt4(bytes)
```

Target flow:
```
ColumnBuffer.buffer(row) → shared byte[] → BinaryCodec.decodeInt4(buffer, offset, length)
```

**Changes needed:**

1. Add overloads to `BinaryCodec` interface:
   ```java
   int decodeInt4(byte[] buffer, int offset, int length);
   long decodeInt8(byte[] buffer, int offset, int length);
   // etc. for all primitive types
   ```

2. Update `Row` getters (getInteger, getLong, etc.) to use the zero-copy path when backed by
   ColumnBuffers, falling back to the copy path for `byte[][]`-backed rows.

3. String decoding already works zero-copy via `new String(buffer, offset, length, UTF_8)`.

**Expected impact**: Eliminates the copy-per-cell overhead for all typed getters. Most impactful
for large result sets where rows are iterated and each cell is accessed once.

### Phase 2: Adaptive initial sizing (low effort, moderate impact)

ColumnBuffer currently uses fixed initial sizes (`initialRows=64, estimatedAvgSize=32`). For
queries that return known-small results (e.g., `SELECT count(*)`), this wastes memory. For
queries returning millions of rows, the buffer grows repeatedly.

**Changes needed:**

1. Pass estimated row count from the query context (if available) to ColumnBuffer construction.
   After the first fetch, the average row size is known — use it for subsequent buffer resizes.

2. For prepared statements with repeated execution, cache the average column sizes from previous
   executions and use them as initial estimates.

### Phase 3: Streaming path integration (moderate effort, niche impact)

The `queryStream()` and `stream()` methods currently create individual `Row` objects backed by
`byte[][]` for each row, since rows are processed one at a time and discarded. ColumnBuffer
doesn't help here because the whole point of streaming is to avoid buffering.

However, for `Cursor.read(batchSize)`, which returns batches of rows, ColumnBuffer integration
would reduce allocations per batch. The cursor already reads in batches internally, so this is
a natural fit.

**Changes needed:**

1. In `PgConnection`'s cursor implementation, use ColumnBuffer when reading each batch via
   `cursor.read(N)`. Each batch gets its own set of ColumnBuffers.

2. When a new batch is read, the previous batch's ColumnBuffers can be reused if the cursor
   consumer has finished with them. This requires a simple double-buffering scheme.

---

## Implementation Steps

### Step 1: Zero-copy BinaryCodec overloads

Modify: `bpdbi-core/src/main/java/io/github/bpdbi/core/BinaryCodec.java`
Modify: `bpdbi-pg-client/src/main/java/io/github/bpdbi/pg/impl/codec/PgBinaryCodec.java`
Modify: `bpdbi-mysql-client/src/main/java/io/github/bpdbi/mysql/impl/codec/MysqlBinaryCodec.java`

Add buffer+offset+length overloads for:
- `decodeBoolean`, `decodeInt2`, `decodeInt4`, `decodeInt8`
- `decodeFloat4`, `decodeFloat8`
- `decodeUUID`
- `decodeDate`, `decodeTime`, `decodeTimestamp`, `decodeTimestamptz`

### Step 2: Row zero-copy integration

Modify: `bpdbi-core/src/main/java/io/github/bpdbi/core/Row.java`

In ColumnBuffer-backed rows, change getters from:
```java
byte[] value = columnData.get(rowIndex);     // copies
return codec.decodeInt4(value);
```
to:
```java
byte[] buf = columnData.buffer(rowIndex);    // shared, no copy
return codec.decodeInt4(buf, columnData.offset(rowIndex), columnData.length(rowIndex));
```

### Step 3: Adaptive ColumnBuffer sizing

Modify: `bpdbi-core/src/main/java/io/github/bpdbi/core/impl/ColumnBuffer.java`

- Add `ColumnBuffer(int initialRows, int[] estimatedColumnSizes)` constructor
- Track actual average size during append for future reference
- Expose `averageValueSize()` for reuse by prepared statement cache

### Step 4: Cursor batch buffering

Modify: `bpdbi-pg-client/src/main/java/io/github/bpdbi/pg/PgConnection.java`

In the cursor read path, allocate ColumnBuffers per batch instead of per-row byte[][] arrays.

### Step 5: Tests

- Benchmark before/after with `MultiRowFetchBenchmark` (1000+ row result sets)
- Unit test zero-copy decoding produces identical results to copy-based decoding
- Test ColumnBuffer reuse across cursor batches
- Verify no memory leaks with streaming + cursor patterns

### Step 6: Benchmark verification

```bash
./gradlew :benchmark:jmh -PjmhIncludes="MultiRowFetch"
```

Compare allocation rates before/after using JMH's GC profiler:
```bash
./gradlew :benchmark:jmh -PjmhIncludes="MultiRowFetch" -PjmhResultFormat=text
# Add to jvmArgs: -prof gc
```

---

## Key Files

| File | Action |
|---|---|
| `bpdbi-core/src/main/java/.../BinaryCodec.java` | **Modify** — add zero-copy overloads |
| `bpdbi-core/src/main/java/.../Row.java` | **Modify** — use zero-copy path for ColumnBuffer rows |
| `bpdbi-core/src/main/java/.../impl/ColumnBuffer.java` | **Modify** — adaptive sizing |
| `bpdbi-pg-client/src/main/java/.../PgBinaryCodec.java` | **Modify** — implement overloads |
| `bpdbi-mysql-client/src/main/java/.../MysqlBinaryCodec.java` | **Modify** — implement overloads |
| `bpdbi-pg-client/src/main/java/.../PgConnection.java` | **Modify** — cursor batch buffering |

## Expected Performance Impact

- **Phase 1 (zero-copy)**: ~10-20% fewer allocations per result set, measurable on 1K+ row queries
- **Phase 2 (adaptive sizing)**: Reduced resize overhead for predictable query patterns
- **Phase 3 (cursor batches)**: Reduced GC for cursor-based large result processing

The biggest win is Phase 1 for workloads that read many rows with typed getters (getInteger,
getLong, etc.) — each getter call currently allocates a byte[] copy that is immediately discarded.
