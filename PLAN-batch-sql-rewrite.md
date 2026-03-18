# Plan: Multi-Row INSERT Rewriting

## Context

Benchmarks with 1ms simulated latency revealed bpdbi's bulk insert is **22-33x slower** than pgjdbc.
Three compounding issues were identified:

1. **Sync-per-statement**: `flush()` calls `executeExtendedQuery()` per enqueued statement, each sending
   Parse/Bind/Describe/Execute/Sync and blocking on ReadyForQuery. For 50 inserts = 50 round-trips.
   pgjdbc sends all Bind/Execute pairs with **one Sync at the end** = 1 round-trip.

2. **Parse-per-statement**: bpdbi sends a Parse message for every iteration even when SQL is identical.
   pgjdbc skips Parse after the first execution (`isPreparedFor()` check).

3. **No multi-row INSERT rewriting**: pgjdbc's `BatchedQuery` rewrites
   `INSERT INTO t VALUES ($1,$2)` × 50 into `INSERT INTO t VALUES ($1,$2),($3,$4),...,($99,$100)` × 1.
   This reduces 50 Bind/Execute pairs to ~3 (using power-of-2 blocks: 32+16+2).

This plan addresses **issue #3** only — multi-row INSERT rewriting. Issues #1 and #2 (true pipelining
in `flush()`) are separate, larger changes to the protocol layer.

### How pgjdbc does it

`PgPreparedStatement.transformQueriesAndParameters()` (`PgPreparedStatement.java:1769-1816`):
- Only applies to `BatchedQuery` instances (INSERT with VALUES clause)
- Combines N single-row inserts into multi-row blocks using power-of-2 sizes (max 128 per block)
- Example: 50 rows → 32 + 16 + 2 = 3 combined queries
- Each combined query has one Parse/Bind/Execute cycle with all params concatenated

### How Vert.x does it

Vert.x does **not** do SQL rewriting. It pipelines at the protocol level instead:
Parse once → N × (Bind + Execute) → one Sync → read all responses.
This is effective but orthogonal to SQL rewriting.

### Why SQL rewriting matters even with pipelining

Even with true pipelining (issue #1 fixed), multi-row INSERT is still faster because:
- Fewer Bind/Execute message pairs = less protocol overhead
- Postgres processes one INSERT with 50 value-tuples faster than 50 separate INSERTs
  (one plan, one WAL entry, one index update batch)
- Reduces server-side per-statement overhead (planning, locking)

---

## Design

### Scope

Add a `rewriteInsertMany()` method that transforms `executeMany("INSERT ... VALUES ($1,$2)", paramSets)`
into combined multi-row INSERTs at the SQL level, **before** hitting the protocol layer.

This is implemented in `BaseConnection` (database-agnostic) but only works for:
- INSERT statements with a VALUES clause containing positional parameters
- `executeMany()` calls with 2+ parameter sets

Non-matching statements fall through to the current behavior (enqueue N + flush).

### SQL rewriting strategy

**Input:**
```
sql:       "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
paramSets: [[1,"x",true], [2,"y",false], [3,"z",true]]
```

**Output (single combined statement):**
```
sql:       "INSERT INTO t (a, b, c) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"
params:    [1,"x",true, 2,"y",false, 3,"z",true]
```

**Blocking strategy** (following pgjdbc): use power-of-2 blocks, max 128 rows per block.
- 50 rows → 32 + 16 + 2 = 3 queries
- 200 rows → 128 + 64 + 8 = 3 queries
- 1 row → falls through to normal path

**Placeholder renumbering:**
- Postgres uses `$1, $2, ...` — renumber for each row: `$1,$2,$3` → `$4,$5,$6` → `$7,$8,$9`
- MySQL uses `?` — no renumbering needed, just repeat the `(?, ?, ?)` tuple

### API

No new public API. The optimization is internal to `executeMany()`:

```java
// BaseConnection.executeMany() — changed behavior:
public List<RowSet> executeMany(String sql, List<Object[]> paramSets) {
    if (paramSets.size() >= 2) {
        String valuesClause = InsertRewriter.extractValuesClause(sql);
        if (valuesClause != null) {
            return executeRewrittenInsert(sql, valuesClause, paramSets);
        }
    }
    // Fallback: current behavior
    for (Object[] params : paramSets) {
        enqueue(sql, params);
    }
    return flush();
}
```

### SQL parsing approach

Minimal parsing — NOT a full SQL parser. Just find the VALUES clause:

1. Find `VALUES` keyword (case-insensitive) after the last `)` that closes the column list
2. Extract the values tuple: everything from `(` to `)` after VALUES
3. Validate it contains only `$N` placeholders, commas, and whitespace
4. Count parameters per tuple

This is intentionally conservative: if the SQL doesn't match the simple pattern, skip rewriting
and fall through to the existing behavior. No risk of breaking anything.

**Edge cases to reject (fall through to normal path):**
- `INSERT ... SELECT ...` (no VALUES)
- `INSERT ... VALUES ($1) RETURNING ...` — needs special handling since RETURNING produces
  one result row per value-tuple. For now, reject if RETURNING is present.
- `INSERT ... ON CONFLICT ...` — works fine with multi-row VALUES, no special handling needed
- Subqueries in VALUES: `VALUES ((SELECT ...), $1)` — rejected by the simple parser
- CTEs: `WITH ... INSERT ...` — rejected (VALUES not at expected position)

---

## Implementation Steps

### Step 1: `InsertRewriter` utility class

New file: `bpdbi-core/src/main/java/io/github/bpdbi/core/impl/InsertRewriter.java`

```java
public final class InsertRewriter {
    // Returns null if SQL doesn't match the rewritable pattern
    public static RewriteResult tryRewrite(
        String sql, int paramSetsSize, int paramsPerTuple, String placeholderPrefix);

    public record RewriteResult(List<RewrittenBlock> blocks) {}
    public record RewrittenBlock(String sql, int rowCount) {}
}
```

Responsibilities:
- Parse SQL to find VALUES clause and extract the tuple template (e.g., `($1, $2, $3)`)
- Detect RETURNING clause → return null (not rewritable)
- Generate combined SQL for each power-of-2 block
- Renumber `$N` placeholders for Postgres, repeat `?` tuples for MySQL

### Step 2: Integrate into `BaseConnection.executeMany()`

Modify: `bpdbi-core/src/main/java/io/github/bpdbi/core/impl/BaseConnection.java`

- Call `InsertRewriter.tryRewrite()` when `paramSets.size() >= 2`
- If rewrite succeeds: execute each block as a single `query(rewrittenSql, combinedParams)`
- Combine the RowSet results (sum `rowsAffected`)
- If rewrite fails: fall through to existing enqueue+flush behavior

### Step 3: Handle placeholder prefix differences

`BaseConnection` already has `abstract String placeholderPrefix()`:
- PgConnection returns `"$"`
- MysqlConnection returns `"?"`

Pass this to `InsertRewriter` to generate correct placeholder syntax.

For MySQL, the tuple template is just `(?, ?, ?)` repeated — no renumbering.
For Postgres, `($1, $2, $3)` becomes `($1, $2, $3), ($4, $5, $6), ...`.

### Step 4: Tests

Add tests to `bpdbi-core` (unit tests for InsertRewriter, no DB needed):
- Various INSERT formats: with/without column list, mixed whitespace, case variations
- RETURNING clause detection → returns null
- ON CONFLICT clause → works (rewrites)
- Block sizing: 1 row (no rewrite), 3 rows, 50 rows, 200 rows, 128 rows (exact power of 2)
- Placeholder renumbering correctness for both `$N` and `?` styles
- Non-INSERT statements → returns null
- INSERT ... SELECT → returns null

Integration tests in `bpdbi-pg-client` and `bpdbi-mysql-client`:
- `executeMany()` with 50 INSERTs, verify all rows inserted correctly
- `executeMany()` with RETURNING clause, verify fallback behavior
- `executeMany()` with ON CONFLICT, verify combined statement works
- Single paramSet → no rewrite, normal behavior

### Step 5: Benchmark verification

Re-run `BulkInsertBenchmark` with 1ms latency to measure improvement:
```bash
./gradlew :benchmark:clean :benchmark:jmhJar -PbenchLatencyMs=1 && \
/home/cies/.jdks/corretto-21.0.7/bin/java -DbenchLatencyMs=1 \
  -jar benchmark/build/libs/benchmark-0.1.0-SNAPSHOT-jmh.jar \
  ".*BulkInsert.*" -f 0 -wi 1 -i 1
```

---

## Key Files

| File | Action |
|---|---|
| `bpdbi-core/src/main/java/io/github/bpdbi/core/impl/InsertRewriter.java` | **New** — SQL rewriting logic |
| `bpdbi-core/src/main/java/io/github/bpdbi/core/impl/BaseConnection.java` | **Modify** — `executeMany()` to call InsertRewriter |
| `bpdbi-core/src/test/java/io/github/bpdbi/core/impl/InsertRewriterTest.java` | **New** — unit tests |
| `bpdbi-pg-client/src/test/java/io/github/bpdbi/pg/PgConnectionTest.java` | **Modify** — integration tests |
| `bpdbi-mysql-client/src/test/java/io/github/bpdbi/mysql/MysqlConnectionTest.java` | **Modify** — integration tests |

## Expected Performance Impact

With 50-row batch at 1ms latency:
- **Before**: 50 separate INSERTs × (Parse+Bind+Describe+Execute+Sync) = 50 round-trips ≈ 8 ops/s
- **After (rewrite only)**: 3 combined INSERTs (32+16+2 blocks) × same protocol = 3 round-trips ≈ ~100 ops/s
- **pgjdbc reference**: ~170-265 ops/s (also has Parse-caching + single-Sync pipelining)

Rewriting alone should give a **~12x improvement** for bulk inserts. Combined with future
pipelining fixes (issues #1 and #2), bpdbi would match or exceed pgjdbc.
