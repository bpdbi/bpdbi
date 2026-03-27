# Benchmark Suite

JMH benchmarks comparing **bpdbi** against JDBC-based libraries (raw JDBC, Jdbi, Hibernate,
jOOQ, Sql2o, Spring JdbcTemplate).
All benchmarks use **pooled connections** — unpooled variants were removed because connection
setup (TCP + SASL auth) dominates the measurement and tells you nothing about query performance.

We used to also include a benchmark for the Vert.x reactive PG client, but since the benchmark
library we use is threaded (and Vert.x is async/reactive) the results were not representative.
We decided to remove it.

## Running

```bash
# Smoke test (useless numbers, just validates the benchmarks compile and run)
./gradlew :benchmark:jmh -PjmhFork=0 -PjmhWarmupIterations=0 -PjmhIterations=1 -PbenchLatencyMs=0

# Full suite with simulated network latency (1ms per direction via Toxiproxy)
./gradlew :benchmark:jmh -PjmhFork=3 -PjmhWarmupIterations=2 -PjmhIterations=3 -PbenchLatencyMs=1

# Single scenario
./gradlew :benchmark:jmh -PjmhIncludes="SingleRowLookup" -PbenchLatencyMs=1

# Only Kotlin benchmarks
./gradlew :benchmark:jmh -PjmhIncludes="Kotlin" -PbenchLatencyMs=1
```

Results are written to `$PROJECT_ROOT/benchmark/build/reports/jmh/results.json`.

## What's Being Compared

| Label prefix     | Driver / Library                    | Pool           |
|------------------|-------------------------------------|----------------|
| `bpdbi_*`        | bpdbi (blocking, pipelined)         | bpdbi pool     |
| `jdbc_raw`       | PG JDBC driver                      | HikariCP       |
| `jdbc_jdbi_*`    | Jdbi (over JDBC)                    | HikariCP       |
| `jdbc_hibernate` | Hibernate ORM (over JDBC)           | Hibernate pool |
| `jdbc_jooq`      | jOOQ (over JDBC)                    | HikariCP       |
| `jdbc_sql2o`     | Sql2o (over JDBC)                   | HikariCP       |
| `jdbc_spring`    | Spring JdbcTemplate (over JDBC)     | HikariCP       |

Vert.x benchmarks are currently **disabled** (commented out). They call
`.toCompletionStage().toCompletableFuture().join()` to block, since JMH is synchronous.
This adds overhead from the async-to-sync hop that wouldn't exist with a fully reactive
code base — the numbers represent Vert.x used from blocking code (e.g. virtual threads),
not its optimal async mode.

## Scenarios

### SingleRowLookupBenchmark
Fetch one user by primary key (7 columns). Tests raw getter access, record mapping,
bean mapping, and equivalents in each library. Parameterized by `userId`.

### MultiRowFetchBenchmark
Fetch all products matching a category (~100 rows). Tests iteration and multi-row
deserialization overhead. Parameterized by `categoryIdx`.

### JoinQueryBenchmark
Join + aggregate query (orders with user join, item count, LIMIT 20). Tests complex
query performance with grouped results. Parameterized by `userId`.

### BulkInsertBenchmark
Batch insert 50 rows. Compares bpdbi's `executeMany` (pipelined), JDBC batch, Jdbi batch,
Hibernate native mutations, jOOQ batch, Sql2o loop, and Spring `batchUpdate`.

### ManyBindingsBenchmark
INSERT with 15 mixed-type bindings (UUID, String, int, long, BigDecimal, boolean,
LocalDateTime, null) across 50 rows, plus a SELECT reading back 20 wide rows.
Tests type encoding/decoding overhead with many parameter types.

### PipelinedLookupBenchmark
Enqueue 20 queries and flush once vs. sequential execution. Demonstrates bpdbi's
pipelining advantage — a capability with no JDBC equivalent.

### TransactionBenchmark
Transaction overhead: BEGIN/COMMIT wrapping, read-only transactions, pipelined inserts
(10 INSERTs in one flush) vs. sequential inserts, and the `inTransaction()` convenience API.

### StreamingFetchBenchmark
Streaming/cursor-based reading of 1000 rows. Compares bpdbi's streaming (buffered,
constant-memory stream, callback-based) against JDBC fetch-all, JDBC `setFetchSize`
(server-side cursor), and Jdbi's `ResultIterator`.

### CursorBenchmark
Cursor-based progressive reading of 1000 rows within a transaction. Compares bpdbi's
cursor (batched FETCH with configurable size), bpdbi stream, and JDBC `setFetchSize`.

### LargeValueBenchmark
Reading 200 rows with ~10KB text values per row. Tests throughput with large payloads
where data transfer time matters more than per-query overhead.

### PreparedStatementReuseBenchmark
Execute 100 lookups on a single pooled connection. Tests prepared statement caching
and per-query overhead when amortized over many executions.

### NamedParamBenchmark
Named parameter parsing overhead (`:name` vs `$1`). Compares bpdbi's positional
parameters, bpdbi's named parameters, and Jdbi's named parameters.

### SingleRowLookupKotlinBenchmark
bpdbi's Kotlin extensions (`queryOneAs<T>()`) using `kotlinx.serialization` for row
mapping vs. Jdbi's `mapTo<T>()` with reflection. Parameterized by `userId`.

### KotlinRowMapperBenchmark
bpdbi's Kotlin extensions for both single-row (`queryOneAs<T>()`) and multi-row
(`queryAs<T>()`) mapping using `kotlinx.serialization`, compared against Jdbi's
reflection-based mapper. Parameterized by `categoryIdx` and `userId`.

## Infrastructure

- **Database**: Postgres 16-alpine via Testcontainers (one container per JMH fork)
- **Latency simulation**: Toxiproxy adds configurable latency in both directions
  (`-PbenchLatencyMs=N`). When 0 (default), Toxiproxy is skipped and connections go
  direct to the container.
- **Pool size**: 10 connections for all pools
- **Schema**: `users` (1000 rows), `products` (500), `orders` (5000), `order_items`
  (1-5 per order), `bench_orders` (truncated between iterations), `events` (wide table
  for ManyBindingsBenchmark)

## Design Decisions

**Why pooled only?** Unpooled benchmarks create a new TCP connection + SASL handshake per
iteration (~11ms), which completely masks query performance differences. Every unpooled
benchmark scored ~80-90 ops/s regardless of query complexity.

**Why block on Vert.x futures?** JMH measures synchronous throughput. Wrapping Vert.x in
`CompletableFuture.join()` is the fairest comparison for the common case of using a reactive
driver from blocking code (e.g. with virtual threads). It's a slight pessimization of Vert.x
but reflects real-world usage patterns.

**Why Toxiproxy for latency?** Without simulated latency, benchmarks run over localhost
loopback with ~0.05ms RTT. This favors drivers with lower per-query overhead but hides
pipelining benefits. Adding even 1ms of latency makes the results more representative of
production deployments where network cost dominates.
