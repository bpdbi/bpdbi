# Benchmark Suite

JMH benchmarks comparing bpdbi against JDBC (raw, Jdbi, Hibernate) and Vert.x PG client.
All benchmarks use **pooled connections** — we removed unpooled variants because connection
setup (TCP + SASL auth) dominates the measurement and tells you nothing about query performance.

## Running

```bash
# Full suite (takes ~20 minutes)
./gradlew :benchmark:jmh

# With simulated network latency (1ms per direction via Toxiproxy)
./gradlew :benchmark:jmh -PbenchLatencyMs=1

# Single scenario
./gradlew :benchmark:jmh -PjmhIncludes="SingleRowLookup"

# Quick run (fewer iterations, less stable but faster feedback)
./gradlew :benchmark:jmh -PjmhWarmupIterations=1 -PjmhIterations=2

# Only Vert.x benchmarks
./gradlew :benchmark:jmh -PjmhIncludes="vertx"
```

Results are written to `build/reports/jmh/results.json`.

## What's Being Compared

| Label prefix     | Driver                              | Pool           |
|------------------|-------------------------------------|----------------|
| `bpdbi_pool_*`   | bpdbi (blocking, pipelined)         | bpdbi pool     |
| `jdbc_raw`       | PG JDBC driver                      | HikariCP       |
| `jdbc_jdbi`      | Jdbi (over JDBC)                    | HikariCP       |
| `jdbc_hibernate` | Hibernate ORM (over JDBC)           | Hibernate pool |
| `vertx_pool_*`   | Vert.x reactive PG client (blocked) | Vert.x pool    |

The Vert.x benchmarks call `.toCompletionStage().toCompletableFuture().join()` to block,
since JMH is synchronous. This adds overhead from the async-to-sync hop that wouldn't exist
in a native reactive pipeline — the numbers represent Vert.x used from blocking code (e.g.
virtual threads), not its optimal async mode.

## Scenarios

### SingleRowLookupBenchmark
Fetch one user by primary key (7 columns). Tests raw getter access, record mapping,
and bean mapping. Parameterized by `userId`.

### MultiRowFetchBenchmark
Fetch all products matching a category (~100 rows). Tests iteration and multi-row
deserialization overhead. Parameterized by `categoryIdx`.

### JoinQueryBenchmark
Join + aggregate query (orders with user join, item count, LIMIT 20). Tests complex
query performance with grouped results. Parameterized by `userId`.

### BulkInsertBenchmark
Batch insert 50 rows. Compares bpdbi's `executeMany`, JDBC batch, Jdbi batch,
Hibernate native mutations, and Vert.x `executeBatch`.

### PreparedStatementReuseBenchmark
Execute 100 lookups on a single pooled connection. Tests prepared statement caching
and per-query overhead when amortized over many executions.

### KotlinRowMapperBenchmark
bpdbi's Kotlin extensions (`queryOneAs<T>`, `queryAs<T>`) using `kotlinx.serialization`
for row mapping. Pooled only.

## Infrastructure

- **Database**: Postgres 16-alpine via Testcontainers (one container per JMH fork)
- **Latency simulation**: Toxiproxy adds configurable latency in both directions
  (`-PbenchLatencyMs=N`). When 0 (default), Toxiproxy is skipped and connections go
  direct to the container.
- **Pool size**: 10 connections for all pools (bpdbi, HikariCP, Hibernate, Vert.x)
- **Schema**: `users` (1000 rows), `products` (500), `orders` (5000), `order_items`
  (1-5 per order), `bench_orders` (truncated between iterations)

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
