# bpdbi — Blocking Pipelined SQL Client

## Build & Test

```bash
# Build everything (skip tests)
./gradlew build -x test

# Run all tests (needs Docker for Testcontainers)
./gradlew test

# Run tests for a specific module
./gradlew :bpdbi-pg-client:test
./gradlew :bpdbi-core:test

# Run a single test class
./gradlew :bpdbi-pg-client:test --tests "io.github.bpdbi.pg.PgConnectionTest"
```

## Benchmarks

```bash
# Run all benchmarks with 1ms latency per direction (2ms round-trip via Toxiproxy)
./gradlew :benchmark:jmh -PbenchLatencyMs=1

# Run a specific benchmark scenario
./gradlew :benchmark:jmh -PbenchLatencyMs=1 -PjmhIncludes="SingleRowLookup"

# Run only Vert.x benchmarks
./gradlew :benchmark:jmh -PbenchLatencyMs=1 -PjmhIncludes="vertx"

# Run without latency simulation (direct connection)
./gradlew :benchmark:jmh

# Tune iterations (defaults: fork=1, warmup=3, measurement=5)
./gradlew :benchmark:jmh -PbenchLatencyMs=1 -PjmhFork=1 -PjmhWarmupIterations=2 -PjmhIterations=3
```

## Project Structure

- `bpdbi-core/` — Database-agnostic API: Connection, Row, RowSet, pipelining, type registries
- `bpdbi-pg-client/` — Postgres wire protocol driver
- `bpdbi-kotlin/` — Kotlin extensions, binders for Kotlin types and mapping of rows via `kotlinx.serialization`
- `bpdbi-record-mapper/` — Java record mapping of rows via reflection
- `bpdbi-javabean-mapper/` — JavaBean/POJO mapping of rows via reflection
- `bpdbi-pool/` — Simple connection pool
- `examples/` — Runnable examples

## Architecture

- **Pipelining-first**: `enqueue()` + `flush()` batches statements in one TCP write
- **Lazy decoding**: Row stores raw `byte[][]`, decodes on getter access
- **Binary protocol**: Uses the Postgres extended query protocol with binary results for all
  queries (including parameterless ones). Multi-statement strings (`SELECT 1; SELECT 2`) are
  not supported
- **BaseConnection**: Abstract class with shared pipeline logic; PgConnection extends it
- **No Netty**: Plain `java.net.Socket` + `BufferedInputStream`/`BufferedOutputStream`
- **Java 21+**: Works especially well with virtual threads, has optional mapper for records,
  and uses sealed types internally

## Conventions

- JSpecify `@Nullable` annotations on all public API
- `DbException` is the base for all database errors (unchecked)
- Parameters: binary-encoded for supported types (int, long, etc.), text fallback for others
- Results: always binary format via the extended query protocol
- Tests use Testcontainers (Postgres 16-alpine)
- Postgres is **not** referred to as "PostgreSQL" anywhere in the project, just "Postgres"
- Do **not** use asterisk `import` statements
- All `@SuppressWarnings` and `@Suppress` (Kotlin) annotations should come with a comment explaining
  why it's needed
- Do **not** use `@NullMarked` in `package-info.java` files but explicitly mark types `@NonNull` or `@Nullable`
- Use **`OID`/`OIDs`** (all-caps) in symbol names, never `Oid`/`Oids` — e.g. `typeOID`, `paramTypeOIDs`, `PgOIDs`
