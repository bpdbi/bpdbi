# djb — Blocking Pipelined SQL Client

## Build & Test

```bash
# Build everything (skip tests)
./gradlew build -x test

# Run all tests (needs Docker for Testcontainers)
./gradlew test

# Run tests for a specific module
./gradlew :djb-pg-client:test
./gradlew :djb-mysql-client:test
./gradlew :djb-core:test

# Run a single test class
./gradlew :djb-pg-client:test --tests "io.djb.pg.PgConnectionTest"
```

## Project Structure

- `djb-core/` — Database-agnostic API: Connection, Row, RowSet, pipelining, type registries
- `djb-pg-client/` — PostgreSQL wire protocol driver (depends on djb-core)
- `djb-mysql-client/` — MySQL wire protocol driver (depends on djb-core)
- `djb-kotlin/` — Kotlin extensions via kotlinx.serialization (depends on djb-core)
- `djb-reflective-record-mapper/` — Java record mapping via reflection
- `djb-javabean-mapper/` — JavaBean/POJO mapping via reflection
- `djb-pool/` — Simple connection pool
- `examples/` — Runnable examples

## Architecture

- **Pipelining-first**: `enqueue()` + `flush()` batches statements in one TCP write
- **Lazy decoding**: Row stores raw `byte[][]`, decodes on getter access
- **Binary protocol**: PG requests binary results via Bind message; MySQL uses COM_STMT_EXECUTE
- **BaseConnection**: Abstract class with shared pipeline logic; PG/MySQL extend it
- **No Netty**: Plain `java.net.Socket` + `BufferedInputStream`/`BufferedOutputStream`
- **Java 21+**: Designed for virtual threads, records, sealed types

## Conventions

- JSpecify `@Nullable` annotations on all public API
- `DbException` is the base for all database errors (unchecked)
- Parameters: text-encoded via `TypeRegistry.bind()`, sent as text format codes
- Results: PG uses binary format (via Bind result format codes), MySQL binary via prepared statements
- Tests use Testcontainers (PostgreSQL 16-alpine, MySQL 8.0)
- No ORM — SQL-first, explicit queries
