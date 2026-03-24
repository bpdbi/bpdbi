# bpdbi-core — Public API and shared abstractions

The public API surface for [bpdbi](../): `Connection`, `Row`, `RowSet`, `TypeRegistry`,
`BinaryCodec`, and all the types that extension modules (`bpdbi-pool`, `bpdbi-kotlin`,
`bpdbi-record-mapper`, etc.) depend on.

## Module boundary

This module contains no driver implementation — it cannot connect to a database by itself.
You always need a driver module (currently `bpdbi-pg-client`) to get a concrete `Connection`.

The split exists so that extension modules can depend on the API without pulling in the
Postgres wire protocol, socket I/O, or Testcontainers. This keeps their dependency footprint
small and lets them be tested with `StubBinaryCodec` (a text-parsing fake) instead of a
running database.

The API is Postgres-shaped by design: `ColumnDescriptor` carries type OIDs,
`BinaryCodec.decodeToString()` dispatches on OIDs, `Connection` uses `$1, $2` placeholders,
and `ConnectionConfig` parses `postgresql://` URIs. These are not accidental leaks — bpdbi is
a Postgres driver, and this module is its public API, not a database-agnostic abstraction layer.

## Key types

| Type | Role |
|---|---|
| `Connection` | Main interface — query, enqueue, flush, prepare, cursor, stream |
| `Row` | Single result row with lazy binary decoding |
| `RowSet` | List of rows from a query result |
| `TypeRegistry` | Custom type mappings for param encoding and result decoding |
| `BinaryCodec` | Interface for binary wire format decoding (implemented per-driver) |
| `RowMapper<T>` | Functional interface for mapping rows to Java objects |
| `JsonMapper` | Pluggable JSON serialization (Jackson, Gson, etc.) |
| `ConnectionConfig` | Fluent builder / URI parser for connection options |
| `Transaction` | Auto-rollback transaction wrapper |

## Not a standalone dependency

Do not depend on `bpdbi-core` alone. Always pair it with a driver:

```kotlin
dependencies {
    implementation("io.github.bpdbi:bpdbi-pg-client") // includes bpdbi-core transitively
}
```
