# bpdbi-pg-client — Postgres wire protocol driver

The Postgres driver for [bpdbi](../). Implements the Postgres v3 wire protocol over
plain `java.net.Socket` with `BufferedInputStream`/`BufferedOutputStream` — no Netty,
no NIO, no external dependencies beyond the JDK.

## Relationship with bpdbi-core

This module provides the concrete `PgConnection` that implements the `Connection` interface
defined in `bpdbi-core`. The split is intentional: extension modules (`bpdbi-pool`,
`bpdbi-kotlin`, `bpdbi-record-mapper`, `bpdbi-javabean-mapper`) depend only on `bpdbi-core`
so they don't pull in the wire protocol, socket I/O, or SCRAM authentication code.

The core module's API is Postgres-shaped (type OIDs, `$N` placeholders, `postgresql://` URIs)
because Bpdbi (for now) is Postgres specific.
The module boundary is about dependency hygiene, not database portability.
