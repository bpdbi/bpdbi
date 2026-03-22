# bpdbi-pool — Connection pool for bpdbi

A lightweight, thread-safe connection pool for [bpdbi](../) connections.

## Setup

```kotlin
dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-pool")
    implementation("io.github.bpdbi:bpdbi-pg-client")
}
```

## Usage

```java
var pool = new ConnectionPool(
    () -> PgConnection.connect("localhost", 5432, "mydb", "user", "pass"),
    new PoolConfig()
        .maxSize(10)
        .maxIdleTimeMillis(300_000)
        .connectionTimeoutMillis(5000));

// Auto-release with callback
pool.withConnection(conn -> {
    conn.query("SELECT 1");
});

// With return value
String name = pool.withConnection(conn ->
    conn.query("SELECT name FROM users WHERE id = $1", 1)
        .first().getString("name"));

// Manual acquire/release
Connection conn = pool.acquire();
try {
    conn.query("SELECT 1");
} finally {
    pool.release(conn);
}

// Shut down
pool.close();
```

<details><summary>Kotlin equivalent</summary>

```kotlin
val pool = ConnectionPool(
    { PgConnection.connect("localhost", 5432, "mydb", "user", "pass") },
    PoolConfig()
        .maxSize(10)
        .maxIdleTimeMillis(300_000)
        .connectionTimeoutMillis(5000))

pool.withConnection { conn ->
    conn.query("SELECT 1")
}

pool.close()
```

</details>

## Configuration

| Setting                   | Default          | Description                                                        |
|---------------------------|------------------|--------------------------------------------------------------------|
| `maxSize`                 | 10               | Maximum number of connections                                      |
| `maxIdleTimeMillis`       | 600,000 (10 min) | Evict idle connections after this duration. 0 = disabled           |
| `maxLifetimeMillis`       | 0 (disabled)     | Evict connections after this total lifetime regardless of activity |
| `connectionTimeoutMillis` | 30,000 (30 sec)  | Max time to wait for a connection when the pool is full            |
| `maxWaitQueueSize`        | -1 (unbounded)   | Max number of threads waiting for a connection. -1 = no limit      |
| `poolCleanerPeriodMillis` | 1,000 (1 sec)    | Background eviction interval. 0 = no background eviction           |
| `validateOnBorrow`        | false            | Ping idle connections before handing them out                      |
| `afterAcquire`            | null             | Callback invoked after a connection is acquired                    |
| `beforeRecycle`           | null             | Callback invoked before a connection is returned to the idle pool  |

## Features

- **Lazy creation** — connections are created on demand up to `maxSize`
- **Idle eviction** — connections sitting idle beyond `maxIdleTimeMillis` are closed
- **Lifetime eviction** — connections older than `maxLifetimeMillis` are replaced
- **Background cleaner** — a daemon thread periodically evicts expired connections
- **Validate on borrow** — optionally ping connections before use to detect stale connections
- **Wait queue limit** — reject requests when too many threads are waiting
- **Lifecycle hooks** — run custom logic after acquire or before recycle (see below)
- **Virtual thread friendly** — blocking on `acquire()` is cheap with virtual threads
- **Works with PgConnection** — designed for the Postgres wire protocol driver

### Lifecycle hooks

Use `afterAcquire` and `beforeRecycle` to reset session state when connections move in
and out of the pool. If a hook throws, the connection is discarded instead of being used
or returned.

```java
var pool = new ConnectionPool(
    () -> PgConnection.connect("localhost", 5432, "mydb", "user", "pass"),
    new PoolConfig()
        .maxSize(10)
        .afterAcquire(conn -> conn.query("SET search_path TO myapp, public"))
        .beforeRecycle(conn -> conn.query("RESET ALL")));
```
