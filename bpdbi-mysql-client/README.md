# bpdbi-mysql-client — MySQL wire protocol driver

A blocking MySQL driver that implements the MySQL client/server protocol directly
over `java.net.Socket`, with no dependencies on the official MySQL connector.

## Setup

```kotlin
dependencies {
    implementation(platform("io.github.bpdbi:bpdbi-bom:0.1.0"))
    implementation("io.github.bpdbi:bpdbi-mysql-client")
}
```

## Usage

```java
var conn = MysqlConnection.connect("localhost", 3306, "mydb", "user", "pass");

// Immediate query
RowSet rs = conn.query("SELECT id, name FROM users WHERE id = ?", 42);
String name = rs.first().getString("name");

// Transactions
try (var tx = conn.begin()) {
    tx.query("INSERT INTO users (name) VALUES (?)", "Alice");
    tx.commit();
}

conn.close();
```

## Pipelining limitations

The bpdbi API exposes `enqueue()`/`flush()` for pipelining multiple statements in a single
network roundtrip. This works well with the Postgres driver, which has first-class protocol
support for pipelining (the server processes a stream of Parse/Bind/Execute/Sync messages
and returns all results in order, with per-statement error isolation).

**MySQL's protocol is strictly request-response.** The server expects one command, sends one
complete response, then waits for the next command. There is no way to send multiple commands
and read all responses in bulk. As a result, `enqueue()`/`flush()` on MySQL is syntactic sugar
for a sequential loop of individual queries — it provides API compatibility with Postgres but
no network-level batching benefit.

### MariaDB improvements

MariaDB extends the MySQL protocol with two features that partially close the gap:

- **`COM_STMT_BULK_EXECUTE`** — a dedicated command for executing one prepared statement with
  many parameter sets in a single roundtrip. The server receives all rows at once and returns
  a single aggregate result (total affected rows, first auto-increment ID). Since MariaDB 11.5
  (`MARIADB_CLIENT_BULK_UNIT_RESULTS` capability), per-row results are also available. This is
  efficient for batch inserts/updates but only covers the `executeMany` use case, not general
  mixed-statement pipelines.

- **Optimistic send** — MariaDB connectors can fire off multiple commands without waiting for
  each response, relying on TCP buffering. This hides latency (especially over slow networks)
  but is a client-side trick: the server has no concept of pipeline boundaries, so there is no
  error isolation. If command N fails, commands N+1 through M have already been sent and their
  responses must be drained and discarded.

Neither feature matches Postgres-style pipelining, where the server understands the pipeline
structure and provides per-statement error boundaries. bpdbi does not currently implement
either MariaDB extension — the MySQL driver uses the standard request-response protocol and
works identically against MySQL and MariaDB.
