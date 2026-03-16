## JDBC Is Definitely Showing Its Age

Yes — the fact that Vert.x had to **bypass JDBC entirely** to build a proper async DB client is a pretty clear indictment.

---

### What Vert.x SQL Client Does Instead

Rather than wrapping JDBC, `vertx-sql-client` implements the **database wire protocol directly** for Postgres, MySQL and DB2:

```
JDBC approach:
  Your Code → JDBC API → Driver → TCP → Postgre/MySQL/DB2

Vert.x approach:
  Your Code → Vert.x SQL Client → Postgre/MySQL/DB2
```

It speaks Postgres [wire protocol](https://www.postgresql.org/docs/current/protocol.html) (or MySQL's)
directly over non-blocking sockets. No JDBC involved at any layer.

```kotlin
// Fully async, no threads parked
pool.preparedQuery("SELECT * FROM orders WHERE id = $1")
    .execute(Tuple.of(orderId))
    .coAwait()  // suspends, doesn't block
```

---

### Where JDBC Shows Its Age

**1. No async from the ground up**

JDBC was designed before non-blocking I/O.
R2DBC was created in 2018 specifically to fill this gap, but it's a completely separate spec rather than an evolution of JDBC.

---

**2. Checked exceptions everywhere**

```java
// Every single JDBC call throws checked SQLException
try {
    Connection conn = dataSource.getConnection();
    PreparedStatement ps = conn.prepareStatement("SELECT...");
    ResultSet rs = ps.executeQuery();
} catch (SQLException e) {  // forced, even for trivial queries
    ...
}
```

Modern Java (and every ORM) wraps these into unchecked exceptions immediately. The checked exception model is widely considered a mistake in hindsight.

---

**3. Manual resource management**

```java
// You have to close everything, in the right order, yourself
Connection conn = null;
PreparedStatement ps = null;
ResultSet rs = null;
try {
    conn = dataSource.getConnection();
    ps = conn.prepareStatement("...");
    rs = ps.executeQuery();
    // ...
} finally {
    if (rs != null) rs.close();
    if (ps != null) ps.close();
    if (conn != null) conn.close();
}
```

`try-with-resources` (Java 7) helped, but the underlying design still requires manual lifecycle management.

---

**4. `ResultSet` is a mutable, stateful cursor**

```java
ResultSet rs = stmt.executeQuery();
rs.next();           // move cursor
rs.getString("name") // read current row
rs.next();           // move again
```

This is a very 1990s API design — stateful, imperative, not composable with streams or functional patterns.
There's no way to get a `List<Row>` back directly.

---

**5. Type mapping is stringly-typed and fragile**

```java
rs.getString("column_name")   // typo in column name? Runtime error.
rs.getLong("column_name")     // wrong type? Runtime error.
rs.getObject("column_name")   // gives you Object, you cast yourself
```

No compile-time safety. Modern clients (jOOQ, Vert.x SQL client) do much better here.

---

**6. No native connection pooling**

Pooling was bolted on via `javax.sql.DataSource` but left entirely to third parties.
In 2025 you still need HikariCP as a separate dependency to do something fundamental.

---

**7. No support for "pipelining"**

Cannot send multiple queries in one go, which can greatly reduce the number of db round trips needed within
an HTTP request cycle (assuming a web application).

---

### Why JDBC Isn't Going Away Though

- **25+ years of ecosystem** — every ORM, migration tool, monitoring agent, and APM tool speaks JDBC
- **Virtual threads** reduce the blocking cost significantly
- **Familiarity** — every Java developer knows it
- **Stability** — the spec hasn't needed to change much because SQL itself hasn't

---

### TL;DR

> JDBC is showing its age badly — no async, checked exceptions, stateful cursors, stringly-typed access,
> and no pooling. Vert.x bypassing it entirely isn't a workaround, it's the **correct architectural decision** 
> for a reactive stack. That said, JDBC's ecosystem weight means it'll be around for decades more — especially as
> virtual threads make its blocking nature less painful.