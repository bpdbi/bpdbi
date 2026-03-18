## Why Not Build on Top of pgjdbc?

A natural question: instead of implementing the Postgres wire protocol from scratch, why not build
bpdbi's pipelining on top of [pgjdbc](https://github.com/pgjdbc/pgjdbc) — the mature, battle-tested
JDBC driver?

Short answer: **pgjdbc doesn't expose what we need, and layering explicit pipelining on top of a
driver that hides it doesn't work.**

---

### The architectures are already very similar

Both bpdbi and pgjdbc use:

- Plain `java.net.Socket` (no Netty, no NIO)
- `BufferedInputStream` / `BufferedOutputStream` (8KB buffers)
- `TCP_NODELAY = true` (disable Nagle, rely on application-level buffering)
- Single-threaded blocking I/O

There's nothing pgjdbc gives us "for free" at the transport layer — we're already at the same level
of abstraction.

---

### pgjdbc's pipelining is implicit and hidden

pgjdbc does pipeline internally — when you call `executeBatch()`, it sends multiple
Parse/Bind/Execute messages before flushing. But this is **entirely hidden** behind the JDBC batch
API. There is no public API equivalent to:

```java
// bpdbi — caller controls the pipeline
conn.enqueue("INSERT INTO orders VALUES ($1, $2)", orderId, amount);
conn.enqueue("UPDATE inventory SET stock = stock - $1 WHERE item_id = $2", qty, itemId);
conn.enqueue("INSERT INTO audit_log VALUES ($1, $2)", orderId, "created");
List<RowSet> results = conn.flush();  // one TCP round-trip for all three
```

pgjdbc's `QueryExecutor.execute(Query[], ...)` is internal (`org.postgresql.core.v3`), not a stable
public API.

---

### The 64KB deadlock heuristic works against us

pgjdbc uses a single thread for both sending and receiving. To avoid deadlocking when both TCP
buffers fill up, it **conservatively estimates** response sizes and forces intermediate
Sync+flush+read cycles every ~64KB of estimated response data:

```
// From QueryExecutorImpl.java — the deadlock avoidance logic:
// "We guess at how much response data we can request from the server
//  before the server -> driver stream's buffer is full"
private static final int MAX_BUFFERED_RECV_BYTES = 64 * 1024;
```

This means pgjdbc will break up your batch into smaller chunks with intermediate round-trips —
**exactly the opposite** of what explicit pipelining is trying to achieve.

bpdbi avoids this by giving the caller control: you decide when to flush based on your knowledge of
your workload. If you're inserting 1000 small rows, you know the responses are tiny
(`CommandComplete` messages) and can safely enqueue them all before flushing.

---

### Three ways to try it, all bad

**1. Use the JDBC API (`addBatch`/`executeBatch`)**

`executeBatch()` owns the entire send+receive lifecycle. You can't interleave enqueues with
application logic, you can't mix queries and updates in the same pipeline, and you can't control when
the flush happens. The core value proposition is lost.

**2. Reach into `QueryExecutorImpl` internals**

Call `sendOneQuery()` without `sendSync()`, accumulate pending queues, then call
`processResults()`. This is fragile: those are package-private methods, the deadlock-avoidance logic
interferes, and you're coupled to implementation details that change between releases without notice.

**3. Fork pgjdbc**

At which point you're maintaining a fork of a ~90K LOC codebase to get behavior you already have in
~1400 lines of `PgConnection.java`.

---

### What we'd gain vs. lose

| Gain from pgjdbc                       | Lose by depending on it              |
|----------------------------------------|--------------------------------------|
| Extra auth methods (GSS, SSPI)         | Control over flush/pipeline timing   |
| COPY protocol support                  | Lazy `byte[][]` row decoding         |
| `reWriteBatchedInserts` optimization   | Binary result format control         |
| Broader type coverage                  | Simplicity (~1400 LOC vs ~90K LOC)   |
| JDBC compatibility                     | Direct wire protocol access          |

The gains are features we can add incrementally. The losses are architectural and fundamental.

---

### TL;DR

> bpdbi's value is **explicit control over pipelining** — when to enqueue, when to flush, when to
> read results. pgjdbc's architecture is designed around the opposite: the driver manages the
> pipeline internally behind a JDBC-shaped API. You can't meaningfully layer explicit pipelining on
> top of a driver that hides it. The current approach — own wire protocol implementation in ~1400 LOC
> — is the right one for this use case.
