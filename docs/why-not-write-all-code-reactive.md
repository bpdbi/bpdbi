## Downsides of Reactive: Why Not Use It Everywhere?

The gains are real, but reactive comes with **significant costs** that often outweigh the benefits.

---

### 1. Code Complexity — The Biggest One

Reactive code is genuinely harder to read and write:

```java
// Blocking — obvious what's happening
public Invoice generateInvoice(long orderId) {
  Order order = orderRepo.findById(orderId);
  Customer customer = customerRepo.findById(order.getCustomerId());
  List<Item> items = itemRepo.findByOrderId(orderId);
  return invoiceService.build(order, customer, items);
}

// Reactive — same logic, now good luck
public Uni<Invoice> generateInvoice(long orderId) {
  return orderRepo.findById(orderId)
      .flatMap(order ->
                   Uni.combine().all()
                       .unis(
                           customerRepo.findById(order.getCustomerId()),
                           itemRepo.findByOrderId(orderId)
                       )
                       .asTuple()
                       .map(tuple -> invoiceService.build(
                           order, tuple.getItem1(), tuple.getItem2()
                       ))
      );
}
```

This is a **simple** example. Real business logic with conditionals, loops, and error handling
becomes significantly
worse.

---

### 2. Debugging is Painful

Stack traces become nearly useless:

```
// Blocking stack trace — clear and actionable
at com.myapp.InvoiceService.build(InvoiceService.java:42)
at com.myapp.OrderService.generateInvoice(OrderService.java:28)
at com.myapp.OrderController.getInvoice(OrderController.java:15)

// Reactive stack trace — good luck
at io.smallrye.mutiny.operators.UniSerializedSubscriber.onFailure(...)
at io.smallrye.mutiny.operators.uni.UniOnItemFlatMap$...)
at io.vertx.core.impl.EventLoopContext.emit(...)
at ... 30 more framework frames ...
at com.myapp.InvoiceService.build(InvoiceService.java:42)  // ← buried
```

The actual error source is buried under framework internals. Mutiny and Reactor both have tooling to
help, but it's
never as clean as blocking code.

---

### 3. The "Reactive Plague" — It Spreads Everywhere

Reactive is **infectious**. Once one layer goes reactive, pressure builds to make everything
reactive:

```java
// You made your DB layer reactive...
Uni<Order> findOrder(long id);

// Now your service must be reactive...
Uni<Invoice> generateInvoice(long id);

// Now your controller must be reactive...
Uni<Response> getInvoice(long id);

// Now your tests must handle async...
@Test
void testGetInvoice() {
  Invoice result = generateInvoice(1L)
      .await().indefinitely(); // awkward in tests
}
```

There's no clean boundary — you can't easily mix blocking and reactive layers without risking
blocking the event loop.

---

### 4. Blocking the Event Loop — Silent Killer

The most dangerous reactive mistake: accidentally blocking on an event loop thread:

```java
// NEVER do this in a reactive handler
@GET
public Uni<String> getResult() {
  return Uni.createFrom().item(() -> {
    Thread.sleep(5000);           // ❌ BLOCKS the event loop
    return legacyService.call();  // ❌ blocking legacy library
  });
}
```

This doesn't throw an error — it **silently kills throughput** for every other request. Vert.x has a
blocked thread
checker, but it's easy to miss. With blocking code, this problem literally cannot happen.

---

### 5. Libraries and Ecosystem Compatibility

Much of the Java ecosystem is **still blocking**:

- JDBC (most relational DB drivers) — blocking by design
- Many legacy REST clients
- Spring `@Transactional` — doesn't compose with reactive naturally
- JPA / Hibernate (though Hibernate Reactive exists, it's less mature)
- Lots of internal enterprise libraries

Every blocking dependency becomes a problem you have to work around with thread pool offloading:

```java
// Wrapping a blocking library call
return Uni.createFrom()
    .

item(() ->legacyBlockingClient.

call())  // has to run on worker pool
    .

runSubscriptionOn(Infrastructure.getDefaultWorkerPool()); // boilerplate
```

---

### 6. Learning Curve and Team Productivity

Reactive requires understanding concepts that aren't obvious:

- Backpressure
- Hot vs cold streams
- `flatMap` vs `concatMap` vs `switchMap` (different concurrency semantics)
- Schedulers and which thread you're on
- Subscription lifecycle and memory leaks from un-cancelled subscriptions

Junior developers and teams new to reactive **slow down significantly**. Bugs are subtler and harder
to reason about.

---

### 7. Testing is Harder

```java
// Blocking test — simple
@Test
void testOrderService() {
  Order order = orderService.findById(1L); // just works
  assertEquals("CONFIRMED", order.getStatus());
}

// Reactive test — more ceremony
@Test
void testOrderService() {
  orderService.findById(1L)
      .subscribe().withSubscriber(UniAssertSubscriber.create())
      .awaitItem()
      .assertItem(order -> assertEquals("CONFIRMED", order.getStatus()));
}
```

---

### When to Actually Use It

| Use Reactive                                          | Use Blocking (or Virtual Threads)  |
|-------------------------------------------------------|------------------------------------|
| Very high concurrency (10k+ simultaneous connections) | Typical enterprise CRUD apps       |
| WebSockets / SSE / streaming                          | Complex business logic             |
| API gateway / proxy patterns                          | Teams without reactive experience  |
| Known thread exhaustion problems                      | Heavy use of JDBC/legacy libraries |
| Fine-grained backpressure needs                       | Rapid prototyping                  |

---

### TL;DR

> Reactive is a **trade** — you give up code clarity, debuggability, ecosystem compatibility, and
> team velocity in
> exchange for better throughput under extreme concurrency. For most business applications, **Java
21 virtual threads
give
you 90% of the benefit for free**, with none of the complexity. Reactive makes sense at the edges:
> streaming, massive
> WebSocket scale, or API gateways — not in every service in your org.
