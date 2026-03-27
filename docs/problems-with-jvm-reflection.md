Problems with the JVM Reflection API
====================================

There are good reasons for not wanting to use reflection on the JVM.
In this text we present an overview of them.

Bpdbi can be used without reflection.
Reflection is only used in optional add-on libraries, such as `bpdbi-javabean-mapper` and `bpdbi-record-mapper`.
The `bpdbi-kotlin` add-on contains a row mapper based on `kotlinx.serialization` which does not use the reflection API,
but a Kotlin compiler plugin.


## 1. Performance Overhead

Reflective calls are slower than direct calls due to three compounding factors:

- **No JIT inlining** — The JIT cannot inline a call whose target is unknown at compile time, forfeiting one of the JVM's most impactful optimisations.
- **Primitive boxing** — All arguments must be boxed into `Object[]` on every `invoke()`, causing heap allocations at call frequency.
- **Access checks** — Unless `setAccessible(true)` is called, each invocation re-runs access control checks.

Typical overhead compared to a direct call:

| Operation                                 | Overhead  |
|-------------------------------------------|-----------|
| `Method.invoke()`, uncached               | 10–100x   |
| `Method.invoke()`, cached `Method` object | 2–5x      |
| `Field` get/set                           | 3–10x     |
| `Constructor.newInstance()`               | 3–7x      |

After roughly 15 invocations the JVM inflates the reflective stub to native code, narrowing but not eliminating the gap.


## 2. Encapsulation Violations

`setAccessible(true)` bypasses `private`, `protected`, and package-private access modifiers entirely. Consequences:

- Implementation details intended to be hidden become reachable and mutable from any code.
- `Field.set()` can mutate `final` fields after construction, producing undefined behavior and confusing the JIT.
- In multi-tenant environments (OSGi, application servers) one module can freely introspect or corrupt the internals of another.


## 3. Module System Friction (Java 9+)

The JPMS module system was designed primarily to close off unrestricted reflective access. Its effects:

- Reflective access to a non-`opens` package in another module throws `InaccessibleObjectException` at runtime.
- Frameworks that relied on deep reflection (Spring, Hibernate, Jackson, Guice) broke at Java 9 and required years of remediation.
- The only workarounds: `--add-opens` and `--add-exports` JVM flags—are brittle and contradict the purpose of the module system.


## 4. Loss of Static Type Safety

All reflective lookups are string-based:

```java
getDeclaredMethod("processOrder", String.class, int.class);
```

- **Silent breakage on refactor.** Renaming a method compiles cleanly but throws `NoSuchMethodException` at runtime.
- **No generics.** `Method.invoke()` returns `Object`; casts are unchecked and fail at runtime.
- IDEs cannot include reflective references in find-usages, rename refactoring, or dead-code analysis.


## 5. Application Startup Cost

At startup, the bottleneck is not `invoke()` speed but the volume of reflective scanning performed before the application is ready.

Frameworks such as Spring and Hibernate scan every class on the classpath for annotations (`@Component`, `@Entity`, `@Inject`, etc.), invoking `getDeclaredMethods()`, `getDeclaredFields()`, and `getDeclaredAnnotations()` on each one. A typical Spring Boot application scans 5,000–20,000+ classes, contributing 500ms–3s of startup time on a cold JVM—before any application logic runs.

This compounds with dynamic proxy and bytecode generation (CGLIB, ByteBuddy) performed immediately after scanning.

Approximate startup comparison:

| Application          | Reflection-heavy (Spring Boot)  | Compile-time DI (Quarkus native)  |
|----------------------|---------------------------------|-----------------------------------|
| Small REST service   | 2–4 s                           | 0.05–0.1 s                        |
| Medium application   | 5–12 s                          | 0.1–0.3 s                         |
| Large enterprise app | 20–60 s                         | 1–5 s                             |

This cost is especially consequential in serverless and container environments where instances are frequently cold-started.


## 6. AOT / Native Image Incompatibility

GraalVM Native Image performs closed-world static analysis at build time.
Any class, method, or field reached only via reflection is invisible to this analysis and will be removed
from the binary unless explicitly registered in a `reflect-config.json` metadata file.

- Large applications may require hundreds of manual registration entries.
- The GraalVM tracing agent can automate some of this, but adds complexity to the build pipeline.
- Android's ART compiler has progressively imposed the same restrictions.

This is the principal technical driver behind the industry shift toward build-time annotation processing
(Micronaut, Quarkus) and Spring AOT (Spring 6+).


## 7. Debuggability

- `Method.invoke()` wraps exceptions thrown by the target in `InvocationTargetException`, requiring explicit unwrapping and obscuring the root cause in logs and error reports.
- Reflective calls insert synthetic frames (`jdk.internal.reflect.*`) into stack traces, making them harder to read.
- Profilers and debuggers have limited ability to instrument dynamically resolved call sites.


## 8. Incomplete Introspection of Modern Java

- Lambdas and method references are implemented as hidden classes (since Java 15) and are not enumerable via `getDeclaredMethods()` or `getDeclaredClasses()`.
- Synthetic fields (inner class `this$0` references, captured lambda variables) appear in `getDeclaredFields()` results, leading to unintentional coupling to compiler implementation details.
- `invokedynamic` call sites (used for lambdas and string concatenation) are entirely opaque to the reflection API.


## 9. Class Initialisation Side Effects

`Class.forName(name)` triggers the class initializer (`<clinit>`) as a side effect.
If the initializer opens a database connection, populates a cache, or registers a global handler,
calling `forName` for inspection purposes has observable and potentially harmful consequences.
The three-argument form `Class.forName(name, false, loader)` suppresses initialization but is infrequently used in practice.


## 10. API Verbosity

Every reflective operation requires catching 3–5 checked exceptions (`ClassNotFoundException`, `NoSuchMethodException`, `IllegalAccessException`, `InvocationTargetException`, `InstantiationException`).
This forces boilerplate error-handling into every call site, reducing readability and increasing the surface for silently swallowed exceptions.
