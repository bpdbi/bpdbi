# Plan: Replace ColumnMapper with a Binary-First Type Extension System

## Problem Statement

The current type extension system has three issues:

1. **ColumnMapper forces a binary→text→parse roundtrip.** When `row.get(index, MyType.class)`
   hits a type the `BinaryCodec` doesn't handle, it calls `decodeToString()` (binary→text) and
   then the `ColumnMapper` parses that text back into the target type. This is wasteful when the
   binary codec already knows how to decode the underlying type (e.g., `BigDecimal` for `Money`,
   `UUID` for `UserId`).

2. **Encoders and decoders are scattered across two separate registries.** A user adding support
   for `Money` must register a `ParamEncoder<Money>` in `BinderRegistry` and a
   `ColumnMapper<Money>` in `ColumnMapperRegistry` — two separate calls on two separate objects,
   with different APIs and different abstractions (one converts to a Java type, the other parses
   from text). The Kotlin extensions in `Binders.kt` show this pain clearly: each type requires
   3 registrations (ParamEncoder + Binder + ColumnMapper) across 2 registries.

3. **`ColumnMapperRegistry.defaults()` duplicates work that `BinaryCodec` already does.** The
   default mappers for `Integer`, `Long`, `UUID`, `LocalDate`, etc. exist only because
   `ColumnMapper` can be reached for any type. In practice, `BinaryCodec.canDecode()` returns
   true for all of these, so the default `ColumnMapper` registrations are dead code for binary
   connections.

## Current Architecture

### Param encoding (input): Java object → wire bytes

```
User value (e.g., Money)
  │
  ├─ ParamEncoder registered? → encode() → standard Java type (e.g., Long)
  │                                │
  │                                ▼
  ├─ BinaryParamEncoder.writeParam() succeeds? → binary bytes (format=1) ✓
  │
  └─ Fallback: Binder.bind() → text string → UTF-8 bytes (format=0)
```

- `ParamEncoder<T>`: converts domain type → binary-encodable Java type (clean, no wire details)
- `Binder<T>`: converts to SQL text string (fallback)
- Both live in `BinderRegistry`

### Result decoding (output): wire bytes → Java object

```
row.get(index, MyType.class)
  │
  ├─ JSON type? → getString() → jsonMapper.fromJson()
  │
  ├─ binaryCodec.canDecode(MyType)? → decode(bytes) → MyType ✓
  │
  └─ Fallback: decodeToString(bytes, typeOID) → text → ColumnMapper.map(text) → MyType
                    ▲ wasteful roundtrip ▲
```

- `ColumnMapper<T>`: parses text → target type
- Lives in `ColumnMapperRegistry` (separate from `BinderRegistry`)

### The asymmetry

| Direction | Binary path extension | Text fallback |
|-----------|----------------------|---------------|
| **Input** (params) | `ParamEncoder<T>`: domain → Java type | `Binder<T>`: domain → text |
| **Output** (results) | *(nothing)* | `ColumnMapper<T>`: text → domain |

There's no output-side equivalent of `ParamEncoder` — a way to say "decode the binary bytes as
`BigDecimal`, then wrap in `Money`".

## What ColumnMapper Is Actually Used For

Looking at the codebase and the Kotlin extensions, `ColumnMapper` serves these use cases:

1. **Wrapper types** — `Money` wrapping `BigDecimal`, `UserId` wrapping `UUID`
   - Could be: `binary → BigDecimal → new Money(bd)` (no text roundtrip)

2. **Foreign type systems** — `kotlinx.datetime.LocalDate`, `kotlin.uuid.Uuid`
   - Could be: `binary → java.time.LocalDate → toKotlinLocalDate()` (no text roundtrip)

3. **Enums** — column text → `Enum.valueOf()`
   - Binary codec already decodes to `String`; enum lookup from string is fine
   - Could be: `binary → String → Enum.valueOf()` (same cost, already in binary path)

4. **Default type parsing** — `String → Integer`, `String → UUID`, etc.
   - Dead code: `BinaryCodec.canDecode()` handles all these already

All of these would be better served by mapping **from a standard Java type** rather than from text.

## Do We Still Need Text Binders?

No. `TypeRegistry.register(MyType.class, String.class, encoder, decoder)` covers every case that
text `Binder<T>` + `ColumnMapper<T>` covered:

| Case | Old path | New path |
|---|---|---|
| Standard types (Integer, UUID, etc.) | Binary directly | Binary directly (no registration needed) |
| Domain wrappers (Money, UserId) | ParamEncoder + Binder + ColumnMapper | Single `register()` call |
| Foreign types (kotlinx.datetime) | ParamEncoder + Binder + ColumnMapper | Single `register()` call |
| Text-only Postgres types (tsvector) | Binder (toString) + ColumnMapper (parse) | `register(TsVector.class, String.class, ...)` |
| Unregistered type as param | Silent `toString()` fallback (footgun) | Loud error (improvement) |
| String params | Binder identity function | BinaryParamEncoder handles String as UTF-8 |

The `Binder<T>` interface, `BinderRegistry`, and `ColumnMapper`/`ColumnMapperRegistry` can all
be removed. The implicit `toString()` fallback for unregistered types was a footgun — better to
fail loudly.

---

## Chosen Design: `TypeRegistry.register()` with Nullable Halves

A single `register()` method on `TypeRegistry` that accepts all parameters directly. The encoder
and decoder functions are `@Nullable` — pass `null` for the direction you don't need. Using a
null half at runtime throws a clear error.

### API

```java
public final class TypeRegistry {

    /**
     * Register a custom type mapping. The encoder converts domain → standard type (for params),
     * the decoder converts standard → domain type (for results). Pass null for either function
     * to indicate that direction is not supported — using the null direction at runtime throws.
     *
     * @param domainType    the custom type (e.g., Money.class)
     * @param standardType  the binary-codec-supported type it maps to (e.g., BigDecimal.class)
     * @param encoder       domain → standard (for param binding), or null if encode-only
     * @param decoder       standard → domain (for result decoding), or null if decode-only
     */
    public <T, S> TypeRegistry register(
            @NonNull Class<T> domainType,
            @NonNull Class<S> standardType,
            @Nullable Function<T, S> encoder,
            @Nullable Function<S, T> decoder) { ... }

    /** Register a type as JSON (serialized/deserialized via JsonMapper). */
    public TypeRegistry registerAsJson(@NonNull Class<?> type) { ... }

    /** JSON type set (used by Row for JSON detection). */
    public Set<Class<?>> jsonTypes() { ... }
}
```

### Registration Examples

```java
// Full codec — both directions
conn.typeRegistry().register(Money.class, BigDecimal.class,
    m -> m.amount(),
    bd -> new Money(bd));

conn.typeRegistry().register(UserId.class, UUID.class,
    UserId::uuid,
    UserId::new);

// Encode-only — can bind as param, but can't read from result row
conn.typeRegistry().register(WriteAuditEvent.class, String.class,
    e -> e.toJson(),
    null);

// Decode-only — can read from result row, but can't bind as param
conn.typeRegistry().register(PgInterval.class, String.class,
    null,
    PgInterval::parse);
```

### Kotlin Extensions

```kotlin
fun TypeRegistry.registerKotlinTypes() = apply {
    register(kotlin.uuid.Uuid::class.java, UUID::class.java,
        { it.toJavaUuid() },
        { kotlin.uuid.Uuid.fromJavaUuid(it) })
    register(kotlinx.datetime.LocalDate::class.java, java.time.LocalDate::class.java,
        { it.toJavaLocalDate() },
        { it.toKotlinLocalDate() })
    register(kotlinx.datetime.LocalDateTime::class.java, java.time.LocalDateTime::class.java,
        { it.toJavaLocalDateTime() },
        { it.toKotlinLocalDateTime() })
    register(kotlinx.datetime.LocalTime::class.java, java.time.LocalTime::class.java,
        { it.toJavaLocalTime() },
        { it.toKotlinLocalTime() })
    register(kotlin.time.Instant::class.java, java.time.Instant::class.java,
        { it.toJavaInstant() },
        { Instant.fromEpochSeconds(it.epochSecond, it.nano) })

    // UInt/ULong: encode-only (read back as Long/Int from result rows)
    register(UInt::class.java, Long::class.java, { it.toLong() }, null)
    register(ULong::class.java, Long::class.java, { it.toLong() }, null)
}

fun Connection.useKotlinTypes() {
    typeRegistry().registerKotlinTypes()
}
```

Compared to current `Binders.kt` which needs 3 registrations per type across 2 registries,
this is 1 registration per type in 1 registry.

### Error Messages

When using a null half at runtime:

```
// Trying to decode a type registered with encoder-only
row.get("event", WriteAuditEvent.class)
→ IllegalStateException: "No result decoder for WriteAuditEvent.
   It is registered with encode-only support (WriteAuditEvent → String).
   Add a decoder via typeRegistry().register(WriteAuditEvent.class, String.class, encoder, decoder)."

// Trying to bind a completely unregistered type
conn.query("SELECT $1", new UnknownType())
→ IllegalStateException: "Cannot bind parameter of type UnknownType.
   Register it via typeRegistry().register(UnknownType.class, ...)."
```

---

## New Pipelines

### Param encoding (input): Java object → wire bytes

```
User value (e.g., Money)
  │
  ├─ TypeRegistry has encoder? → encode() → standard Java type (e.g., BigDecimal)
  │                                 │
  │                                 ▼
  ├─ BinaryParamEncoder.writeParam() succeeds? → binary bytes (format=1) ✓
  │
  └─ No encoder, not binary-encodable? → error
```

No more silent `toString()` fallback. Either the type is natively binary-encodable, has a
registered encoder, or it's an error.

### Result decoding (output): wire bytes → Java object

```
row.get(index, MyType.class)
  │
  ├─ JSON type? → getString() → jsonMapper.fromJson()
  │
  ├─ binaryCodec.canDecode(MyType)? → decode(bytes) → MyType
  │
  ├─ TypeRegistry has decoder? → decode bytes as standardType → decoder.apply() → MyType
  │
  ├─ Enum? → decodeString(bytes) → Enum.valueOf()
  │
  └─ None of the above? → error
```

No more `decodeToString()` roundtrip. The binary codec decodes to the standard Java type
directly, then the user's decoder function wraps it.

---

## What Happens to Existing APIs

| Current API | Fate |
|---|---|
| `ColumnMapper<T>` | **Removed** — replaced by decoder function in `register()` |
| `ColumnMapperRegistry` | **Removed** — merged into `TypeRegistry` |
| `ColumnMapperRegistry.defaults()` | **Removed** — dead code, `BinaryCodec` handles all default types |
| `ParamEncoder<T>` | **Removed** — replaced by encoder function in `register()` |
| `Binder<T>` | **Removed** — `register(T.class, String.class, encoder, null)` covers text encoding |
| `BinderRegistry` | **Replaced** by `TypeRegistry` |
| `BinderRegistry.defaults()` | **Removed** — dead code for binary connections |
| `Connection.setMapperRegistry()` | **Removed** |
| `Connection.setBinderRegistry()` | **Replaced** by `setTypeRegistry()` |
| `Connection.mapperRegistry()` | **Removed** |
| `Connection.binderRegistry()` | **Replaced** by `typeRegistry()` |
| `BinaryCodec.decodeToString()` | **Kept** — still used by `Row.getString()` on non-text columns |
| Kotlin `Binders.kt` | **Simplified** — single `registerKotlinTypes()` on `TypeRegistry` |
| `RowExtractors` (SPI) | **Unchanged** — uses typed Row getters, unaffected |
| `Row.get(int, Class)` | **Updated** — new fallback path through TypeRegistry |
| `QualifiedType` / qualified binders | **TBD** — evaluate if still needed |

### Enum handling

Enums get built-in handling in `Row.get()` without needing a registry entry:

```java
if (type.isEnum()) {
    String text = binaryCodec.decodeString(vBuf, vOff, vLen);
    return (T) Enum.valueOf((Class<? extends Enum>) type, text);
}
```

This stays in the binary path (`decodeString` is a direct binary→String decode, not the
OID-based `decodeToString` roundtrip).

### JSON handling

Unchanged. JSON types are detected first in `Row.get()` and go through `JsonMapper`. The
`registerAsJson()` method moves to `TypeRegistry`.

---

## Impact on `BinaryCodec.decodeToString()`

`decodeToString(byte[], int, int, int)` is currently called in two places:

1. **`Row.get()` fallback** (line 461) — to feed `ColumnMapper`. **This call goes away.**
2. **`Row.getString()`** (line 225) — for `getString()` on non-text columns (e.g., calling
   `getString()` on an integer column). **This stays** — it's a legitimate use case.

So `decodeToString()` doesn't go away entirely, but it's no longer on the type-extension
critical path.

---

## Implementation Steps

Since this is pre-1.0, we remove the old APIs directly (no deprecation cycle).

1. **Create `TypeRegistry`** — single class with `register()`, `registerAsJson()`, internal
   lookup methods for encode/decode
2. **Update `Connection` interface** — replace `binderRegistry()`/`setBinderRegistry()` and
   `mapperRegistry()`/`setMapperRegistry()` with `typeRegistry()`/`setTypeRegistry()`
3. **Update `BaseConnection`** — replace both registry fields with single `TypeRegistry`
4. **Update `Row.get()`** — replace ColumnMapper fallback with TypeRegistry decode path
5. **Update param encoding path** — replace `BinderRegistry.encode()` with
   `TypeRegistry.encode()`, remove text `Binder` fallback
6. **Update `PgConnection`** — adjust `applyEncoders()` and `encodeParamToText()` to use
   `TypeRegistry`
7. **Convert Kotlin extensions** — rewrite `Binders.kt` to use `TypeRegistry.register()`
8. **Delete old types** — `ColumnMapper`, `ColumnMapperRegistry`, `ParamEncoder`, `Binder`,
   `BinderRegistry`
9. **Update tests** — all tests that register custom types
10. **Update examples** — any examples showing custom type registration
