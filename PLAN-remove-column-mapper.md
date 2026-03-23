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

---

## Proposed Solutions

### Option A: `ResultDecoder<T>` — Mirror of `ParamEncoder`

Add a single new interface that mirrors `ParamEncoder` on the output side:

```java
@FunctionalInterface
public interface ResultDecoder<T> {
    @NonNull T decode(@NonNull Object value);

    // The type to ask BinaryCodec to decode first
    @NonNull Class<?> intermediateType();
}
```

**Row.get() pipeline becomes:**

```
row.get(index, MyType.class)
  ├─ JSON type? → jsonMapper
  ├─ binaryCodec.canDecode(MyType)? → direct binary decode
  ├─ ResultDecoder registered? → decode as intermediateType → ResultDecoder.decode()
  └─ Enum? → decode as String → Enum.valueOf()
```

**Registration example:**

```java
// ParamEncoder + ResultDecoder in one registry
registry.registerEncoder(Money.class, m -> m.cents());           // Money → Long
registry.registerDecoder(Money.class, Long.class, v -> new Money((Long) v));  // Long → Money

// Kotlin
registerEncoder(kotlin.uuid.Uuid::class.java) { it.toJavaUuid() }
registerDecoder(kotlin.uuid.Uuid::class.java, UUID::class.java) { Uuid.fromJavaUuid(it as UUID) }
```

**Pros:**
- Clean mirror of `ParamEncoder` — easy to understand
- Stays in binary path (no text roundtrip)
- Minimal API surface

**Cons:**
- `Object` return from intermediate decode requires cast
- Two separate registrations still needed (encoder + decoder), just in the same registry
- `intermediateType()` is a second method on a `@FunctionalInterface` (needs a factory or default)

---

### Option B: `TypeCodec<T, S>` — Paired Encoder/Decoder

A single interface that bundles encoding and decoding for a custom type:

```java
public interface TypeCodec<T, S> {
    /** The domain type this codec handles (e.g., Money.class) */
    @NonNull Class<T> type();

    /** The standard type used on the wire (e.g., Long.class, UUID.class, String.class) */
    @NonNull Class<S> standardType();

    /** Encode: domain → standard (for params) */
    @NonNull S encode(@NonNull T value);

    /** Decode: standard → domain (for results) */
    @NonNull T decode(@NonNull S value);
}
```

**Registration example:**

```java
registry.register(TypeCodec.of(
    Money.class, BigDecimal.class,
    m -> m.amount(),             // encode
    bd -> new Money(bd)          // decode
));

// Kotlin
registry.register(TypeCodec.of(
    kotlin.uuid.Uuid::class.java, UUID::class.java,
    { it.toJavaUuid() },
    { Uuid.fromJavaUuid(it) }
))
```

**Row.get() pipeline becomes:**

```
row.get(index, MyType.class)
  ├─ JSON type? → jsonMapper
  ├─ binaryCodec.canDecode(MyType)? → direct binary decode
  ├─ TypeCodec registered? → decode as standardType → codec.decode()
  └─ Enum? → decode as String → Enum.valueOf()
```

**Param encoding pipeline becomes:**

```
User value (e.g., Money)
  ├─ TypeCodec registered? → codec.encode() → standard type → binary encode
  └─ Fallback: Binder.bind() → text string
```

**Pros:**
- Encoder and decoder are always paired — impossible to forget one
- Single registration per type
- Type-safe: no `Object` casting, `S` is known at compile time
- `standardType()` replaces the separate `intermediateType()` concept
- Natural home for Kotlin type wrappers (`Uuid`, `kotlinx.datetime.*`)

**Cons:**
- Some types only need one direction (rare but possible)
- Slightly more ceremony for simple cases compared to a lambda

---

### Option C: `TypeCodec<T, S>` with Separate Halves

Like Option B, but allow registering just one half:

```java
// Full codec (both directions)
registry.register(TypeCodec.of(Money.class, BigDecimal.class, encode, decode));

// Encode only (param binding, no result decoding)
registry.registerEncoder(WriteOnly.class, String.class, w -> w.text());

// Decode only (result reading, no param encoding)
registry.registerDecoder(ReadOnly.class, String.class, s -> new ReadOnly(s));
```

This is essentially Option B with `registerEncoder`/`registerDecoder` as convenience shortcuts
that create a half-codec internally.

**Pros:**
- Flexibility of Option A with the paired-by-default design of Option B
- Handles edge cases (encode-only, decode-only)

**Cons:**
- Larger API surface
- Two ways to do the same thing

---

## What Happens to Existing APIs

### In all options:

| Current API | Fate |
|---|---|
| `ColumnMapper<T>` | **Removed** — replaced by decode-half of new system |
| `ColumnMapperRegistry` | **Removed** — merged into `BinderRegistry` (or renamed `TypeRegistry`) |
| `ColumnMapperRegistry.defaults()` | **Removed** — dead code, `BinaryCodec` handles all default types |
| `ParamEncoder<T>` | **Absorbed** into new system (or kept as alias in Option A) |
| `Binder<T>` (text) | **Kept** — still needed for text fallback on non-binary-capable types (primarily `String`) |
| `BinderRegistry` | **Renamed** to `TypeRegistry` — holds codecs, binders, JSON types |
| `Connection.setMapperRegistry()` | **Removed** — single `TypeRegistry` replaces both |
| `Connection.setBinderRegistry()` | **Renamed** to `setTypeRegistry()` or similar |
| `BinaryCodec.decodeToString()` | **Removed from result path** — only used for `getString()` on non-text columns |
| Kotlin `registerKotlinTypes()` | **Simplified** — one extension function, one registration per type |
| `RowExtractors` (SPI) | **Unchanged** — uses typed Row getters, unaffected |
| `Row.get(int, Class)` | **Updated** — new fallback path through codec instead of text mapper |

### Enum handling

Enums currently have special handling in `ColumnMapperRegistry.map()`. In the new system,
the `Row.get()` fallback for enums would be:

```java
if (type.isEnum()) {
    String text = binaryCodec.decodeString(vBuf, vOff, vLen);
    return (T) Enum.valueOf((Class<? extends Enum>) type, text);
}
```

This stays in the binary path (`decodeString` is a direct binary→String decode, not the OID-based
`decodeToString` roundtrip).

### JSON handling

Unchanged. JSON types are detected first in `Row.get()` and go through `JsonMapper`. The
`registerAsJson()` method moves from `BinderRegistry` to the new `TypeRegistry`.

---

## Impact on `BinaryCodec.decodeToString()`

`decodeToString(byte[], int, int, int)` is currently called in two places:

1. **`Row.get()` fallback** (line 461) — to feed `ColumnMapper`. **This call goes away.**
2. **`Row.getString()`** (line 225) — for `getString()` on non-text columns (e.g., calling
   `getString()` on an integer column). **This stays** — it's a legitimate use case.

So `decodeToString()` doesn't go away entirely, but it's no longer on the type-extension critical
path.

---

## Registry Naming

If we merge `BinderRegistry` + `ColumnMapperRegistry` into one, possible names:

- `TypeRegistry` — simple, clear
- `TypeCodecRegistry` — explicit about what it holds
- `CustomTypeRegistry` — emphasizes it's for user-defined types

The connection API would become:

```java
@NonNull TypeRegistry typeRegistry();
void setTypeRegistry(@NonNull TypeRegistry registry);
```

---

## Migration Path

1. Add `TypeCodec` interface and registration to the registry
2. Update `Row.get()` to check codec before falling through to text
3. Convert Kotlin type registrations to use `TypeCodec`
4. Deprecate `ColumnMapper`, `ColumnMapperRegistry`, `ParamEncoder` (one release)
5. Remove deprecated types

Or, since this is pre-1.0, just remove them directly.

---

## Open Questions

1. **Should `Binder<T>` (text encoding) also be absorbed into `TypeCodec`?** Currently `Binder`
   handles the text-fallback path for param encoding. If we keep `TypeCodec.encode()` returning
   a standard Java type that's binary-encodable, and the binary encoder handles all standard
   types, is there still a need for text binders at all? The answer is yes for `String` params
   and types where no standard-type mapping exists, but these are rare.

2. **What about types that map to different standard types for encoding vs decoding?** For
   example, could a type encode as `Long` but decode from `BigDecimal`? This seems unlikely but
   Option C handles it. Option B does not.

3. **Should the default `TypeRegistry` be empty?** Since `BinaryCodec.canDecode()` handles all
   standard types, the default registry could be empty (no default codecs needed). Only
   user-registered custom types would appear. This is cleaner than `ColumnMapperRegistry.defaults()`
   which was full of dead entries.

4. **Naming: `TypeCodec` vs `TypeAdapter` vs `TypeMapping`?** `TypeCodec` is consistent with
   Postgres driver terminology. `TypeAdapter` is Gson-style. `TypeMapping` is neutral.

---

## Recommendation

**Option B (`TypeCodec<T, S>`)** is the strongest choice:

- Single registration per type eliminates the "forgot the decoder" class of bugs
- Type-safe intermediate type (no `Object` casts)
- Clean symmetry: `encode: T → S`, `decode: S → T`
- Natural pairing makes it obvious what standard type each custom type maps to
- Smallest API surface while covering all real use cases
- If encode-only or decode-only is truly needed, a `TypeCodec` with a throwing unused half
  is acceptable (or add convenience methods per Option C later)
