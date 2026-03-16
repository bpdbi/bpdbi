# Plan: Binary Wire Format (aligning with Vert.x)

## Current state

Both djb drivers use **text format exclusively**:
- PG: Extended query sends text params, requests text results
- MySQL: Uses COM_QUERY with text interpolation — no binary protocol at all

Vert.x uses **binary format** for extended/prepared queries in both drivers.

## Key design decision: lazy decoding

Row keeps `byte[][]` — raw bytes from the wire, whether text or binary.
Decoding happens lazily on access (e.g., `getInteger()`), not eagerly when
building the row. This avoids decoding columns the user never reads.

Row needs two extra pieces of metadata per column:
- **format** (text=0, binary=1) — from the RowDescription or Bind response
- **typeOID** — already in ColumnDescriptor

```java
public final class Row {
    private final ColumnDescriptor[] columns;
    private final byte[][] values;    // raw bytes (text OR binary), null = SQL NULL
    private final int[] formats;      // 0=text, 1=binary, per column

    public Integer getInteger(int index) {
        if (values[index] == null) return null;
        if (formats[index] == 1) {
            // Binary: 4 bytes big-endian (PG) or little-endian (MySQL)
            return PgBinaryCodec.decodeInt4(values[index]);
        } else {
            // Text: parse ASCII string
            return Integer.parseInt(new String(values[index], UTF_8));
        }
    }
}
```

Advantages:
- Only decode columns that are accessed
- No Object[] allocation for entire row upfront
- Text and binary coexist — simple queries return text, extended return binary
- RowMapper / kotlinx.serialization decoder can read raw bytes directly

---

## ColumnDescriptor changes (djb-core)

Add `format` field:

```java
public record ColumnDescriptor(
    String name,
    int tableOID,
    short columnAttributeNumber,
    int typeOID,
    short typeSize,
    int typeModifier,
    int format          // 0=text, 1=binary
) {}
```

Row constructor gains `int[] formats` (or derives from ColumnDescriptor[]).

## Row changes (djb-core)

Row getters dispatch on format. For text format, behavior is identical to
today. For binary format, a codec interface is called:

```java
public interface BinaryCodec {
    Integer decodeInt4(byte[] value);
    Long decodeInt8(byte[] value);
    Float decodeFloat4(byte[] value);
    Double decodeFloat8(byte[] value);
    Boolean decodeBool(byte[] value);
    String decodeText(byte[] value);
    LocalDate decodeDate(byte[] value);
    LocalTime decodeTime(byte[] value);
    LocalDateTime decodeTimestamp(byte[] value);
    OffsetDateTime decodeTimestamptz(byte[] value);
    UUID decodeUuid(byte[] value);
    // ... etc
}
```

Each driver provides its own `BinaryCodec` implementation (PG is big-endian,
MySQL is little-endian). Row holds a reference to the codec.

This keeps djb-core database-agnostic — it doesn't know PG vs MySQL byte order.

---

## PG driver changes

### Step 1 — PgDataType enum

Port from Vert.x. Maps OID → metadata (supportsBinary flag, Java type):

```java
public enum PgDataType {
    BOOL(16, true),
    INT2(21, true),
    INT4(23, true),
    INT8(20, true),
    FLOAT4(700, true),
    FLOAT8(701, true),
    NUMERIC(1700, false),  // stays text (binary BCD is complex)
    TEXT(25, true),
    VARCHAR(1043, true),
    // ... all types from Vert.x DataType enum
    UNKNOWN(705, false);

    public final int oid;
    public final boolean supportsBinary;

    public static PgDataType fromOid(int oid) { ... }
}
```

### Step 2 — PgBinaryCodec

Implements `BinaryCodec` for PostgreSQL (big-endian):

```java
// INT4: 4 bytes big-endian → int
static int decodeInt4(byte[] b) {
    return (b[0] & 0xFF) << 24 | (b[1] & 0xFF) << 16 | (b[2] & 0xFF) << 8 | (b[3] & 0xFF);
}

// FLOAT8: 8 bytes IEEE 754 big-endian → double
static double decodeFloat8(byte[] b) {
    return Double.longBitsToDouble(decodeInt8(b));
}

// BOOL: 1 byte → boolean
static boolean decodeBool(byte[] b) { return b[0] != 0; }

// DATE: 4 bytes (days since 2000-01-01) → LocalDate
static LocalDate decodeDate(byte[] b) {
    int days = decodeInt4(b);
    return LocalDate.of(2000, 1, 1).plusDays(days);
}

// TIMESTAMP: 8 bytes (microseconds since 2000-01-01 00:00) → LocalDateTime
static LocalDateTime decodeTimestamp(byte[] b) {
    long micros = decodeInt8(b);
    return LocalDateTime.of(2000, 1, 1, 0, 0).plus(micros, ChronoUnit.MICROS);
}

// UUID: 16 bytes → UUID
static UUID decodeUuid(byte[] b) {
    long msb = decodeInt8(b, 0);
    long lsb = decodeInt8(b, 8);
    return new UUID(msb, lsb);
}

// TEXT/VARCHAR: raw UTF-8 bytes → String (same as text format for strings)
static String decodeString(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
}

// JSONB: skip 1-byte version prefix, then UTF-8 string
static String decodeJsonb(byte[] b) {
    return new String(b, 1, b.length - 1, StandardCharsets.UTF_8);
}
```

Also binary **encode** methods for parameters:

```java
static byte[] encodeInt4(int value) { /* 4 bytes big-endian */ }
static byte[] encodeInt8(long value) { /* 8 bytes big-endian */ }
static byte[] encodeFloat8(double value) { /* 8 bytes IEEE 754 */ }
static byte[] encodeBool(boolean value) { return new byte[]{(byte)(value ? 1 : 0)}; }
static byte[] encodeDate(LocalDate d) { /* days since 2000-01-01 as int4 */ }
static byte[] encodeTimestamp(LocalDateTime ts) { /* micros since 2000-01-01 as int8 */ }
static byte[] encodeUuid(UUID u) { /* 16 bytes: msb + lsb */ }
// etc.
```

### Step 3 — PgEncoder: request binary format

Change `writeBind()`:
- Parameter format codes: binary (1) for types that support it, text (0) for others
- Result format codes: binary (1) for types that support it
- Encode parameter values using `PgBinaryCodec.encodeXxx()` for binary types

Requires knowing parameter types. For unnamed prepared statements, we get
`ParameterDescription` from the server after `Parse`. For named prepared
statements, we already have it from `prepare()`.

**Simplified approach for MVP**: request all results as binary (format code 1
for all columns), and keep parameters as text (format code 0). This gets the
biggest win (result decoding) without needing parameter type inference.

### Step 4 — PgDecoder: pass format to Row

The `RowDescription` message already contains the format code per column.
Pass it through to the Row constructor.

`DataRow` decoding stays the same — it stores raw `byte[][]`. The format
metadata tells Row how to decode on access.

### Step 5 — Simple query stays text

PG simple query protocol has no format negotiation — results are always text.
Row's format array will be all zeros (text). Existing text decode paths work
as before.

---

## MySQL driver changes

### Step 6 — COM_STMT_PREPARE + COM_STMT_EXECUTE

Replace COM_QUERY text interpolation with real prepared statement protocol:

1. **COM_STMT_PREPARE** (0x16): Send SQL, receive:
   - Statement ID (4 bytes LE)
   - Number of params and columns
   - Param definitions, column definitions

2. **COM_STMT_EXECUTE** (0x17): Send:
   - Statement ID
   - Null bitmap for params
   - Type info + binary-encoded parameter values

3. **Binary result rows**: Header byte (0x00) + null bitmap + binary column values

### Step 7 — MysqlBinaryCodec

MySQL binary format uses **little-endian** (opposite of PG):

```java
static int decodeInt4(byte[] b) {
    return (b[0] & 0xFF) | (b[1] & 0xFF) << 8 | (b[2] & 0xFF) << 16 | (b[3] & 0xFF) << 24;
}

// DATE: length-encoded struct
static LocalDate decodeDate(byte[] b) {
    int len = b[0] & 0xFF;
    if (len == 0) return null;
    int year = (b[1] & 0xFF) | (b[2] & 0xFF) << 8;
    int month = b[3] & 0xFF;
    int day = b[4] & 0xFF;
    return LocalDate.of(year, month, day);
}

// DATETIME: length-encoded struct (4/7/11 bytes)
static LocalDateTime decodeDatetime(byte[] b) {
    // similar length-encoded struct with hour/min/sec/micros
}
```

### Step 8 — Remove text interpolation

Delete `MysqlEncoder.interpolateParams()` and `escapeString()`.
Parameterized queries now go through COM_STMT_PREPARE + COM_STMT_EXECUTE.
The SQL injection risk from text interpolation is eliminated.

---

## Implementation order

```
Phase 1 — Foundation (djb-core)
  1a. Add format field to ColumnDescriptor
  1b. Add formats array + BinaryCodec interface to Row
  1c. Row getters dispatch on format (text path unchanged)

Phase 2 — PG binary results
  2a. PgDataType enum (OID → metadata)
  2b. PgBinaryCodec (binary decode for all types)
  2c. PgEncoder.writeBind() requests binary results
  2d. PgDecoder passes format to Row

Phase 3 — PG binary parameters
  3a. PgBinaryCodec encode methods
  3b. PgEncoder.writeBind() sends binary params
  3c. TypeRegistry integration for binary param encoding

Phase 4 — MySQL binary protocol
  4a. COM_STMT_PREPARE implementation
  4b. COM_STMT_EXECUTE with binary params
  4c. Binary result row decoding
  4d. MysqlBinaryCodec (little-endian)
  4e. Remove text interpolation
```

Phase 2 is the biggest win (PG result decoding). Phase 3 adds param
encoding. Phase 4 is the MySQL overhaul. Each phase can be shipped
independently — text format continues to work as fallback.

## What stays the same

- Simple query (COM_QUERY / PG "Q" message) — always text
- Row stores `byte[][]` — raw bytes from wire
- Pipeline API (enqueue/flush/query) — unchanged
- RowMapper, TypeBinder, ColumnMapper, named params — unchanged
- Existing tests pass (text path still works, binary is additive)
