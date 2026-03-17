# Plan: JSpecify Nullability Annotations

## Convention

Per CLAUDE.md: do **not** use `@NullMarked` in `package-info.java` files. Instead, explicitly annotate
every public/protected method parameter and return type with `@NonNull` or `@Nullable` from
`org.jspecify.annotations`. Primitives are skipped (they cannot be null).

## How to determine `@NonNull` vs `@Nullable`

A type is `@Nullable` when **any** of these apply:

1. **SQL NULL propagation** — the method returns data that originates from a database column which
   can be SQL NULL. Example: `Row.getString(int)` returns `null` when the column value is NULL.
   Verify by reading the implementation: if there's a null-check/early-return-null path for the
   column data, it's `@Nullable`.

2. **Optional configuration** — the value is not required for the object to function. Example:
   `ConnectionConfig.database()` can be null because some drivers allow connecting without
   specifying a database. Verify by checking whether the field has a non-null default value in the
   constructor; if the default is `null`, the getter is `@Nullable` and the setter's param is
   `@Nullable`.

3. **Absence signaling** — null is a documented part of the return contract to mean "not present"
   or "no result." Example: `RowSet.getError()` returns null when the query succeeded (no error).
   Verify by checking callers: if they null-check the return value, it's `@Nullable`.

4. **User-supplied null in parameters** — a method parameter where callers legitimately pass null.
   Example: `BinderRegistry.bind(Object value)` accepts null and returns null for it (SQL NULL
   encoding). Verify by checking the method body: if the first thing it does is `if (value == null)
   return null`, the param is `@Nullable`.
   If the tests supply the null parameter, it cold be that it's non-null, but tests conveniently pass null because it's quick.
   In these cases it maybe that it's nullable and the tests need to become a bit more elaborate.

5. **Nullable varargs elements** — for `Object... params`, the array itself is `@NonNull` but
   individual elements can be null (to represent SQL NULL). JSpecify: annotate as
   `@Nullable Object... params`.

A type is `@NonNull` (the common case) when:

- The method **never** returns null — it always returns a valid object, or throws an exception on
  failure. Verify by checking the implementation: if there is no code path that returns null, and
  the method throws on invalid input rather than returning null, it's `@NonNull`.
- The parameter **must not** be null — passing null would cause a NullPointerException or is not a
  valid use of the API. Verify by checking: does the method use the parameter without a null-check?
  Does it call `.toString()`, `.length()`, etc. directly on it?

**When in doubt**, read the implementation and its callers. Follow the data flow: if a value
originates from a nullable source (e.g., a Map.get(), a database column, an optional field) and is
passed through without null-checking, it remains `@Nullable` through the chain.

We -- obviously -- prefer nonnull types over nullable types. So try hard to avoid nulls in the
public API, and give a list of cases that you were not really sure what to do (for human review).
Also write those cases back to this plan file in the "considereation" section.

## Iteration protocol (per file)

1. Read the file
2. For each public/protected method, determine nullability of every parameter and return type using
   the criteria above (read the implementation, check callers if needed)
3. Add `@Nullable` to parameters and return types that can be null
4. Add `@NonNull` to parameters and return types that cannot be null
5. Add the necessary imports (`org.jspecify.annotations.NonNull`, `org.jspecify.annotations.Nullable`)
6. Remove any redundant existing annotations that are already present
7. Run `./gradlew :module:test` to verify compilation + tests pass
8. Move to next file

After completing each module, run `./gradlew :module:test` once more as a final check.

## File order

### Phase 1: `djb-core` — `io.djb` package (interfaces first)

#### 1.1 Simple functional interfaces (no nullable surface)

1. `djb-core/src/main/java/io/djb/Binder.java`
   - `bind(T value)`: param `@NonNull`, return `@NonNull`
   - Contract: null is handled by BinderRegistry before calling bind()

2. `djb-core/src/main/java/io/djb/ColumnMapper.java`
   - `map(String value, String columnName)`: both params `@NonNull`, return `@NonNull`
   - Contract: null is handled by Row before calling map()

3. `djb-core/src/main/java/io/djb/JsonMapper.java`
   - `fromJson(String json, Class<T> type)`: params `@NonNull`, return `@Nullable`
     (JSON `null` literal can deserialize to Java null; also Row.get() null-checks the result of
     fromJson — see Row.java:424-428 where jsonStr null is handled before calling fromJson, but
     the JSON content itself could represent a null value depending on the mapper implementation)
   - `toJson(Object value)`: param `@NonNull`, return `@NonNull`
     (called from BaseConnection.java:216 on values known to be non-null after null-check)

4. `djb-core/src/main/java/io/djb/RowMapper.java`
   - `map(Row row)`: param `@NonNull`, return `@NonNull`
   - Rationale: RowSet.mapTo() collects results into a List via `.toList()` — null elements in the
     list would be surprising. mapFirst() returns the result directly. In practice, mappers always
     construct and return an object. If a user needs to filter rows, they should use stream()
     + filter() rather than returning null from a mapper.

#### 1.2 Data-access interfaces

5. `djb-core/src/main/java/io/djb/BinaryCodec.java`
   - All decode methods: params `@NonNull`, returns `@NonNull`
   - `canDecode(Class<?>)`: param `@NonNull`, return primitive
   - `decode(byte[], Class<T>)`: both params `@NonNull`, return `@NonNull` (throws on unsupported)
   - Rationale: null bytes are handled before the codec is called (Row checks isNull first)

6. `djb-core/src/main/java/io/djb/ColumnData.java`
   - `get(int)`: return `@Nullable` (null for SQL NULL)
   - `isNull(int)`: return primitive
   - `buffer(int)`: return `@Nullable` (null for SQL NULL, per javadoc)
   - `offset(int)`: return primitive
   - `length(int)`: return primitive (uses -1 sentinel for NULL, not Java null)

7. `djb-core/src/main/java/io/djb/Row.java`
   - All typed getters (`getString`, `getInteger`, `getLong`, etc.): return `@Nullable`
     (null for SQL NULL — implementation returns null when column data isNull)
   - `get(int, Class<T>)`, `get(String, Class<T>)`: return `@Nullable`
     (can return null for SQL NULL — see Row.java:425-426)
   - `isNull(int)`, `isNull(String)`, `size()`: primitives, skip
   - String/Class params in getters: `@NonNull`

#### 1.3 Statement/cursor interfaces

8. `djb-core/src/main/java/io/djb/PreparedStatement.java`
   - `query(Object... params)`: return `@NonNull`, elements `@Nullable Object...`
   - `query(Map<String, Object> params)`: return `@NonNull`, param `@NonNull`
     (but Map values can be null — not expressible without type-use annotations on the generic;
     document in javadoc instead)
   - `close()`: void

9. `djb-core/src/main/java/io/djb/Cursor.java`
   - `read(int)`: return `@NonNull`
   - `hasMore()`: primitive
   - `close()`: void

10. `djb-core/src/main/java/io/djb/Connection.java`
    - Most query/enqueue methods: return `@NonNull`, SQL param `@NonNull`,
      `Object... params` → `@Nullable Object...` (elements can be null for SQL NULL)
    - `query(String, Map<String, Object>)`: params `@NonNull`
    - `flush()`: return `@NonNull`
    - `prepare(String)`: return `@NonNull`, param `@NonNull`
    - `cursor(String, Object...)`: return `@NonNull`, sql `@NonNull`, params `@Nullable Object...`
    - `begin()`: return `@NonNull`
    - `withTransaction(Function)`: param `@NonNull`, return `@Nullable`
      (the function's return type T could be null; callers may use it for side-effects)
    - `executeMany(String, List)`: return `@NonNull`, params `@NonNull`
    - `ping()`: void
    - `parameters()`: return `@NonNull`
    - `queryStream(...)`: void, sql `@NonNull`, consumer `@NonNull`, params `@Nullable Object...`
    - `stream(String, Object...)`: return `@NonNull`, params `@Nullable Object...`
    - `binderRegistry()`: return `@NonNull`
    - `setBinderRegistry(BinderRegistry)`: param `@NonNull`
    - `mapperRegistry()`: return `@NonNull`
    - `setColumnMapperRegistry(ColumnMapperRegistry)`: param `@NonNull`
    - `jsonMapper()`: return `@Nullable` (null when unconfigured — see javadoc)
    - `setJsonMapper(JsonMapper)`: param `@Nullable` (allows unsetting)
    - `close()`: void

#### 1.4 Records, enums, exceptions

11. `djb-core/src/main/java/io/djb/ColumnDescriptor.java`
    - Record components: `name` is `@NonNull`, rest are primitives
    - `isJsonType()`: primitive

12. `djb-core/src/main/java/io/djb/SslMode.java`
    - Enum, no nullable surface

13. `djb-core/src/main/java/io/djb/DbException.java`
    - Check constructor params and any getters

14. `djb-core/src/main/java/io/djb/DbConnectionException.java`
    - Check constructor params (message non-null, cause nullable per Java convention)

#### 1.5 Classes

15. `djb-core/src/main/java/io/djb/BinderRegistry.java`
    - `register(Class, Binder)`: both params `@NonNull`, return `@NonNull`
    - `registerAsJson(Class)`: param `@NonNull`, return `@NonNull`
    - `jsonTypes()`: return `@NonNull`
    - `isJsonType(Class)`: param `@NonNull`, primitive return
    - `bind(Object)`: param `@Nullable`, return `@Nullable`
      (explicit null check at line 73: `if (value == null) return null`)
    - `defaults()`: return `@NonNull`
    - `hexEncode(byte[])`: param `@NonNull`, return `@NonNull`

16. `djb-core/src/main/java/io/djb/ColumnMapperRegistry.java`
    - Read implementation to determine nullability of `map()` return
    - `register(Class, ColumnMapper)`: params `@NonNull`, return `@NonNull`
    - `map(Class, String, String)`: check if it can return null or always throws
    - `hasMapper(Class)`: param `@NonNull`, primitive return
    - `defaults()`: return `@NonNull`

17. `djb-core/src/main/java/io/djb/ConnectionConfig.java`
    - Fluent setters: all return `@NonNull` (return this)
    - Nullable getters (optional fields, null default): `database()`, `username()`, `password()`,
      `sslContext()`, `pemCertPath()`, `trustStorePath()`, `trustStorePassword()`, `properties()`
    - Non-null getters (have defaults): `host()`, `sslMode()`
    - Nullable setter params: same fields as nullable getters
    - Non-null setter params: `host()`, `sslMode()`
    - Primitive getters/setters: `port()`, `hostnameVerification()`, `socketTimeoutMillis()`,
      `cachePreparedStatements()`, `preparedStatementCacheMaxSize()`,
      `preparedStatementCacheSqlLimit()`
    - `fromUri(String)`: param `@NonNull`, return `@NonNull`

18. `djb-core/src/main/java/io/djb/RowSet.java`
    - Constructor params: check existing annotations, keep/adjust
    - `first()`: return `@NonNull` (throws if empty)
    - `stream()`: return `@NonNull`
    - `iterator()`: return `@NonNull`
    - `mapTo(RowMapper)`: param `@NonNull`, return `@NonNull`
    - `mapFirst(RowMapper)`: param `@NonNull`, return `@NonNull` (mapper returns @NonNull per #4)
    - `getError()`: return `@Nullable`
    - `columnDescriptors()`: return `@NonNull`
    - `size()`, `rowsAffected()`: primitives

19. `djb-core/src/main/java/io/djb/RowStream.java`
    - Constructor: `nextRow` supplier returns `@Nullable Row` (null = exhausted),
      `onClose` is `@NonNull`
    - `forEach(Consumer)`: param `@NonNull`
    - `stream()`: return `@NonNull`
    - `iterator()`: return `@NonNull`
    - `close()`: void

20. `djb-core/src/main/java/io/djb/RowExtractors.java`
    - `extractorFor(Class)`: param `@NonNull`, return `@Nullable`
      (returns null for unsupported types — see line 106)
    - `EXTRACTORS` field: `@NonNull`

21. `djb-core/src/main/java/io/djb/Transaction.java`
    - Delegates to Connection — annotate constructor param and any overridden methods
    - `begin()`: return `@NonNull`
    - `commit()`, `rollback()`, `close()`: void

Run: `./gradlew :djb-core:test`

### Phase 2: `djb-core` — `io.djb.impl` package

22. `djb-core/src/main/java/io/djb/impl/ColumnBuffer.java`
    - Implements ColumnData — annotations must match interface
    - `append(byte[])`: param `@Nullable` (null = SQL NULL)
    - `rowCount()`: primitive

23. `djb-core/src/main/java/io/djb/impl/ByteBuffer.java`
    - Read implementation to determine surface

24. `djb-core/src/main/java/io/djb/impl/NamedParamParser.java`
    - Read implementation to determine surface

25. `djb-core/src/main/java/io/djb/impl/PreparedStatementCache.java`
    - Read implementation — CachedStatement record fields, get()/cache()/remove() methods

26. `djb-core/src/main/java/io/djb/impl/BaseConnection.java`
    - Abstract class — protected/public methods, constructor params

Run: `./gradlew :djb-core:test`

### Phase 3: `djb-pg-client`

27. `djb-pg-client/src/main/java/io/djb/pg/PgException.java`
28. `djb-pg-client/src/main/java/io/djb/pg/PgNotification.java`
29. `djb-pg-client/src/main/java/io/djb/pg/PgConnection.java`
30. `djb-pg-client/src/main/java/io/djb/pg/data/Point.java`
31. `djb-pg-client/src/main/java/io/djb/pg/data/Line.java`
32. `djb-pg-client/src/main/java/io/djb/pg/data/LineSegment.java`
33. `djb-pg-client/src/main/java/io/djb/pg/data/Box.java`
34. `djb-pg-client/src/main/java/io/djb/pg/data/Circle.java`
35. `djb-pg-client/src/main/java/io/djb/pg/data/Path.java`
36. `djb-pg-client/src/main/java/io/djb/pg/data/Polygon.java`
37. `djb-pg-client/src/main/java/io/djb/pg/data/Inet.java`
38. `djb-pg-client/src/main/java/io/djb/pg/data/Cidr.java`
39. `djb-pg-client/src/main/java/io/djb/pg/data/Interval.java`
40. `djb-pg-client/src/main/java/io/djb/pg/data/Money.java`
41. `djb-pg-client/src/main/java/io/djb/pg/data/ParserHelpers.java`
42. `djb-pg-client/src/main/java/io/djb/pg/impl/auth/MD5Authentication.java`
43. `djb-pg-client/src/main/java/io/djb/pg/impl/codec/PgProtocolConstants.java`
44. `djb-pg-client/src/main/java/io/djb/pg/impl/codec/BackendMessage.java`
45. `djb-pg-client/src/main/java/io/djb/pg/impl/codec/PgBinaryCodec.java`
46. `djb-pg-client/src/main/java/io/djb/pg/impl/codec/PgDecoder.java`
47. `djb-pg-client/src/main/java/io/djb/pg/impl/codec/PgEncoder.java`

Run: `./gradlew :djb-pg-client:test`

### Phase 4: `djb-mysql-client`

48. `djb-mysql-client/src/main/java/io/djb/mysql/MysqlException.java`
49. `djb-mysql-client/src/main/java/io/djb/mysql/MysqlConnection.java`
50. `djb-mysql-client/src/main/java/io/djb/mysql/impl/auth/Native41Authenticator.java`
51. `djb-mysql-client/src/main/java/io/djb/mysql/impl/auth/CachingSha2Authenticator.java`
52. `djb-mysql-client/src/main/java/io/djb/mysql/impl/auth/RsaPublicKeyEncryptor.java`
53. `djb-mysql-client/src/main/java/io/djb/mysql/impl/codec/MysqlProtocolConstants.java`
54. `djb-mysql-client/src/main/java/io/djb/mysql/impl/codec/MysqlBinaryCodec.java`
55. `djb-mysql-client/src/main/java/io/djb/mysql/impl/codec/MysqlDecoder.java`
56. `djb-mysql-client/src/main/java/io/djb/mysql/impl/codec/MysqlEncoder.java`

Run: `./gradlew :djb-mysql-client:test`

### Phase 5: `djb-pool`

57. `djb-pool/src/main/java/io/djb/pool/ConnectionFactory.java`
58. `djb-pool/src/main/java/io/djb/pool/PoolConfig.java`
59. `djb-pool/src/main/java/io/djb/pool/PoolException.java`
60. `djb-pool/src/main/java/io/djb/pool/PoolExhaustedException.java`
61. `djb-pool/src/main/java/io/djb/pool/PoolTimeoutException.java`
62. `djb-pool/src/main/java/io/djb/pool/PooledConnection.java`
63. `djb-pool/src/main/java/io/djb/pool/ConnectionPool.java`

Run: `./gradlew :djb-pool:test`

### Phase 6: `djb-record-mapper` and `djb-javabean-mapper`

64. `djb-record-mapper/src/main/java/io/djb/mapper/RecordRowMapper.java`
65. `djb-javabean-mapper/src/main/java/io/djb/mapper/JavaBeanRowMapper.java`

Run: `./gradlew :djb-record-mapper:test :djb-javabean-mapper:test`

### Phase 7: `djb-kotlin`

Kotlin has its own nullability system. The JSpecify annotations on the Java API will automatically
be recognized by the Kotlin compiler. Review the Kotlin files to ensure they correctly handle
nullable returns from the now-annotated Java API:

66. `djb-kotlin/src/main/kotlin/io/djb/kotlin/Extensions.kt`
67. `djb-kotlin/src/main/kotlin/io/djb/kotlin/Binders.kt`
68. `djb-kotlin/src/main/kotlin/io/djb/kotlin/RowDecoder.kt`
69. `djb-kotlin/src/main/kotlin/io/djb/kotlin/SqlJsonValue.kt`

Run: `./gradlew :djb-kotlin:test`

### Final

Run: `./gradlew test` (full test suite, requires Docker for Testcontainers)

## Considerations (for human review)

1. **`JsonMapper.fromJson()` return** — marked `@Nullable`. JSON mappers (Jackson, Gson) can return
   null for JSON `null` literals. However, in practice most users deserialize into non-null objects.
   Could be `@NonNull` if we document that JSON null should map to Java null at the Row level (which
   already handles null before calling fromJson). Worth considering.

2. **`RowMapper.map()` return** — marked `@NonNull`. This means users cannot return null from a
   mapper. The alternative is `@Nullable`, but then `mapTo()` returns `List<@Nullable T>` which is
   unergonomic. Users who need to filter should use `stream().filter()` instead.

3. **`Connection.withTransaction()` return** — left unannotated on return type `T`. The generic
   type parameter is unconstrained, so the caller decides nullability. JSpecify handles this via the
   type argument at the call site.

4. **`DbException` severity/sqlState** — marked `@Nullable`. The `toString()` method null-checks
   both. DbConnectionException passes hardcoded non-null values, but other constructors (e.g.
   PgException, MysqlException) may pass null for unknown fields.

5. **`BaseConnection.buildRowSet()` columns param** — marked `@Nullable` because the method
   explicitly checks `if (columns == null)` and returns an empty RowSet. This is for statements
   like INSERT/UPDATE that don't return column metadata.

6. **JSpecify limitation: scoping constructs** — `@NonNull` cannot be applied to nested class
   references like `PreparedStatementCache.CachedStatement`. These are left unannotated. This is a
   known JSpecify limitation with type-use annotations on inner class references.
