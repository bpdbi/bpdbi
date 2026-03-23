package io.github.bpdbi.core.impl;

import io.github.bpdbi.core.BinaryCodec;
import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.JsonMapper;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import io.github.bpdbi.core.TypeRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Abstract base class that implements the shared pipeline logic (enqueue/flush/query).
 * Database-specific drivers extend this and implement the protocol-level methods.
 *
 * <p>All queries use the extended query protocol with binary result format.
 */
public abstract class BaseConnection implements Connection {

  private static final Object[] EMPTY_PARAMS = new Object[0];

  private @NonNull List<PendingStatement> pending = new ArrayList<>(4);
  private @NonNull TypeRegistry typeRegistry = new TypeRegistry();
  private @Nullable JsonMapper jsonMapper;
  protected @Nullable PreparedStatementCache psCache;
  protected int cacheSqlLimit;

  @Override
  public void setTypeRegistry(@NonNull TypeRegistry registry) {
    this.typeRegistry = registry;
  }

  @Override
  public @NonNull TypeRegistry typeRegistry() {
    return typeRegistry;
  }

  @Override
  public void setJsonMapper(@Nullable JsonMapper mapper) {
    this.jsonMapper = mapper;
  }

  @Override
  public @Nullable JsonMapper jsonMapper() {
    return jsonMapper;
  }

  /** Initialize the prepared statement cache from config. Called by subclass constructors. */
  protected void initCache(@Nullable ConnectionConfig config) {
    if (config != null && config.cachePreparedStatements()) {
      this.psCache = new PreparedStatementCache(config.preparedStatementCacheMaxSize());
      this.cacheSqlLimit = config.preparedStatementCacheSqlLimit();
    }
  }

  /** Check if a SQL string is eligible for caching. */
  protected boolean isCacheable(@NonNull String sql) {
    return psCache != null && sql.length() <= cacheSqlLimit;
  }

  /**
   * Handle an evicted statement by closing it server-side. Subclasses implement the actual close.
   */
  protected void handleEvicted(PreparedStatementCache.@Nullable CachedStatement evicted) {
    if (evicted != null) {
      closeCachedStatement(evicted);
    }
  }

  /** Close a cached statement server-side. DB-specific implementations override this. */
  protected abstract void closeCachedStatement(PreparedStatementCache.CachedStatement stmt);

  @Override
  public @NonNull RowSet query(@NonNull String sql) {
    enqueue(sql);
    var result = flush().getLast();
    if (result.getError() != null) {
      throw result.getError();
    }
    return result;
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @Nullable Object... params) {
    enqueue(sql, params);
    var result = flush().getLast();
    if (result.getError() != null) {
      throw result.getError();
    }
    return result;
  }

  @Override
  public @NonNull RowSet query(@NonNull String sql, @NonNull Map<String, Object> params) {
    var parsed = NamedParamParser.parse(sql, params, "$");
    enqueue(parsed.sql(), parsed.params());
    var result = flush().getLast();
    if (result.getError() != null) {
      throw result.getError();
    }
    return result;
  }

  /**
   * Attach the SQL string to a RowSet's error (if any). Called after executeExtendedQuery so the
   * exception carries context about which statement failed.
   */
  private static void attachSqlToError(@NonNull RowSet result, @NonNull String sql) {
    var error = result.getError();
    if (error != null) {
      error.setSql(sql);
    }
  }

  @Override
  public int enqueue(@NonNull String sql) {
    int index = pending.size();
    pending.add(new PendingStatement(sql, EMPTY_PARAMS));
    return index;
  }

  @Override
  public int enqueue(@NonNull String sql, @Nullable Object... params) {
    int index = pending.size();
    pending.add(new PendingStatement(sql, params));
    return index;
  }

  @Override
  public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
    var parsed = NamedParamParser.parse(sql, params, "$");
    return enqueue(parsed.sql(), parsed.params());
  }

  @Override
  public @NonNull List<RowSet> executeMany(@NonNull String sql, @NonNull List<Object[]> paramSets) {
    int offset = pending.size(); // skip any pre-existing pending statements (e.g. lazy BEGIN)
    for (Object[] params : paramSets) {
      enqueue(sql, params);
    }
    List<RowSet> allResults = flush();
    // Return only the results for the paramSets we enqueued, not pre-existing pending statements
    if (offset == 0) {
      return allResults;
    }
    return allResults.subList(offset, allResults.size());
  }

  @Override
  public @NonNull List<RowSet> executeManyNamed(
      @NonNull String sql, @NonNull List<Map<String, Object>> paramSets) {
    int offset = pending.size();
    for (Map<String, Object> params : paramSets) {
      enqueue(sql, params);
    }
    List<RowSet> allResults = flush();
    if (offset == 0) {
      return allResults;
    }
    return allResults.subList(offset, allResults.size());
  }

  @Override
  public @NonNull List<RowSet> flush() {
    if (pending.isEmpty()) {
      return List.of();
    }

    // Swap instead of copying — avoids O(n) ArrayList copy on every flush
    List<PendingStatement> toFlush = pending;
    pending = new ArrayList<>(4);

    List<RowSet> results = executePipelinedBatch(toFlush);
    for (int i = 0; i < results.size(); i++) {
      attachSqlToError(results.get(i), toFlush.get(i).sql);
    }

    return results;
  }

  /**
   * Execute a batch of pending statements. Subclasses may override to implement pipelining (e.g.
   * sending all messages in a single TCP write with one Sync at the end). The default
   * implementation executes statements sequentially.
   */
  protected @NonNull List<RowSet> executePipelinedBatch(
      @NonNull List<PendingStatement> statements) {
    List<RowSet> results = new ArrayList<>(statements.size());
    for (PendingStatement stmt : statements) {
      results.add(executeExtendedQuery(stmt.sql, stmt.params));
    }
    return results;
  }

  @Override
  public void queryStream(@NonNull String sql, @NonNull Consumer<Row> consumer) {
    if (!pending.isEmpty()) {
      flush();
    }
    executeExtendedQueryStreaming(sql, EMPTY_PARAMS, consumer);
  }

  @Override
  public void queryStream(
      @NonNull String sql, @NonNull Object[] params, @NonNull Consumer<Row> consumer) {
    if (!pending.isEmpty()) {
      flush();
    }
    executeExtendedQueryStreaming(sql, params, consumer);
  }

  @Override
  public @NonNull RowStream stream(@NonNull String sql, @Nullable Object... params) {
    if (!pending.isEmpty()) {
      flush();
    }
    return createExtendedQueryRowStream(sql, params.length == 0 ? EMPTY_PARAMS : params);
  }

  /** Text-encode a single parameter value, handling JSON types via jsonMapper if configured. */
  protected String encodeParamToText(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (jsonMapper != null && typeRegistry.isJsonType(value.getClass())) {
      return jsonMapper.toJson(value);
    }
    return value.toString();
  }

  /**
   * Create a Row with a pre-built column name index. Use this when creating many rows from the same
   * column set (e.g. streaming) to share the map across rows.
   */
  protected @NonNull Row createRow(
      @NonNull ColumnDescriptor[] columns,
      @NonNull Map<String, Integer> columnNameIndex,
      byte @NonNull [][] values) {
    return new Row(columns, columnNameIndex, values, binaryCodec(), typeRegistry, jsonMapper);
  }

  /**
   * Create a buffered Row with a pre-built column name index. Use this when creating many rows from
   * the same column set to share the map across rows.
   */
  protected @NonNull Row createBufferedRow(
      @NonNull ColumnDescriptor[] columns,
      @NonNull Map<String, Integer> columnNameIndex,
      @NonNull ColumnBuffer[] buffers,
      int rowIndex) {
    return new Row(
        columns, columnNameIndex, buffers, rowIndex, binaryCodec(), typeRegistry, jsonMapper);
  }

  /** Return the binary codec for this database driver. */
  protected abstract @NonNull BinaryCodec binaryCodec();

  @Override
  public void close() {
    if (psCache != null) {
      for (var stmt : psCache.values()) {
        closeCachedStatement(stmt);
      }
      psCache.clear();
    }
    sendTerminate();
    closeTransport();
  }

  // --- Abstract methods for database-specific protocol ---

  /** Flush the write buffer to the network. */
  protected abstract void flushToNetwork();

  /** Execute a query using the extended query protocol. An empty params array is parameterless. */
  protected abstract @NonNull RowSet executeExtendedQuery(
      @NonNull String sql, @NonNull Object[] params);

  protected abstract void sendTerminate();

  protected abstract void closeTransport();

  // --- Streaming abstract methods ---

  /**
   * Execute a query and call consumer for each row instead of buffering. An empty params array
   * indicates a parameterless query.
   */
  protected abstract void executeExtendedQueryStreaming(
      @NonNull String sql, @NonNull Object[] params, @NonNull Consumer<Row> consumer);

  /** Create a RowStream for a query. An empty params array indicates a parameterless query. */
  protected abstract @NonNull RowStream createExtendedQueryRowStream(
      @NonNull String sql, @NonNull Object[] params);

  // --- Shared helpers for result set construction ---

  protected static ColumnBuffer[] newColumnBuffers(int columnCount) {
    return newColumnBuffers(columnCount, 64, 32);
  }

  /**
   * Create column buffers with explicit initial sizing. Use when the expected row count and average
   * value size are known (e.g., from a previous cursor batch).
   */
  protected static ColumnBuffer[] newColumnBuffers(
      int columnCount, int initialRows, int estimatedAvgSize) {
    ColumnBuffer[] buffers = new ColumnBuffer[columnCount];
    for (int i = 0; i < columnCount; i++) {
      buffers[i] = new ColumnBuffer(initialRows, estimatedAvgSize);
    }
    return buffers;
  }

  protected static void appendToBuffers(ColumnBuffer[] buffers, byte[][] values) {
    for (int i = 0; i < values.length; i++) {
      buffers[i].append(values[i]);
    }
  }

  /**
   * Build a RowSet from column buffers. {@code columns} is nullable because statements like
   * INSERT/UPDATE/DELETE don't return column metadata — in that case an empty RowSet with only the
   * rowsAffected count is returned.
   */
  protected @NonNull RowSet buildRowSet(
      ColumnDescriptor @Nullable [] columns,
      @NonNull ColumnBuffer[] buffers,
      int rowCount,
      int rowsAffected) {
    if (columns == null) {
      return new RowSet(List.of(), List.of(), rowsAffected);
    }
    // Build the column name index once and share it across all rows
    Map<String, Integer> nameIndex = Row.buildColumnNameIndex(columns);
    List<Row> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(createBufferedRow(columns, nameIndex, buffers, i));
    }
    return new RowSet(rows, List.of(columns), rowsAffected);
  }

  // --- Internal ---

  @SuppressWarnings("ArrayRecordComponent") // internal record; array ownership is intentional
  protected record PendingStatement(String sql, Object[] params) {}
}
