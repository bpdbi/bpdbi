package io.github.bpdbi.core.impl;

import io.github.bpdbi.core.BinaryCodec;
import io.github.bpdbi.core.BinderRegistry;
import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.ColumnMapperRegistry;
import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.JsonMapper;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
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
 * <p>Parameterized queries use the binary wire protocol (Postgres Parse/Bind/Execute, MySQL
 * COM_STMT_PREPARE/COM_STMT_EXECUTE). Parameterless queries use the simple/text protocol because
 * neither database's prepared-statement protocol supports all SQL commands — MySQL rejects
 * BEGIN/COMMIT/SET/etc. via COM_STMT_PREPARE, and Postgres's extended protocol forbids
 * multi-statement strings. Text format also allows getString() to work for types without a
 * dedicated binary decoder (geometric, network, array types).
 */
public abstract class BaseConnection implements Connection {

  private List<PendingStatement> pending = new ArrayList<>();
  private BinderRegistry binderRegistry = BinderRegistry.defaults();
  private ColumnMapperRegistry mapperRegistry = ColumnMapperRegistry.defaults();
  private @Nullable JsonMapper jsonMapper;
  protected @Nullable PreparedStatementCache psCache;
  protected int cacheSqlLimit;

  @Override
  public void setBinderRegistry(@NonNull BinderRegistry registry) {
    this.binderRegistry = registry;
  }

  @Override
  public @NonNull BinderRegistry binderRegistry() {
    return binderRegistry;
  }

  @Override
  public void setMapperRegistry(@NonNull ColumnMapperRegistry registry) {
    this.mapperRegistry = registry;
  }

  @Override
  public @NonNull ColumnMapperRegistry mapperRegistry() {
    return mapperRegistry;
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
   * Handle evicted statements by closing them server-side. Subclasses implement the actual close.
   */
  protected void handleEvicted(@NonNull List<PreparedStatementCache.CachedStatement> evicted) {
    for (var stmt : evicted) {
      closeCachedStatement(stmt);
    }
  }

  /** Close a cached statement server-side. DB-specific implementations override this. */
  protected abstract void closeCachedStatement(PreparedStatementCache.CachedStatement stmt);

  private static final String[] EMPTY_PARAMS = new String[0];

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
    var parsed = NamedParamParser.parse(sql, params, placeholderPrefix(), binderRegistry);
    enqueue(parsed.sql(), (Object[]) parsed.params());
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
    pending.add(new PendingStatement(sql, encodeParams(params)));
    return index;
  }

  @Override
  public int enqueue(@NonNull String sql, @NonNull Map<String, Object> params) {
    var parsed = NamedParamParser.parse(sql, params, placeholderPrefix(), binderRegistry);
    return enqueue(parsed.sql(), (Object[]) parsed.params());
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
  public @NonNull List<RowSet> flush() {
    if (pending.isEmpty()) {
      return List.of();
    }

    // Swap instead of copying — avoids O(n) ArrayList copy on every flush
    List<PendingStatement> toFlush = pending;
    pending = new ArrayList<>();

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
      results.add(executeExtendedQuery(stmt.sql, stmt.paramValues));
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
      @NonNull String sql, @Nullable Object[] params, @NonNull Consumer<Row> consumer) {
    if (!pending.isEmpty()) {
      flush();
    }
    String[] textParams = encodeParams(params);
    executeExtendedQueryStreaming(sql, textParams, consumer);
  }

  @Override
  public @NonNull RowStream stream(@NonNull String sql, @Nullable Object... params) {
    if (!pending.isEmpty()) {
      flush();
    }
    String[] textParams = params.length == 0 ? EMPTY_PARAMS : encodeParams(params);
    return createExtendedQueryRowStream(sql, textParams);
  }

  protected String[] encodeParams(Object[] params) {
    String[] textParams = new String[params.length];
    for (int i = 0; i < params.length; i++) {
      if (params[i] != null
          && jsonMapper != null
          && binderRegistry.isJsonType(params[i].getClass())) {
        textParams[i] = jsonMapper.toJson(params[i]);
      } else {
        textParams[i] = binderRegistry.bind(params[i]);
      }
    }
    return textParams;
  }

  /**
   * Create a Row with the connection's binary codec and mapper registry. Subclasses should use this
   * instead of calling the Row constructor directly.
   */
  protected @NonNull Row createRow(@NonNull ColumnDescriptor[] columns, byte @NonNull [][] values) {
    return new Row(
        columns, values, binaryCodec(), mapperRegistry, jsonMapper, binderRegistry.jsonTypes());
  }

  /**
   * Create a Row with a pre-built column name index. Use this when creating many rows from the same
   * column set (e.g. streaming) to share the map across rows.
   */
  protected @NonNull Row createRow(
      @NonNull ColumnDescriptor[] columns,
      @NonNull Map<String, Integer> columnNameIndex,
      byte @NonNull [][] values) {
    return new Row(
        columns,
        columnNameIndex,
        values,
        binaryCodec(),
        mapperRegistry,
        jsonMapper,
        binderRegistry.jsonTypes());
  }

  /**
   * Create a Row for text-format results (no binary decoding). Used by the MySQL driver for
   * parameterless queries (COM_QUERY), which return text-format data.
   */
  protected @NonNull Row createTextRow(
      @NonNull ColumnDescriptor[] columns, byte @NonNull [][] values) {
    return new Row(columns, values, null, mapperRegistry, jsonMapper, binderRegistry.jsonTypes());
  }

  /**
   * Create a Row backed by column buffers. The Row is a lightweight view that reads from the shared
   * buffers on demand.
   */
  protected @NonNull Row createBufferedRow(
      @NonNull ColumnDescriptor[] columns, @NonNull ColumnBuffer[] buffers, int rowIndex) {
    return new Row(
        columns,
        buffers,
        rowIndex,
        binaryCodec(),
        mapperRegistry,
        jsonMapper,
        binderRegistry.jsonTypes());
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
        columns,
        columnNameIndex,
        buffers,
        rowIndex,
        binaryCodec(),
        mapperRegistry,
        jsonMapper,
        binderRegistry.jsonTypes());
  }

  /**
   * Create a buffered Row for text-format results (no binary decoding). Used by the MySQL driver
   * for parameterless queries (COM_QUERY), which return text-format data.
   */
  protected @NonNull Row createTextBufferedRow(
      @NonNull ColumnDescriptor[] columns, @NonNull ColumnBuffer[] buffers, int rowIndex) {
    return new Row(
        columns, buffers, rowIndex, null, mapperRegistry, jsonMapper, binderRegistry.jsonTypes());
  }

  /**
   * Return the binary codec for this database driver, or null if not applicable. Subclasses
   * override to provide their driver-specific codec.
   */
  protected @Nullable BinaryCodec binaryCodec() {
    return null;
  }

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

  /**
   * Execute a query. An empty params array indicates a parameterless query.
   *
   * <p>Parameterized queries use the binary wire protocol (PG Parse/Bind/Execute, MySQL
   * COM_STMT_PREPARE/COM_STMT_EXECUTE). Parameterless queries use the simple/text protocol (PG
   * Query message, MySQL COM_QUERY) — see class javadoc for why.
   */
  protected abstract @NonNull RowSet executeExtendedQuery(
      @NonNull String sql, @NonNull String[] params);

  protected abstract void sendTerminate();

  protected abstract void closeTransport();

  /**
   * Return the placeholder prefix for this database. PG uses "$" (for $1, $2, ...), MySQL uses "?"
   * (positional).
   */
  protected abstract @NonNull String placeholderPrefix();

  // --- Streaming abstract methods ---

  /**
   * Execute a query and call consumer for each row instead of buffering. An empty params array
   * indicates a parameterless query.
   */
  protected abstract void executeExtendedQueryStreaming(
      @NonNull String sql, @NonNull String[] params, @NonNull Consumer<Row> consumer);

  /** Create a RowStream for a query. An empty params array indicates a parameterless query. */
  protected abstract @NonNull RowStream createExtendedQueryRowStream(
      @NonNull String sql, @NonNull String[] params);

  // --- Shared helpers for result set construction ---

  protected static ColumnBuffer[] newColumnBuffers(int columnCount) {
    return newColumnBuffers(columnCount, 64, 32);
  }

  /**
   * Create column buffers with explicit initial sizing. Use when the expected row count and average
   * value size are known (e.g. from a previous cursor batch).
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
  protected record PendingStatement(String sql, String[] paramValues) {}
}
