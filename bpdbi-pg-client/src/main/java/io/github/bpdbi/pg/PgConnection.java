package io.github.bpdbi.pg;

import io.github.bpdbi.core.BinaryCodec;
import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.DbException;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import io.github.bpdbi.core.SslMode;
import io.github.bpdbi.core.impl.BaseConnection;
import io.github.bpdbi.core.impl.ColumnBuffer;
import io.github.bpdbi.core.impl.NamedParamParser;
import io.github.bpdbi.core.impl.PreparedStatementCache;
import io.github.bpdbi.core.impl.UnsyncBufferedInputStream;
import io.github.bpdbi.core.impl.UnsyncBufferedOutputStream;
import io.github.bpdbi.pg.impl.auth.MD5Authentication;
import io.github.bpdbi.pg.impl.codec.BackendMessage;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSasl;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslContinue;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslFinal;
import io.github.bpdbi.pg.impl.codec.BinaryParamEncoder;
import io.github.bpdbi.pg.impl.codec.PgBinaryCodec;
import io.github.bpdbi.pg.impl.codec.PgDecoder;
import io.github.bpdbi.pg.impl.codec.PgEncoder;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Postgres implementation of {@link io.github.bpdbi.core.Connection}.
 *
 * <p><b>Not thread-safe.</b> Each connection must be used by a single thread at a time. Designed
 * for one-connection-per-(virtual-)thread usage with Java 21+ virtual threads.
 */
public final class PgConnection extends BaseConnection {

  private final Socket socket;
  private final OutputStream out;
  private final PgEncoder encoder;
  private final PgDecoder decoder;
  private final Map<String, String> parameters = new HashMap<>();
  private @Nullable Map<String, String> parametersView;
  private final List<PgNotification> notifications = new ArrayList<>();
  private int processId;
  private int secretKey;
  private long stmtCounter = 0;
  private int maxResultBufferBytes = 0;

  /**
   * Conservative estimate of TCP receive buffer size. If the estimated server response exceeds
   * this, a mid-pipeline Sync is inserted to prevent deadlock. Same threshold as pgjdbc.
   */
  private static final int MAX_BUFFERED_RECV_BYTES = 64 * 1024;

  /**
   * Estimated response bytes per statement. Each query returns at minimum: ParseComplete (5) +
   * BindComplete (5) + RowDescription/NoData (~50) + CommandComplete (~20) + overhead (~170).
   */
  private static final int ESTIMATED_RESPONSE_BYTES_PER_QUERY = 250;

  private PgConnection(Socket socket, OutputStream out, PgEncoder encoder, PgDecoder decoder) {
    this.socket = socket;
    this.out = out;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  /** Connect to a Postgres server. */
  public static @NonNull PgConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password) {
    return connect(host, port, database, user, password, null);
  }

  /** Connect to a Postgres server with optional properties. */
  public static @NonNull PgConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password,
      @Nullable Map<String, String> properties) {
    return connect(host, port, database, user, password, properties, SslMode.DISABLE);
  }

  /** Connect to a Postgres server with SSL mode. */
  public static @NonNull PgConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password,
      @Nullable Map<String, String> properties,
      @NonNull SslMode sslMode) {
    var config = new ConnectionConfig(host, port, database, user, password);
    config.sslMode(sslMode);
    config.properties(properties);
    return connect(config);
  }

  /** Connect using a ConnectionConfig (supports URI parsing, SSL, and all options). */
  public static @NonNull PgConnection connect(@NonNull ConnectionConfig config) {
    try {
      Socket socket = new Socket(config.host(), config.port() > 0 ? config.port() : 5432);
      socket.setTcpNoDelay(true);
      if (config.socketTimeoutMillis() > 0) {
        socket.setSoTimeout(config.socketTimeoutMillis());
      }

      if (config.sslMode() != SslMode.DISABLE) {
        socket = io.github.bpdbi.pg.impl.PgSsl.upgradeToSsl(socket, config);
      }

      var out = new UnsyncBufferedOutputStream(socket.getOutputStream(), 8192);
      var in = new UnsyncBufferedInputStream(socket.getInputStream(), 8192);
      var encoder = new PgEncoder();
      var decoder = new PgDecoder(in);

      var conn = new PgConnection(socket, out, encoder, decoder);
      conn.initCache(config);
      conn.maxResultBufferBytes = config.maxResultBufferBytes();
      conn.performStartup(
          config.username(), config.database(), config.password(), config.properties());
      return conn;
    } catch (IOException e) {
      throw new DbConnectionException(
          "Failed to connect to Postgres at " + config.host() + ":" + config.port(), e);
    }
  }

  @Override
  public void ping() {
    query("");
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    Map<String, String> view = parametersView;
    if (view == null) {
      view = Collections.unmodifiableMap(parameters);
      parametersView = view;
    }
    return view;
  }

  public int processId() {
    return processId;
  }

  public int secretKey() {
    return secretKey;
  }

  // --- Prepared statements ---

  @Override
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    // Parse named parameters if present
    List<String> parameterNames = null;
    String sqlToSend = sql;
    if (NamedParamParser.containsNamedParams(sql)) {
      var template = NamedParamParser.parseTemplate(sql, "$");
      sqlToSend = template.sql();
      parameterNames = template.parameterNames();
    }

    // Check cache (keyed on original SQL)
    if (isCacheable(sql)) {
      var cached = psCache.get(sql);
      if (cached != null) {
        return new CachedPgPreparedStatement(cached);
      }
    }

    byte[] stmtNameCString = nextStatementNameCString();
    String stmtName = cstringToString(stmtNameCString);
    final List<String> paramNames = parameterNames;
    try {
      encoder.writeParse(stmtName, sqlToSend, null);
      encoder.writeDescribeStatement(stmtName);
      encoder.writeSync();
      encoder.flush(out);

      // Read ParseComplete, ParameterDescription/RowDescription/NoData, ReadyForQuery
      ColumnDescriptor[] columns = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.RowDescription rd -> columns = rd.columns();
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.ReadyForQuery rq -> {
            if (isCacheable(sql)) {
              var cached =
                  new PreparedStatementCache.CachedStatement(
                      sql, stmtName, stmtNameCString, columns, null, paramNames);
              handleEvicted(psCache.cache(sql, cached));
              return new CachedPgPreparedStatement(cached);
            }
            return new PgPreparedStatement(stmtName, stmtNameCString, paramNames);
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error preparing statement", e);
    }
  }

  @Override
  public @NonNull Cursor cursor(@NonNull String sql, @Nullable Object... params) {
    String portalName = "_bpdbi_p" + (stmtCounter++);
    Object[] cursorParams =
        applyEncoders((params != null && params.length > 0) ? params : new Object[0]);
    try {
      // Parse unnamed + Bind into named portal + Describe portal + Sync
      encoder.writeParse("", sql, computeParamTypeOIDs(cursorParams));
      encoder.writeBindInline(
          PgEncoder.toCString(portalName), PgEncoder.EMPTY_CSTRING, cursorParams, textEncoder());
      encoder.writeDescribePortal(portalName);
      encoder.writeSync();
      encoder.flush(out);

      ColumnDescriptor[] columns = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.RowDescription rd -> columns = rd.columns();
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.ReadyForQuery rq -> {
            return new PgCursor(this, portalName, columns);
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error creating cursor", e);
    }
  }

  // --- LISTEN/NOTIFY ---

  /**
   * Subscribe to a Postgres notification channel. Sends {@code LISTEN channel}. Notifications are
   * collected and can be retrieved via {@link #getNotifications()}.
   *
   * @param channel the channel name to listen on
   */
  public void listen(@NonNull String channel) {
    query("LISTEN " + quoteIdentifier(channel));
  }

  /**
   * Unsubscribe from a Postgres notification channel. Sends {@code UNLISTEN channel}.
   *
   * @param channel the channel name to stop listening on
   */
  public void unlisten(@NonNull String channel) {
    query("UNLISTEN " + quoteIdentifier(channel));
  }

  /** Unsubscribe from all notification channels. Sends {@code UNLISTEN *}. */
  public void unlistenAll() {
    query("UNLISTEN *");
  }

  /**
   * Send a notification on a channel with an optional payload. Sends {@code NOTIFY channel,
   * 'payload'}.
   *
   * @param channel the channel name
   * @param payload the notification payload (may be empty)
   */
  public void notify(@NonNull String channel, @NonNull String payload) {
    query("NOTIFY " + quoteIdentifier(channel) + ", " + quoteLiteral(payload));
  }

  /**
   * Retrieve and clear all notifications received since the last call. Notifications arrive
   * asynchronously from the server and are buffered as they are encountered during normal query
   * processing.
   *
   * <p>To poll for notifications without executing a query, call with an empty query:
   *
   * <pre>{@code
   * conn.query("");  // triggers a network roundtrip, buffering any pending notifications
   * List<PgNotification> notes = conn.getNotifications();
   * }</pre>
   *
   * @return list of notifications received since the last call (never null, may be empty)
   */
  public @NonNull List<PgNotification> getNotifications() {
    var result = List.copyOf(notifications);
    notifications.clear();
    return result;
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static String quoteLiteral(String literal) {
    return "'" + literal.replace("'", "''") + "'";
  }

  /** Cancel a running query on this connection (sends cancel request via a new TCP connection). */
  public void cancelRequest() {
    try (var cancelSocket = new Socket(socket.getInetAddress(), socket.getPort())) {
      var cancelEncoder = new PgEncoder();
      cancelEncoder.writeCancelRequest(processId, secretKey);
      cancelEncoder.flush(cancelSocket.getOutputStream());
    } catch (IOException e) {
      throw new DbConnectionException("Failed to send cancel request", e);
    }
  }

  // --- Inner classes for prepared statements and cursors ---

  /**
   * Execute a prepared statement query: Bind + DescribePortal + Execute + Sync + flush, then read
   * the response. Shared by both PgPreparedStatement and CachedPgPreparedStatement.
   */
  private RowSet executePreparedQuery(byte[] stmtNameCString, Object[] params) {
    params = applyEncoders(params);
    try {
      encoder.writeBindInline(PgEncoder.EMPTY_CSTRING, stmtNameCString, params, textEncoder());
      encoder.writeDescribePortal();
      encoder.writeExecute();
      encoder.writeSync();
      encoder.flush(out);
      return readExtendedQueryResponse();
    } catch (IOException e) {
      throw new DbConnectionException("I/O error executing prepared statement", e);
    }
  }

  private final class PgPreparedStatement implements PreparedStatement {

    private final String name;
    private final byte[] nameCString;
    private final List<String> parameterNames;
    private boolean closed = false;

    PgPreparedStatement(String name, byte[] nameCString, List<String> parameterNames) {
      this.name = name;
      this.nameCString = nameCString;
      this.parameterNames = parameterNames;
    }

    @Override
    public @NonNull RowSet query(@Nullable Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      return executePreparedQuery(nameCString, params);
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (parameterNames == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      return query(NamedParamParser.resolveParams(parameterNames, params));
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        try {
          encoder.writeCloseStatement(name);
          encoder.writeSync();
          encoder.flush(out);
          // Read CloseComplete + ReadyForQuery
          while (true) {
            BackendMessage msg = decoder.readMessage();
            if (msg instanceof BackendMessage.ReadyForQuery) {
              break;
            }
          }
        } catch (IOException e) {
          // best effort
        }
      }
    }
  }

  /**
   * Wrapper around a cached prepared statement. close() evicts from cache and closes server-side
   * (matching vertx behavior).
   */
  private final class CachedPgPreparedStatement implements PreparedStatement {

    private final PreparedStatementCache.CachedStatement cached;
    private boolean closed = false;

    CachedPgPreparedStatement(PreparedStatementCache.CachedStatement cached) {
      this.cached = cached;
    }

    @Override
    public @NonNull RowSet query(@Nullable Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      return executePreparedQuery(cached.pgStatementNameCString(), params);
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (cached.parameterNames() == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      return query(NamedParamParser.resolveParams(cached.parameterNames(), params));
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        // Evict from cache and close server-side
        psCache.remove(cached.sql());
        closeCachedStatement(cached);
      }
    }
  }

  // --- BaseConnection protocol methods ---

  /**
   * Apply registered type encoders to convert domain types into binary-encodable types. Returns the
   * original array if no encoders are registered.
   */
  private Object[] applyEncoders(Object[] params) {
    if (!typeRegistry().hasEncoders() || params.length == 0) return params;
    Object[] result = new Object[params.length];
    for (int i = 0; i < params.length; i++) {
      result[i] = typeRegistry().encode(params[i]);
    }
    return result;
  }

  /**
   * Compute Postgres type OIDs from Java parameter types. Used in Parse messages so the server
   * knows how to interpret binary params. Returns null for empty params.
   */
  private static int @Nullable [] computeParamTypeOIDs(Object[] params) {
    if (params.length == 0) return null;
    int[] oids = new int[params.length];
    for (int i = 0; i < params.length; i++) {
      oids[i] = BinaryParamEncoder.typeOID(params[i]);
    }
    return oids;
  }

  private final Function<Object, String> textEncoder = this::encodeParamToText;

  /** Returns a text encoding function for params that cannot be binary-encoded. */
  private Function<Object, String> textEncoder() {
    return textEncoder;
  }

  @Override
  protected @NonNull RowSet executeExtendedQuery(@NonNull String sql, @NonNull Object[] params) {
    params = applyEncoders(params);
    invalidateCacheIfNeeded(sql);

    if (isCacheable(sql)) {
      var cached = psCache.get(sql);
      if (cached != null) {
        // Cache hit: skip Parse and DescribePortal — the server already knows this statement,
        // and we have the column metadata cached from the first execution.
        // No OID computation needed — Parse was already sent on the cache-miss path.
        encoder.writeBindInline(
            PgEncoder.EMPTY_CSTRING, cached.pgStatementNameCString(), params, textEncoder());
        encoder.writeExecute();
        encoder.writeSync();
        flushToNetwork();
        RowSet result = readExtendedQueryResponse(cached.columns());
        if (result.getError() != null && isStalePlanError(result.getError())) {
          // Evict this specific stale statement and re-execute
          closeCachedStatement(cached);
          psCache.remove(sql);
          return executeExtendedQuery(sql, params);
        }
        return result;
      } else {
        // Cache miss: use named statement so we can cache it.
        // Send Java-type OIDs in Parse so Postgres knows how to interpret binary params.
        int[] typeOIDs = computeParamTypeOIDs(params);
        byte[] stmtNameCString = nextStatementNameCString();
        String stmtName = cstringToString(stmtNameCString);
        encoder.writeParse(stmtName, sql, typeOIDs);
        encoder.writeBindInline(PgEncoder.EMPTY_CSTRING, stmtNameCString, params, textEncoder());
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
        flushToNetwork();
        RowSet result = readExtendedQueryResponse();
        // Cache after successful execution, including column metadata for future Describe skipping
        if (result.getError() == null) {
          List<ColumnDescriptor> colList = result.columnDescriptors();
          ColumnDescriptor[] resultColumns =
              colList.isEmpty() ? null : colList.toArray(new ColumnDescriptor[0]);
          var stmt =
              new PreparedStatementCache.CachedStatement(
                  sql, stmtName, stmtNameCString, resultColumns, typeOIDs, null);
          handleEvicted(psCache.cache(sql, stmt));
        }
        return result;
      }
    }

    // No caching: use unnamed statement
    encoder.writeParse(sql, computeParamTypeOIDs(params));
    encoder.writeBindInline(
        PgEncoder.EMPTY_CSTRING, PgEncoder.EMPTY_CSTRING, params, textEncoder());
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    flushToNetwork();
    return readExtendedQueryResponse();
  }

  // --- Pipelined batch execution ---

  @Override
  protected @NonNull List<RowSet> executePipelinedBatch(
      @NonNull List<PendingStatement> statements) {
    if (statements.size() <= 1) {
      return super.executePipelinedBatch(statements);
    }

    // Pre-size the encoder buffer to avoid resizing during batch encoding
    int estimatedBytes = 5; // Sync message at the end
    for (PendingStatement stmt : statements) {
      estimatedBytes += PgEncoder.estimateExtendedQuerySize(stmt.sql(), stmt.params());
    }
    encoder.ensureCapacity(estimatedBytes);

    // Deadlock prevention: if the estimated server response exceeds the TCP receive
    // buffer (~64KB), the server may block trying to send while we block trying to write.
    // Split large batches into chunks with intermediate Sync+drain to avoid this.
    // Conservative estimate: each query response is at least ~250 bytes
    // (ParseComplete + BindComplete + RowDescription/NoData + CommandComplete).
    int estimatedResponseBytes = 0;
    int chunkStart = 0;
    List<RowSet> allResults = new ArrayList<>(statements.size());
    Function<Object, String> fallback = textEncoder();

    // Phase 1: Write messages, flushing when response estimate gets risky.
    // Optimization: skip Parse when SQL matches the previous statement (server reuses unnamed
    // statement), and skip Describe when SQL matches (column metadata is identical — the response
    // reader carries forward the last RowDescription).
    String lastParsedSql = null;
    for (int i = 0; i < statements.size(); i++) {
      PendingStatement stmt = statements.get(i);
      estimatedResponseBytes += ESTIMATED_RESPONSE_BYTES_PER_QUERY;

      Object[] stmtParams = applyEncoders(stmt.params());
      boolean sqlChanged = !stmt.sql().equals(lastParsedSql);
      if (sqlChanged) {
        encoder.writeParse(stmt.sql(), computeParamTypeOIDs(stmtParams));
        lastParsedSql = stmt.sql();
      }
      encoder.writeBindInline(
          PgEncoder.EMPTY_CSTRING, PgEncoder.EMPTY_CSTRING, stmtParams, fallback);
      if (sqlChanged) {
        encoder.writeDescribePortal();
      }
      encoder.writeExecute();

      // Flush and drain mid-pipeline if approaching TCP buffer limit
      if (estimatedResponseBytes >= MAX_BUFFERED_RECV_BYTES && i < statements.size() - 1) {
        int chunkSize = i - chunkStart + 1;
        encoder.writeSync();
        flushToNetwork();
        allResults.addAll(readPipelinedResponses(chunkSize));
        chunkStart = i + 1;
        estimatedResponseBytes = 0;
        lastParsedSql = null; // server forgets unnamed statement after Sync
      }
    }

    // Final chunk
    encoder.writeSync();
    flushToNetwork();
    int remainingCount = statements.size() - chunkStart;
    allResults.addAll(readPipelinedResponses(remainingCount));

    return allResults;
  }

  private List<RowSet> readPipelinedResponses(int count) {
    try {
      List<RowSet> results = new ArrayList<>(count);
      // lastColumns carries forward column metadata from the most recent RowDescription.
      // When Describe is skipped for repeated SQL, the server won't send RowDescription,
      // so we reuse the previous metadata for SELECT results.
      ColumnDescriptor[] lastColumns = null;
      ColumnDescriptor[] columns = null;
      ColumnBuffer[] buffers = null;
      int rowCount = 0;
      int rowsAffected = 0;

      while (true) {
        BackendMessage msg = decoder.readMessageIntoBuffers(buffers);
        // Reference equality for high-frequency singletons — avoids pattern-matching overhead
        // on the hottest path of pipelined response decoding.
        if (msg == BackendMessage.DATA_ROW_SENTINEL) {
          if (columns == null && lastColumns != null) {
            // Describe was skipped — reuse previous column metadata
            columns = lastColumns;
            buffers = newColumnBuffers(columns.length);
          }
          rowCount++;
          continue;
        }
        if (msg == BackendMessage.PARSE_COMPLETE
            || msg == BackendMessage.BIND_COMPLETE
            || msg == BackendMessage.NO_DATA) {
          continue;
        }
        switch (msg) {
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            lastColumns = columns;
            buffers = newColumnBuffers(columns.length);
          }
          case BackendMessage.DataRow dr -> {
            if (buffers == null && lastColumns != null) {
              columns = lastColumns;
              buffers = newColumnBuffers(columns.length);
            }
            appendToBuffers(buffers, dr.values());
            rowCount++;
          }
          case BackendMessage.CommandComplete cc -> {
            rowsAffected = cc.rowsAffected();
            results.add(buildRowSet(columns, buffers, rowCount, rowsAffected));
            // Reset per-statement accumulators but keep lastColumns for reuse
            columns = null;
            buffers = null;
            rowCount = 0;
            rowsAffected = 0;
          }
          case BackendMessage.EmptyQueryResponse eq -> {
            results.add(buildRowSet(null, null, 0, 0));
          }
          case BackendMessage.ErrorResponse err -> {
            results.add(new RowSet(PgException.fromErrorResponse(err)));
            // Server skips remaining messages until Sync; wait for ReadyForQuery
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif -> handleNotification(notif);
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ReadyForQuery rq -> {
            // Fill skipped statements (after an error) with error RowSets
            while (results.size() < count) {
              results.add(
                  new RowSet(
                      new DbException(
                          null, null, "Skipped: earlier statement in pipeline failed")));
            }
            return results;
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading pipelined response", e);
    }
  }

  /**
   * Detect statements that may invalidate cached prepared statements. search_path changes cause
   * table/function names to resolve differently, so cached Parse results may become stale.
   * DEALLOCATE ALL and DISCARD ALL explicitly destroy all prepared statements. Like pgjdbc, we
   * detect this by inspecting the SQL text since Postgres does not send ParameterStatus for
   * search_path changes.
   */
  private void invalidateCacheIfNeeded(String sql) {
    if (psCache != null && !psCache.isEmpty()) {
      if ((sql.regionMatches(true, 0, "SET", 0, 3) && containsIgnoreCase(sql, "search_path"))
          || sql.regionMatches(true, 0, "DEALLOCATE", 0, 10)
          || sql.regionMatches(true, 0, "DISCARD", 0, 7)) {
        invalidatePreparedStatementCache();
      }
    }
  }

  private static boolean containsIgnoreCase(String haystack, String needle) {
    int hLen = haystack.length();
    int nLen = needle.length();
    for (int i = 0; i <= hLen - nLen; i++) {
      if (haystack.regionMatches(true, i, needle, 0, nLen)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Generate the next statement name as a null-terminated C string byte array. Uses hex encoding
   * like Vert.x — avoids String allocation and getBytes() on the wire-write hot path. Format:
   * "0000000\0", "0000001\0", ..., "000FFFF\0", etc. (minimum 7 hex digits).
   */
  private byte[] nextStatementNameCString() {
    long n = stmtCounter++;
    int hexDigits = Math.max(7, (64 - Long.numberOfLeadingZeros(n) + 3) / 4);
    byte[] name = new byte[hexDigits + 1]; // +1 for null terminator
    for (int i = hexDigits - 1; i >= 0; i--) {
      int digit = (int) (n & 0xF);
      name[i] = (byte) (digit < 10 ? '0' + digit : 'A' + digit - 10);
      n >>>= 4;
    }
    // name[hexDigits] is already 0 (null terminator)
    return name;
  }

  /** Convert a C string byte[] (with null terminator) to a String (without null terminator). */
  private static String cstringToString(byte[] cstring) {
    return new String(cstring, 0, cstring.length - 1, StandardCharsets.UTF_8);
  }

  @Override
  protected void flushToNetwork() {
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
  }

  /**
   * Read a single-statement response using the lightweight direct-row path instead of
   * ColumnBuffers. This is used for the prepared statement cache-hit path where column metadata is
   * already known.
   *
   * <p><b>Why not ColumnBuffers here?</b> ColumnBuffers are designed for multi-row result sets —
   * they pack all values for each column into a contiguous byte array, reducing GC pressure for
   * large result sets (100+ rows). But they pre-allocate capacity for 64 rows × 32 bytes per column
   * = ~2.5 KB per column. For the most common case — a single-row lookup query with 7 columns —
   * that's ~18 KB of allocation for ~200 bytes of actual data. By reading DataRow values into a
   * {@code byte[][]} per row (like pgjdbc's Tuple), we allocate only what's needed: one small array
   * per column (~200 bytes total for a typical row). This reduces allocation pressure by ~80× for
   * single-row results, which dominate prepared-statement workloads.
   *
   * <p>For queries that return many rows, this path still works correctly — it just allocates a
   * {@code byte[][]} per row instead of packing into column-oriented buffers. The column-buffer
   * path (used by the non-cached and pipelined paths) remains better for 100+ row result sets.
   */
  private RowSet readExtendedQueryResponse(ColumnDescriptor @Nullable [] cachedColumns) {
    if (cachedColumns == null) {
      return readExtendedQueryResponse();
    }
    try {
      List<byte[][]> rows = new ArrayList<>();
      int rowsAffected = 0;

      while (true) {
        BackendMessage msg = decoder.readMessage();
        if (msg == BackendMessage.BIND_COMPLETE) {
          continue;
        }
        switch (msg) {
          case BackendMessage.DataRow dr -> rows.add(dr.values());
          case BackendMessage.CommandComplete cc -> rowsAffected = cc.rowsAffected();
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            return new RowSet(PgException.fromErrorResponse(err));
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif -> handleNotification(notif);
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ReadyForQuery rq -> {
            return buildDirectRowSet(cachedColumns, rows, rowsAffected);
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading response", e);
    }
  }

  /**
   * Build a RowSet from direct byte[][] row data (non-ColumnBuffer path). Each row is an
   * independent byte[][] with one byte[] per column value. Used for the lightweight cache-hit
   * response path.
   */
  private @NonNull RowSet buildDirectRowSet(
      @NonNull ColumnDescriptor[] columns, @NonNull List<byte[][]> rows, int rowsAffected) {
    Map<String, Integer> nameIndex = Row.buildColumnNameIndex(columns);
    List<Row> rowList = new ArrayList<>(rows.size());
    for (byte[][] values : rows) {
      rowList.add(createRow(columns, nameIndex, values));
    }
    return new RowSet(rowList, List.of(columns), rowsAffected);
  }

  private RowSet readExtendedQueryResponse() {
    try {
      ColumnDescriptor[] columns = null;
      ColumnBuffer[] buffers = null;
      int rowCount = 0;
      int rowsAffected = 0;

      while (true) {
        // Use readMessageIntoBuffers to write DataRow values directly into ColumnBuffers,
        // skipping the intermediate byte[][] allocation per row.
        BackendMessage msg = decoder.readMessageIntoBuffers(buffers);
        // Reference equality for high-frequency singletons — avoids pattern-matching overhead
        // on the hottest path of query response decoding.
        if (msg == BackendMessage.DATA_ROW_SENTINEL) {
          rowCount++;
          continue;
        }
        if (msg == BackendMessage.PARSE_COMPLETE
            || msg == BackendMessage.BIND_COMPLETE
            || msg == BackendMessage.NO_DATA) {
          continue;
        }
        switch (msg) {
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            buffers = newColumnBuffers(columns.length);
          }
          case BackendMessage.DataRow dr -> {
            // Fallback for DataRow before RowDescription (buffers not yet allocated)
            appendToBuffers(buffers, dr.values());
            rowCount++;
          }
          case BackendMessage.CommandComplete cc -> rowsAffected = cc.rowsAffected();
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.PortalSuspended ps -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            return new RowSet(PgException.fromErrorResponse(err));
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif -> handleNotification(notif);
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ReadyForQuery rq -> {
            return buildRowSet(columns, buffers, rowCount, rowsAffected);
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading response", e);
    }
  }

  @Override
  protected void closeCachedStatement(PreparedStatementCache.CachedStatement stmt) {
    try {
      encoder.writeCloseStatement(stmt.pgStatementName());
      encoder.writeSync();
      encoder.flush(out);
      drainUntilReady();
    } catch (IOException e) {
      // best effort
    }
  }

  /** Check if an error indicates a stale cached plan that should trigger re-preparation. */
  private static boolean isStalePlanError(DbException error) {
    String code = error.sqlState();
    String msg = error.getMessage();
    if (code == null || msg == null) return false;
    // "cached plan must not change result type" (0A000)
    if ("0A000".equals(code) && msg.contains("cached plan")) return true;
    // "prepared statement X does not exist" (26000)
    if ("26000".equals(code)) return true;
    return false;
  }

  @Override
  protected void executeExtendedQueryStreaming(
      @NonNull String sql, @NonNull Object[] params, @NonNull Consumer<Row> consumer) {
    params = applyEncoders(params);
    encoder.writeParse(sql, computeParamTypeOIDs(params));
    encoder.writeBindInline(
        PgEncoder.EMPTY_CSTRING, PgEncoder.EMPTY_CSTRING, params, textEncoder());
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    flushToNetwork();
    readExtendedQueryStreaming(consumer);
  }

  private void readExtendedQueryStreaming(Consumer<Row> consumer) {
    try {
      ColumnDescriptor[] columns = null;
      Map<String, Integer> columnNameIndex = null;
      Row recyclableRow = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.ParseComplete pc -> {}
          case BackendMessage.BindComplete bc -> {}
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            columnNameIndex = Row.buildColumnNameIndex(columns);
          }
          case BackendMessage.NoData nd -> {}
          case BackendMessage.DataRow dr -> {
            if (recyclableRow == null) {
              recyclableRow = createRow(columns, columnNameIndex, dr.values());
            } else {
              recyclableRow.resetForStreaming(dr.values());
            }
            consumer.accept(recyclableRow);
          }
          case BackendMessage.CommandComplete cc -> {}
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.PortalSuspended ps -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif -> handleNotification(notif);
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ReadyForQuery rq -> {
            return;
          }
          default -> {}
        }
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading streaming response", e);
    }
  }

  @Override
  protected @NonNull RowStream createExtendedQueryRowStream(
      @NonNull String sql, @NonNull Object[] params) {
    params = applyEncoders(params);
    encoder.writeParse(sql, computeParamTypeOIDs(params));
    encoder.writeBindInline(
        PgEncoder.EMPTY_CSTRING, PgEncoder.EMPTY_CSTRING, params, textEncoder());
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    flushToNetwork();

    var columns = new ColumnDescriptor[1][];
    @SuppressWarnings({"unchecked", "rawtypes"}) // generic array creation for closure capture
    Map<String, Integer>[] columnNameIndex = new Map[1];
    var exhausted = new boolean[] {false};

    return new RowStream(
        () -> {
          if (exhausted[0]) {
            return null;
          }
          try {
            while (true) {
              BackendMessage msg = decoder.readMessage();
              switch (msg) {
                case BackendMessage.ParseComplete pc -> {}
                case BackendMessage.BindComplete bc -> {}
                case BackendMessage.RowDescription rd -> {
                  columns[0] = rd.columns();
                  columnNameIndex[0] = Row.buildColumnNameIndex(rd.columns());
                }
                case BackendMessage.NoData nd -> {}
                case BackendMessage.DataRow dr -> {
                  return createRow(columns[0], columnNameIndex[0], dr.values());
                }
                case BackendMessage.CommandComplete cc -> {}
                case BackendMessage.EmptyQueryResponse eq -> {}
                case BackendMessage.PortalSuspended ps -> {}
                case BackendMessage.ErrorResponse err -> {
                  drainUntilReady();
                  exhausted[0] = true;
                  throw PgException.fromErrorResponse(err);
                }
                case BackendMessage.NoticeResponse notice -> {}
                case BackendMessage.NotificationResponse notif -> handleNotification(notif);
                case BackendMessage.ReadyForQuery rq -> {
                  exhausted[0] = true;
                  return null;
                }
                default -> {}
              }
            }
          } catch (IOException e) {
            exhausted[0] = true;
            throw new DbConnectionException("I/O error reading streaming response", e);
          }
        },
        () -> {
          if (!exhausted[0]) {
            try {
              drainUntilReady();
            } catch (IOException e) {
              // best effort drain on close
            }
            exhausted[0] = true;
          }
        });
  }

  @Override
  protected void sendTerminate() {
    try {
      encoder.writeTerminate();
      encoder.flush(out);
    } catch (IOException e) {
      // best effort
    }
  }

  @Override
  protected void closeTransport() {
    try {
      socket.close();
    } catch (IOException e) {
      // best effort
    }
  }

  // --- PG-specific startup/auth ---

  private void performStartup(
      String user, String database, String password, Map<String, String> properties)
      throws IOException {
    encoder.writeStartupMessage(user, database, properties);
    encoder.flush(out);

    boolean done = false;
    while (!done) {
      BackendMessage msg = decoder.readMessage();
      switch (msg) {
        case BackendMessage.AuthenticationOk ok -> {}
        case BackendMessage.AuthenticationCleartextPassword cleartext -> {
          encoder.writePasswordMessage(password);
          encoder.flush(out);
        }
        case BackendMessage.AuthenticationMD5Password md5 -> {
          String hash = MD5Authentication.encode(user, password, md5.salt());
          encoder.writePasswordMessage(hash);
          encoder.flush(out);
        }
        case AuthenticationSasl sasl -> handleSaslAuth(sasl, user, password);
        case BackendMessage.ParameterStatus ps -> parameters.put(ps.name(), ps.value());
        case BackendMessage.BackendKeyData kd -> {
          processId = kd.processId();
          secretKey = kd.secretKey();
        }
        case BackendMessage.ReadyForQuery rq -> {
          String encoding = parameters.get("client_encoding");
          if (encoding != null && !encoding.equalsIgnoreCase("UTF8")) {
            throw new PgException(
                "FATAL", "08000", encoding + " encoding is not supported, only UTF8", null, null);
          }
          done = true;
        }
        case BackendMessage.ErrorResponse err -> throw PgException.fromErrorResponse(err);
        case BackendMessage.NoticeResponse notice -> {}
        default ->
            throw new DbConnectionException(
                "Unexpected message during startup: " + msg.getClass().getSimpleName());
      }
    }
  }

  private void handleSaslAuth(AuthenticationSasl sasl, String user, String password)
      throws IOException {
    try {
      var scramClient =
          com.ongres.scram.client.ScramClient.builder()
              .advertisedMechanisms(List.of(sasl.mechanisms().split(",")))
              .username(user)
              .password(password.toCharArray())
              .build();

      var clientFirstMsg = scramClient.clientFirstMessage();
      encoder.writeScramInitialMessage(
          scramClient.getScramMechanism().getName(), clientFirstMsg.toString());
      encoder.flush(out);

      BackendMessage msg = decoder.readMessage();
      if (msg instanceof BackendMessage.ErrorResponse err) {
        throw PgException.fromErrorResponse(err);
      }
      if (!(msg instanceof AuthenticationSaslContinue(byte[] cont))) {
        throw new DbConnectionException("Expected AuthenticationSASLContinue, got: " + msg);
      }
      scramClient.serverFirstMessage(new String(cont, StandardCharsets.UTF_8));
      var clientFinalMsg = scramClient.clientFinalMessage();

      encoder.writeScramFinalMessage(clientFinalMsg.toString());
      encoder.flush(out);

      msg = decoder.readMessage();
      if (msg instanceof BackendMessage.ErrorResponse err) {
        throw PgException.fromErrorResponse(err);
      }
      if (!(msg instanceof AuthenticationSaslFinal(byte[] data))) {
        throw new DbConnectionException("Expected AuthenticationSASLFinal, got: " + msg);
      }
      scramClient.serverFinalMessage(new String(data, StandardCharsets.UTF_8));
    } catch (com.ongres.scram.common.exception.ScramException e) {
      throw new PgException(
          "FATAL", "28P01", "SCRAM authentication failed: " + e.getMessage(), null, null);
    }
  }

  @Override
  protected @NonNull BinaryCodec binaryCodec() {
    return PgBinaryCodec.INSTANCE;
  }

  /**
   * Handle a ParameterStatus message received during query execution. Updates the parameters map.
   */
  private void handleParameterStatus(BackendMessage.ParameterStatus ps) {
    parameters.put(ps.name(), ps.value());
    parametersView = null; // invalidate cached unmodifiable view
  }

  private void handleNotification(BackendMessage.NotificationResponse notif) {
    notifications.add(new PgNotification(notif.processId(), notif.channel(), notif.payload()));
  }

  /**
   * Invalidate all cached prepared statements. Called when server-side state changes (e.g.
   * search_path) that could cause cached statements to resolve differently.
   */
  private void invalidatePreparedStatementCache() {
    if (psCache != null && !psCache.isEmpty()) {
      for (var stmt : psCache.values()) {
        closeCachedStatement(stmt);
      }
      psCache.clear();
    }
  }

  // --- Package-private accessors for PgCursor ---

  PgEncoder encoder() {
    return encoder;
  }

  PgDecoder decoder() {
    return decoder;
  }

  OutputStream out() {
    return out;
  }

  int maxResultBufferBytes() {
    return maxResultBufferBytes;
  }

  static void appendToColumnBuffers(ColumnBuffer[] buffers, byte[][] values) {
    appendToBuffers(buffers, values);
  }

  static ColumnBuffer[] createColumnBuffers(
      int columnCount, int initialRows, int estimatedAvgSize) {
    return newColumnBuffers(columnCount, initialRows, estimatedAvgSize);
  }

  @NonNull RowSet buildCursorRowSet(
      ColumnDescriptor @Nullable [] columns,
      @NonNull ColumnBuffer[] buffers,
      int rowCount,
      int rowsAffected) {
    return buildRowSet(columns, buffers, rowCount, rowsAffected);
  }

  void drainUntilReady() throws IOException {
    while (true) {
      BackendMessage msg = decoder.readMessage();
      if (msg instanceof BackendMessage.ReadyForQuery) {
        return;
      }
      if (msg instanceof BackendMessage.ParameterStatus ps) {
        handleParameterStatus(ps);
      }
    }
  }
}
