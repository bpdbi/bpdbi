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
import io.github.bpdbi.pg.impl.auth.MD5Authentication;
import io.github.bpdbi.pg.impl.codec.BackendMessage;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSasl;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslContinue;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslFinal;
import io.github.bpdbi.pg.impl.codec.PgBinaryCodec;
import io.github.bpdbi.pg.impl.codec.PgDecoder;
import io.github.bpdbi.pg.impl.codec.PgEncoder;
import java.io.BufferedInputStream;
import io.github.bpdbi.core.impl.UnsyncBufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
  private final List<PgNotification> notifications = new ArrayList<>();
  private int processId;
  private int secretKey;
  private int stmtCounter = 0;
  private int deallocateEpoch = 0;

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
        socket = upgradeToSsl(socket, config);
      }

      var out = new UnsyncBufferedOutputStream(socket.getOutputStream(), 8192);
      var in = new BufferedInputStream(socket.getInputStream(), 8192);
      var encoder = new PgEncoder();
      var decoder = new PgDecoder(in);

      var conn = new PgConnection(socket, out, encoder, decoder);
      conn.initCache(config);
      conn.performStartup(
          config.username(), config.database(), config.password(), config.properties());
      return conn;
    } catch (IOException e) {
      throw new DbConnectionException(
          "Failed to connect to Postgres at " + config.host() + ":" + config.port(), e);
    }
  }

  /**
   * Upgrade a plain socket to SSL/TLS using the Postgres SSLRequest protocol. Sends the 8-byte
   * SSLRequest message, reads the server's single-byte response, and wraps the socket in an
   * SSLSocket if the server accepts.
   */
  private static Socket upgradeToSsl(Socket socket, ConnectionConfig config) throws IOException {
    SslMode sslMode = config.sslMode();
    String host = config.host();
    int port = config.port() > 0 ? config.port() : 5432;

    // Send SSLRequest (not a normal startup message — no type byte)
    var encoder = new PgEncoder();
    encoder.writeSSLRequest();
    encoder.flush(socket.getOutputStream());

    // Read single byte response: 'S' = upgrade, 'N' = no SSL
    int response = socket.getInputStream().read();
    if (response == 'S') {
      try {
        SSLContext ctx = buildSslContext(config);
        SSLSocketFactory factory = ctx.getSocketFactory();
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, host, port, true);
        sslSocket.setUseClientMode(true);

        // Enable hostname verification for VERIFY_FULL
        if (sslMode == SslMode.VERIFY_FULL || config.hostnameVerification()) {
          SSLParameters sslParams = sslSocket.getSSLParameters();
          sslParams.setEndpointIdentificationAlgorithm("HTTPS");
          sslSocket.setSSLParameters(sslParams);
        }

        sslSocket.startHandshake();
        return sslSocket;
      } catch (Exception e) {
        throw new IOException("SSL handshake failed", e);
      }
    } else if (response == 'N') {
      if (sslMode == SslMode.REQUIRE
          || sslMode == SslMode.VERIFY_CA
          || sslMode == SslMode.VERIFY_FULL) {
        socket.close();
        throw new IOException("Server does not support SSL but sslMode=" + sslMode);
      }
      // PREFER: fall back to non-SSL
      return socket;
    } else {
      socket.close();
      throw new IOException("Unexpected SSL response: " + (char) response);
    }
  }

  /**
   * Build an SSLContext based on the connection configuration.
   *
   * <p>Priority order:
   *
   * <ol>
   *   <li>User-provided {@link ConnectionConfig#sslContext()} — used as-is
   *   <li>PEM certificate file via {@link ConnectionConfig#pemCertPath()}
   *   <li>JKS trust store via {@link ConnectionConfig#trustStorePath()}
   *   <li>For VERIFY_CA/VERIFY_FULL — system default trust store
   *   <li>For REQUIRE/PREFER — trust-all (no certificate verification)
   * </ol>
   */
  private static SSLContext buildSslContext(ConnectionConfig config) {
    // 1. User-provided SSLContext
    if (config.sslContext() != null) {
      return config.sslContext();
    }

    try {
      SSLContext ctx = SSLContext.getInstance("TLS");

      // 2. PEM certificate file
      if (config.pemCertPath() != null) {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream certIn = new FileInputStream(config.pemCertPath())) {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          int i = 0;
          for (var cert : cf.generateCertificates(certIn)) {
            ks.setCertificateEntry("cert-" + (i++), cert);
          }
        }
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 3. JKS trust store
      if (config.trustStorePath() != null) {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] pass =
            config.trustStorePassword() != null ? config.trustStorePassword().toCharArray() : null;
        try (InputStream tsIn = new FileInputStream(config.trustStorePath())) {
          ks.load(tsIn, pass);
        }
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 4. For VERIFY_CA/VERIFY_FULL — use system default trust store
      if (config.sslMode() == SslMode.VERIFY_CA || config.sslMode() == SslMode.VERIFY_FULL) {
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // uses default system trust store
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 5. REQUIRE/PREFER — trust-all (no certificate verification)
      ctx.init(
          null,
          new TrustManager[] {
            new X509TrustManager() {
              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(X509Certificate[] certs, String authType) {}

              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
              }
            }
          },
          null);
      return ctx;
    } catch (Exception e) {
      throw new DbConnectionException("Failed to create SSL context", e);
    }
  }

  @Override
  public void ping() {
    query("");
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    return Collections.unmodifiableMap(parameters);
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

    String stmtName = "_djb_s" + (stmtCounter++);
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
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.ReadyForQuery rq -> {
            if (isCacheable(sql)) {
              var cached =
                  new PreparedStatementCache.CachedStatement(
                      sql, stmtName, -1, columns, null, paramNames);
              handleEvicted(psCache.cache(sql, cached));
              return new CachedPgPreparedStatement(cached);
            }
            return new PgPreparedStatement(stmtName, columns, paramNames);
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
    String[] textParams = null;
    if (params != null && params.length > 0) {
      textParams = encodeParams(params);
    }
    try {
      // Parse unnamed + Bind into named portal + Describe portal + Sync
      encoder.writeParse("", sql, null);
      encoder.writeBind(portalName, "", textParams);
      encoder.writeDescribePortal(portalName);
      encoder.writeSync();
      encoder.flush(out);

      ColumnDescriptor[] columns = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.RowDescription rd -> columns = rd.columns();
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.ReadyForQuery rq -> {
            return new PgCursor(portalName, columns);
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

  /**
   * Convert resolved named params for use with Postgres prepared statements. Collection/array
   * values are converted to Postgres array literal format ({v1,v2,...}) so they can be used with
   * {@code = ANY(:param)} syntax.
   */
  private static Object[] convertPgParams(Object[] params) {
    for (int i = 0; i < params.length; i++) {
      params[i] = toPgValue(params[i]);
    }
    return params;
  }

  private static Object toPgValue(Object value) {
    if (value instanceof Collection<?> c) {
      return toPgArrayLiteral(c);
    }
    if (value != null && value.getClass().isArray()) {
      int len = java.lang.reflect.Array.getLength(value);
      var list = new ArrayList<>(len);
      for (int i = 0; i < len; i++) {
        list.add(java.lang.reflect.Array.get(value, i));
      }
      return toPgArrayLiteral(list);
    }
    return value;
  }

  /**
   * Format a collection as a Postgres array literal: {v1,v2,v3}. Values containing special
   * characters are double-quoted.
   */
  private static String toPgArrayLiteral(Collection<?> elements) {
    var sb = new StringBuilder();
    sb.append('{');
    boolean first = true;
    for (Object element : elements) {
      if (!first) {
        sb.append(',');
      }
      if (element == null) {
        sb.append("NULL");
      } else {
        String s = element.toString();
        if (needsPgArrayQuoting(s)) {
          sb.append('"');
          for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
              sb.append('\\');
            }
            sb.append(c);
          }
          sb.append('"');
        } else {
          sb.append(s);
        }
      }
      first = false;
    }
    sb.append('}');
    return sb.toString();
  }

  private static boolean needsPgArrayQuoting(String s) {
    if (s.isEmpty()) {
      return true;
    }
    if (s.equalsIgnoreCase("NULL")) {
      return true;
    }
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' || c == ' ') {
        return true;
      }
    }
    return false;
  }

  // --- Inner classes for prepared statements and cursors ---

  private final class PgPreparedStatement implements PreparedStatement {

    private final String name;

    // Retained from the server's RowDescription response during prepare();
    // not currently used but available for future column metadata access on prepared statements.
    @SuppressWarnings("unused") // kept for future PreparedStatement.getColumns() API
    private final ColumnDescriptor[] columns;

    private final List<String> parameterNames;
    private boolean closed = false;

    PgPreparedStatement(String name, ColumnDescriptor[] columns, List<String> parameterNames) {
      this.name = name;
      this.columns = columns;
      this.parameterNames = parameterNames;
    }

    @Override
    public @NonNull RowSet query(Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      String[] textParams = null;
      if (params != null && params.length > 0) {
        textParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
          textParams[i] = params[i] == null ? null : params[i].toString();
        }
      }
      try {
        encoder.writeBind("", name, textParams);
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
        encoder.flush(out);
        return readExtendedQueryResponse();
      } catch (IOException e) {
        throw new DbConnectionException("I/O error executing prepared statement", e);
      }
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (parameterNames == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      return query(convertPgParams(NamedParamParser.resolveParams(parameterNames, params)));
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
    public @NonNull RowSet query(Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      String[] textParams = null;
      if (params != null && params.length > 0) {
        textParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
          textParams[i] = params[i] == null ? null : params[i].toString();
        }
      }
      try {
        encoder.writeBind("", cached.pgStatementName(), textParams);
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
        encoder.flush(out);
        return readExtendedQueryResponse();
      } catch (IOException e) {
        throw new DbConnectionException("I/O error executing prepared statement", e);
      }
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (cached.parameterNames() == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      return query(
          convertPgParams(NamedParamParser.resolveParams(cached.parameterNames(), params)));
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

  private final class PgCursor implements Cursor {

    private final String portalName;
    private final ColumnDescriptor[] columns;
    private boolean hasMore = true;
    private boolean closed = false;
    // Reusable column buffers — sized from the previous batch's observed data
    private ColumnBuffer @Nullable [] prevBuffers;

    PgCursor(String portalName, ColumnDescriptor[] columns) {
      this.portalName = portalName;
      this.columns = columns;
    }

    @Override
    public @NonNull RowSet read(int count) {
      if (closed) {
        throw new IllegalStateException("Cursor is closed");
      }
      if (!hasMore) {
        return new RowSet(List.of(), columns != null ? List.of(columns) : List.of(), 0);
      }
      try {
        encoder.writeExecute(portalName, count);
        encoder.writeSync();
        encoder.flush(out);

        ColumnBuffer[] buffers = createOrReuseBuffers(count);
        int rowCount = 0;
        int rowsAffected = 0;
        while (true) {
          BackendMessage msg = decoder.readMessage();
          switch (msg) {
            case BackendMessage.DataRow dr -> {
              appendToBuffers(buffers, dr.values());
              rowCount++;
            }
            case BackendMessage.CommandComplete cc -> {
              rowsAffected = cc.rowsAffected();
              hasMore = false;
            }
            case BackendMessage.PortalSuspended ps -> hasMore = true;
            case BackendMessage.ErrorResponse err -> {
              drainUntilReady();
              throw PgException.fromErrorResponse(err);
            }
            case BackendMessage.ReadyForQuery rq -> {
              prevBuffers = buffers;
              return buildRowSet(columns, buffers, rowCount, rowsAffected);
            }
            default -> {}
          }
        }
      } catch (IOException e) {
        throw new DbConnectionException("I/O error reading cursor", e);
      }
    }

    private ColumnBuffer[] createOrReuseBuffers(int expectedRows) {
      if (columns == null) {
        return new ColumnBuffer[0];
      }
      ColumnBuffer[] prev = this.prevBuffers;
      if (prev != null && prev.length > 0) {
        // Size new buffers based on observed average from last batch
        int avgSize = Math.max(prev[0].averageValueSize(), 8);
        return newColumnBuffers(columns.length, expectedRows, avgSize);
      }
      return newColumnBuffers(columns.length, expectedRows, 32);
    }

    @Override
    public boolean hasMore() {
      return hasMore && !closed;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        hasMore = false;
        try {
          encoder.writeClosePortal(portalName);
          encoder.writeSync();
          encoder.flush(out);
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

  // --- BaseConnection protocol methods ---

  @Override
  protected @NonNull String placeholderPrefix() {
    return "$";
  }

  @Override
  protected @NonNull RowSet executeExtendedQuery(@NonNull String sql, @NonNull String[] params) {
    // Parameterless queries use the simple query protocol (text-format results) because:
    // 1. The extended protocol rejects multi-statement strings ("SELECT 1; SELECT 2")
    // 2. Text format lets getString() work for types without a binary decoder (geometric,
    //    network, array, interval) — binary format for these is raw packed bytes, not UTF-8
    if (params.length == 0) {
      return executeSimpleQuery(sql);
    }

    if (isCacheable(sql)) {
      var cached = psCache.get(sql);
      if (cached != null) {
        // Cache hit: skip Parse, bind to cached named statement
        encoder.writeBind("", cached.pgStatementName(), params);
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
        try {
          encoder.flush(out);
        } catch (IOException e) {
          throw new DbConnectionException("I/O error during flush", e);
        }
        return readExtendedQueryResponse();
      } else {
        // Cache miss: use named statement so we can cache it
        String stmtName = "_djb_s" + (stmtCounter++);
        encoder.writeParse(stmtName, sql, null);
        encoder.writeBind("", stmtName, params);
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
        try {
          encoder.flush(out);
        } catch (IOException e) {
          throw new DbConnectionException("I/O error during flush", e);
        }
        RowSet result = readExtendedQueryResponse();
        // Cache after successful execution
        if (result.getError() == null) {
          var stmt = new PreparedStatementCache.CachedStatement(sql, stmtName, -1, null, null);
          handleEvicted(psCache.cache(sql, stmt));
        }
        return result;
      }
    }

    // No caching: use unnamed statement (current behavior)
    encoder.writeParse(sql, null);
    encoder.writeBind(params);
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
    return readExtendedQueryResponse();
  }

  // --- Pipelined batch execution ---

  @Override
  protected @NonNull List<RowSet> executePipelinedBatch(
      @NonNull List<PendingStatement> statements) {
    if (statements.size() <= 1) {
      return super.executePipelinedBatch(statements);
    }

    // Fall back to sequential if any statement is parameterless (uses simple protocol)
    for (PendingStatement stmt : statements) {
      if (stmt.paramValues().length == 0) {
        return super.executePipelinedBatch(statements);
      }
    }

    // Pre-size the encoder buffer to avoid resizing during batch encoding
    int estimatedBytes = 5; // Sync message at the end
    for (PendingStatement stmt : statements) {
      estimatedBytes += PgEncoder.estimateExtendedQuerySize(stmt.sql(), stmt.paramValues());
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

    // Phase 1: Write messages, flushing when response estimate gets risky
    String lastParsedSql = null;
    for (int i = 0; i < statements.size(); i++) {
      PendingStatement stmt = statements.get(i);
      estimatedResponseBytes += ESTIMATED_RESPONSE_BYTES_PER_QUERY;

      if (!stmt.sql().equals(lastParsedSql)) {
        encoder.writeParse(stmt.sql(), null);
        lastParsedSql = stmt.sql();
      }
      encoder.writeBind(stmt.paramValues());
      encoder.writeDescribePortal();
      encoder.writeExecute();

      // Flush and drain mid-pipeline if approaching TCP buffer limit
      if (estimatedResponseBytes >= MAX_BUFFERED_RECV_BYTES
          && i < statements.size() - 1) {
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
      ColumnDescriptor[] columns = null;
      ColumnBuffer[] buffers = null;
      int rowCount = 0;
      int rowsAffected = 0;

      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.ParseComplete pc -> {}
          case BackendMessage.BindComplete bc -> {}
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            buffers = newColumnBuffers(columns.length);
          }
          case BackendMessage.NoData nd -> {}
          case BackendMessage.DataRow dr -> {
            appendToBuffers(buffers, dr.values());
            rowCount++;
          }
          case BackendMessage.CommandComplete cc -> {
            rowsAffected = cc.rowsAffected();
            results.add(buildRowSet(columns, buffers, rowCount, rowsAffected));
            // Reset accumulators for next statement
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
          case BackendMessage.NotificationResponse notif ->
              notifications.add(
                  new PgNotification(notif.processId(), notif.channel(), notif.payload()));
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

  // --- Simple query protocol (parameterless queries, text format results) ---

  private RowSet executeSimpleQuery(String sql) {
    // Detect statements that may invalidate cached prepared statements.
    // search_path changes cause table/function names to resolve differently,
    // so cached Parse results may become stale. DEALLOCATE ALL and DISCARD ALL
    // explicitly destroy all prepared statements. Like pgjdbc, we detect this
    // by inspecting the SQL text since Postgres does not send ParameterStatus
    // for search_path changes.
    if (psCache != null && !psCache.isEmpty()) {
      if ((sql.regionMatches(true, 0, "SET", 0, 3)
              && containsIgnoreCase(sql, "search_path"))
          || sql.regionMatches(true, 0, "DEALLOCATE", 0, 10)
          || sql.regionMatches(true, 0, "DISCARD", 0, 7)) {
        invalidatePreparedStatementCache();
      }
    }

    encoder.writeQuery(sql);
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
    return readSimpleQueryResponse();
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

  private RowSet readSimpleQueryResponse() {
    try {
      ColumnDescriptor[] columns = null;
      ColumnBuffer[] buffers = null;
      int rowCount = 0;
      int rowsAffected = 0;

      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            buffers = newColumnBuffers(columns.length);
          }
          case BackendMessage.DataRow dr -> {
            appendToBuffers(buffers, dr.values());
            rowCount++;
          }
          case BackendMessage.CommandComplete cc -> rowsAffected = cc.rowsAffected();
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            return new RowSet(PgException.fromErrorResponse(err));
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif ->
              notifications.add(
                  new PgNotification(notif.processId(), notif.channel(), notif.payload()));
          case BackendMessage.ParameterStatus ps -> handleParameterStatus(ps);
          case BackendMessage.ReadyForQuery rq -> {
            return buildTextRowSet(columns, buffers, rowCount, rowsAffected);
          }
          default -> {}
}
      }
    } catch (IOException e) {
      throw new DbConnectionException("I/O error reading response", e);
    }
  }

  private RowSet buildTextRowSet(
      ColumnDescriptor[] columns, ColumnBuffer[] buffers, int rowCount, int rowsAffected) {
    if (columns == null) {
      return new RowSet(List.of(), List.of(), rowsAffected);
    }
    List<Row> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(createTextBufferedRow(columns, buffers, i));
    }
    return new RowSet(rows, List.of(columns), rowsAffected);
  }

  private void readSimpleQueryStreaming(Consumer<Row> consumer) {
    try {
      ColumnDescriptor[] columns = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.RowDescription rd -> columns = rd.columns();
          case BackendMessage.DataRow dr -> consumer.accept(createTextRow(columns, dr.values()));
          case BackendMessage.CommandComplete cc -> {}
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif ->
              notifications.add(
                  new PgNotification(notif.processId(), notif.channel(), notif.payload()));
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

  private RowStream createSimpleQueryRowStream() {
    var columns = new ColumnDescriptor[1][];
    var exhausted = new AtomicBoolean(false);

    return new RowStream(
        () -> {
          if (exhausted.get()) {
            return null;
          }
          try {
            while (true) {
              BackendMessage msg = decoder.readMessage();
              switch (msg) {
                case BackendMessage.RowDescription rd -> columns[0] = rd.columns();
                case BackendMessage.DataRow dr -> {
                  return createTextRow(columns[0], dr.values());
                }
                case BackendMessage.CommandComplete cc -> {}
                case BackendMessage.EmptyQueryResponse eq -> {}
                case BackendMessage.ErrorResponse err -> {
                  drainUntilReady();
                  exhausted.set(true);
                  throw PgException.fromErrorResponse(err);
                }
                case BackendMessage.NoticeResponse notice -> {}
                case BackendMessage.NotificationResponse notif ->
                    notifications.add(
                        new PgNotification(notif.processId(), notif.channel(), notif.payload()));
                case BackendMessage.ReadyForQuery rq -> {
                  exhausted.set(true);
                  return null;
                }
                default -> {}
              }
            }
          } catch (IOException e) {
            exhausted.set(true);
            throw new DbConnectionException("I/O error reading streaming response", e);
          }
        },
        () -> {
          if (!exhausted.get()) {
            try {
              drainUntilReady();
            } catch (IOException e) {
              // best effort drain on close
            }
            exhausted.set(true);
          }
        });
  }

  @Override
  protected void flushToNetwork() {
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
  }

  private RowSet readExtendedQueryResponse() {
    try {
      ColumnDescriptor[] columns = null;
      ColumnBuffer[] buffers = null;
      int rowCount = 0;
      int rowsAffected = 0;

      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.ParseComplete pc -> {}
          case BackendMessage.BindComplete bc -> {}
          case BackendMessage.RowDescription rd -> {
            columns = rd.columns();
            buffers = newColumnBuffers(columns.length);
          }
          case BackendMessage.NoData nd -> {}
          case BackendMessage.DataRow dr -> {
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
          case BackendMessage.NotificationResponse notif ->
              notifications.add(
                  new PgNotification(notif.processId(), notif.channel(), notif.payload()));
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

  @Override
  protected void executeExtendedQueryStreaming(
      @NonNull String sql, @NonNull String[] params, @NonNull Consumer<Row> consumer) {
    if (params.length == 0) {
      encoder.writeQuery(sql);
      try {
        encoder.flush(out);
      } catch (IOException e) {
        throw new DbConnectionException("I/O error during flush", e);
      }
      readSimpleQueryStreaming(consumer);
      return;
    }

    // Encode extended query, then read streaming
    encoder.writeParse(sql, null);
    encoder.writeBind(params);
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
    readExtendedQueryStreaming(consumer);
  }

  private void readExtendedQueryStreaming(Consumer<Row> consumer) {
    try {
      ColumnDescriptor[] columns = null;
      while (true) {
        BackendMessage msg = decoder.readMessage();
        switch (msg) {
          case BackendMessage.ParseComplete pc -> {}
          case BackendMessage.BindComplete bc -> {}
          case BackendMessage.RowDescription rd -> columns = rd.columns();
          case BackendMessage.NoData nd -> {}
          case BackendMessage.DataRow dr -> consumer.accept(createRow(columns, dr.values()));
          case BackendMessage.CommandComplete cc -> {}
          case BackendMessage.EmptyQueryResponse eq -> {}
          case BackendMessage.PortalSuspended ps -> {}
          case BackendMessage.ErrorResponse err -> {
            drainUntilReady();
            throw PgException.fromErrorResponse(err);
          }
          case BackendMessage.NoticeResponse notice -> {}
          case BackendMessage.NotificationResponse notif ->
              notifications.add(
                  new PgNotification(notif.processId(), notif.channel(), notif.payload()));
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
      @NonNull String sql, @NonNull String[] params) {
    if (params.length == 0) {
      encoder.writeQuery(sql);
      try {
        encoder.flush(out);
      } catch (IOException e) {
        throw new DbConnectionException("I/O error during flush", e);
      }
      return createSimpleQueryRowStream();
    }

    // Send extended query protocol messages
    encoder.writeParse(sql, null);
    encoder.writeBind(params);
    encoder.writeDescribePortal();
    encoder.writeExecute();
    encoder.writeSync();
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }

    var columns = new ColumnDescriptor[1][];
    var exhausted = new AtomicBoolean(false);

    return new RowStream(
        () -> {
          if (exhausted.get()) {
            return null;
          }
          try {
            while (true) {
              BackendMessage msg = decoder.readMessage();
              switch (msg) {
                case BackendMessage.ParseComplete pc -> {}
                case BackendMessage.BindComplete bc -> {}
                case BackendMessage.RowDescription rd -> columns[0] = rd.columns();
                case BackendMessage.NoData nd -> {}
                case BackendMessage.DataRow dr -> {
                  return createRow(columns[0], dr.values());
                }
                case BackendMessage.CommandComplete cc -> {}
                case BackendMessage.EmptyQueryResponse eq -> {}
                case BackendMessage.PortalSuspended ps -> {}
                case BackendMessage.ErrorResponse err -> {
                  drainUntilReady();
                  exhausted.set(true);
                  throw PgException.fromErrorResponse(err);
                }
                case BackendMessage.NoticeResponse notice -> {}
                case BackendMessage.NotificationResponse notif ->
                    notifications.add(
                        new PgNotification(notif.processId(), notif.channel(), notif.payload()));
                case BackendMessage.ReadyForQuery rq -> {
                  exhausted.set(true);
                  return null;
                }
                default -> {}
              }
            }
          } catch (IOException e) {
            exhausted.set(true);
            throw new DbConnectionException("I/O error reading streaming response", e);
          }
        },
        () -> {
          if (!exhausted.get()) {
            try {
              drainUntilReady();
            } catch (IOException e) {
              // best effort drain on close
            }
            exhausted.set(true);
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
  }

  /**
   * Invalidate all cached prepared statements. Called when server-side state changes (e.g.
   * search_path) that could cause cached statements to resolve differently.
   */
  private void invalidatePreparedStatementCache() {
    if (psCache != null && !psCache.isEmpty()) {
      deallocateEpoch++;
      handleEvicted(new ArrayList<>(psCache.values()));
      psCache.clear();
    }
  }

  /** Return the current deallocate epoch (for testing). */
  int deallocateEpoch() {
    return deallocateEpoch;
  }

  private void drainUntilReady() throws IOException {
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
