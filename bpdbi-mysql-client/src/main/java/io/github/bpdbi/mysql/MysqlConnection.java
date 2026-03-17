package io.github.bpdbi.mysql;

import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.AUTH_MORE_DATA;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.AUTH_SWITCH_REQUEST;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.CLIENT_CONNECT_WITH_DB;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.CLIENT_DEPRECATE_EOF;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.CLIENT_SSL;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.CLIENT_SUPPORTED_FLAGS;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.EOF_PACKET;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.ERR_PACKET;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.NONCE_LENGTH;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.OK_PACKET;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.PACKET_PAYLOAD_LIMIT;
import static io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants.SERVER_MORE_RESULTS_EXISTS;

import io.github.bpdbi.core.BinaryCodec;
import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.Cursor;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.PreparedStatement;
import io.github.bpdbi.core.Row;
import io.github.bpdbi.core.RowSet;
import io.github.bpdbi.core.RowStream;
import io.github.bpdbi.core.SslMode;
import io.github.bpdbi.core.impl.BaseConnection;
import io.github.bpdbi.core.impl.ColumnBuffer;
import io.github.bpdbi.core.impl.NamedParamParser;
import io.github.bpdbi.core.impl.PreparedStatementCache;
import io.github.bpdbi.mysql.impl.auth.CachingSha2Authenticator;
import io.github.bpdbi.mysql.impl.auth.Native41Authenticator;
import io.github.bpdbi.mysql.impl.auth.RsaPublicKeyEncryptor;
import io.github.bpdbi.mysql.impl.codec.MysqlBinaryCodec;
import io.github.bpdbi.mysql.impl.codec.MysqlDecoder;
import io.github.bpdbi.mysql.impl.codec.MysqlEncoder;
import io.github.bpdbi.mysql.impl.codec.MysqlProtocolConstants;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * MySQL implementation of {@link io.github.bpdbi.core.Connection}.
 *
 * <p><b>Not thread-safe.</b> Each connection must be used by a single thread at a time. Designed
 * for one-connection-per-(virtual-)thread usage with Java 21+ virtual threads.
 */
public final class MysqlConnection extends BaseConnection {

  private Socket socket;
  private OutputStream out;
  private final MysqlEncoder encoder;
  private MysqlDecoder decoder;
  private final Map<String, String> parameters = new HashMap<>();
  private final Charset charset = StandardCharsets.UTF_8;
  private int connectionId;
  private byte @Nullable [] authPluginData; // nonce from handshake, needed for RSA full auth
  private SslMode sslMode = SslMode.DISABLE;

  private MysqlConnection(
      Socket socket, OutputStream out, MysqlEncoder encoder, MysqlDecoder decoder) {
    this.socket = socket;
    this.out = out;
    this.encoder = encoder;
    this.decoder = decoder;
  }

  public static @NonNull MysqlConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password) {
    return connect(host, port, database, user, password, null, SslMode.DISABLE);
  }

  public static @NonNull MysqlConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password,
      @Nullable Map<String, String> properties) {
    return connect(host, port, database, user, password, properties, SslMode.DISABLE);
  }

  public static @NonNull MysqlConnection connect(
      @NonNull String host,
      int port,
      @NonNull String database,
      @NonNull String user,
      @NonNull String password,
      @Nullable Map<String, String> properties,
      @NonNull SslMode sslMode) {
    try {
      Socket socket = new Socket(host, port);
      socket.setTcpNoDelay(true);
      var out = new BufferedOutputStream(socket.getOutputStream(), 8192);

      var in = new BufferedInputStream(socket.getInputStream(), 8192);
      var encoder = new MysqlEncoder();
      var decoder = new MysqlDecoder(in);

      var conn = new MysqlConnection(socket, out, encoder, decoder);
      conn.sslMode = sslMode;
      conn.performHandshake(user, password, database);
      return conn;
    } catch (IOException e) {
      throw new DbConnectionException("Failed to connect to MySQL at " + host + ":" + port, e);
    }
  }

  public static @NonNull MysqlConnection connect(@NonNull ConnectionConfig config) {
    var conn =
        connect(
            config.host(),
            config.port() > 0 ? config.port() : 3306,
            config.database(),
            config.username(),
            config.password(),
            config.properties(),
            config.sslMode());
    conn.initCache(config);
    if (config.socketTimeoutMillis() > 0) {
      try {
        conn.socket.setSoTimeout(config.socketTimeoutMillis());
      } catch (java.net.SocketException e) {
        throw new DbConnectionException("Failed to set socket timeout", e);
      }
    }
    return conn;
  }

  @Override
  public @NonNull PreparedStatement prepare(@NonNull String sql) {
    // Parse named parameters if present
    List<String> parameterNames = null;
    String sqlToSend = sql;
    if (NamedParamParser.containsNamedParams(sql)) {
      var template = NamedParamParser.parseTemplate(sql, "?");
      sqlToSend = template.sql();
      parameterNames = template.parameterNames();
    }

    // Check cache (keyed on original SQL)
    if (isCacheable(sql)) {
      var cached = psCache.get(sql);
      if (cached != null) {
        return new CachedMysqlPreparedStmt(cached);
      }
    }

    final List<String> paramNames = parameterNames;
    try {
      // Send COM_STMT_PREPARE
      encoder.resetSequenceId();
      encoder.writeComStmtPrepare(sqlToSend, charset);
      encoder.flush(out);

      // Read prepare response
      byte[] payload = decoder.readPacket();
      if ((payload[0] & 0xFF) == ERR_PACKET) {
        var err = decoder.readErrPacket(payload);
        throw MysqlException.fromErrPacket(err);
      }
      var prepResult = decoder.readPrepareResult(payload);

      // Read and skip parameter definitions
      int[] paramTypes = new int[prepResult.numParams()];
      for (int i = 0; i < prepResult.numParams(); i++) {
        byte[] paramDef = decoder.readPacket();
        // We don't need the param definitions for now
      }
      if (prepResult.numParams() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF after param defs
      }

      // Read and store column definitions
      ColumnDescriptor[] columns = new ColumnDescriptor[prepResult.numColumns()];
      int[] columnTypes = new int[prepResult.numColumns()];
      for (int i = 0; i < prepResult.numColumns(); i++) {
        byte[] colDef = decoder.readPacket();
        columns[i] = decoder.readColumnDefinition(colDef);
        columnTypes[i] = columns[i].typeOID(); // MySQL type code stored in typeOID
      }
      if (prepResult.numColumns() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF after column defs
      }

      if (isCacheable(sql)) {
        var cached =
            new PreparedStatementCache.CachedStatement(
                sql, null, prepResult.statementId(), columns, columnTypes, paramNames);
        handleEvicted(psCache.cache(sql, cached));
        return new CachedMysqlPreparedStmt(cached);
      }
      // Column definitions already consumed above to stay in sync with protocol framing
      return new MysqlPreparedStmt(prepResult.statementId(), paramNames);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error preparing statement", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>MySQL limitation:</b> Unlike Postgres which uses true server-side cursors (portal +
   * FETCH), MySQL's cursor implementation here eagerly fetches all rows into memory and then pages
   * through them client-side. This means memory usage is proportional to the full result set size,
   * not the batch size passed to {@link Cursor#read(int)}. For very large result sets, consider
   * using {@code LIMIT}/{@code OFFSET} queries or streaming approaches instead.
   */
  @Override
  public @NonNull Cursor cursor(@NonNull String sql, @Nullable Object... params) {
    RowSet all = query(sql, params);
    return new Cursor() {
      private int offset = 0;
      private final List<Row> rows = new ArrayList<>();

      {
        for (Row r : all) {
          rows.add(r);
        }
      }

      @Override
      public @NonNull RowSet read(int count) {
        int end = Math.min(offset + count, rows.size());
        var batch = rows.subList(offset, end);
        offset = end;
        return new RowSet(
            new ArrayList<>(batch),
            all.columnDescriptors(),
            offset >= rows.size() ? all.rowsAffected() : 0);
      }

      @Override
      public boolean hasMore() {
        return offset < rows.size();
      }

      @Override
      public void close() {
        offset = rows.size();
      }
    };
  }

  @Override
  public void ping() {
    try {
      encoder.resetSequenceId();
      encoder.writeComPing();
      encoder.flush(out);
      byte[] payload = decoder.readPacket();
      if ((payload[0] & 0xFF) == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
      }
    } catch (IOException e) {
      throw new DbConnectionException("Ping failed", e);
    }
  }

  @Override
  public @NonNull Map<String, String> parameters() {
    return Collections.unmodifiableMap(parameters);
  }

  public int connectionId() {
    return connectionId;
  }

  private static void rejectCollections(Object[] params) {
    for (Object val : params) {
      if (val instanceof Collection<?> || (val != null && val.getClass().isArray())) {
        throw new IllegalArgumentException(
            "Collection/array values are not supported in MySQL prepared statements. "
                + "Use query(String, Map) for IN-list expansion, or use Postgres which supports "
                + "= ANY(:param) with array parameters.");
      }
    }
  }

  private record Result(byte[] paramTypeBytes, byte[][] paramValues) {}

  private @NonNull Result getResult(Object[] params) {
    int numParams = params == null ? 0 : params.length;
    byte[] paramTypeBytes = null;
    byte[][] paramValues = null;

    if (numParams > 0) {
      paramTypeBytes = new byte[numParams * 2];
      paramValues = new byte[numParams][];
      for (int i = 0; i < numParams; i++) {
        Object val = params[i];
        if (val == null) {
          paramTypeBytes[i * 2] = 0x06; // MYSQL_TYPE_NULL
          paramTypeBytes[i * 2 + 1] = 0x00;
          paramValues[i] = null;
        } else {
          encodeParam(i, val, paramTypeBytes, paramValues);
        }
      }
    }
    return new Result(paramTypeBytes, paramValues);
  }

  private void encodeParam(int idx, Object val, byte[] types, byte[][] values) {
    switch (val) {
      case Integer v -> {
        types[idx * 2] = 0x03;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeInt4LE(v);
      }
      case Long v -> {
        types[idx * 2] = 0x08;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeInt8LE(v);
      }
      case Double v -> {
        types[idx * 2] = 0x05;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeFloat8LE(v);
      }
      case Float v -> {
        types[idx * 2] = 0x04;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeFloat4LE(v);
      }
      case Short v -> {
        types[idx * 2] = 0x02;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeInt2LE(v);
      }
      case Boolean v -> {
        types[idx * 2] = 0x01;
        types[idx * 2 + 1] = 0x00;
        values[idx] = MysqlBinaryCodec.encodeInt1(v ? 1 : 0);
      }
      default -> {
        types[idx * 2] = (byte) 0xFD;
        types[idx * 2 + 1] = 0x00;
        byte[] strBytes = val.toString().getBytes(StandardCharsets.UTF_8);
        if (strBytes.length < 251) {
          byte[] result = new byte[1 + strBytes.length];
          result[0] = (byte) strBytes.length;
          System.arraycopy(strBytes, 0, result, 1, strBytes.length);
          values[idx] = result;
        } else if (strBytes.length <= 0xFFFF) {
          byte[] result = new byte[3 + strBytes.length];
          result[0] = (byte) 0xFC;
          result[1] = (byte) (strBytes.length & 0xFF);
          result[2] = (byte) ((strBytes.length >> 8) & 0xFF);
          System.arraycopy(strBytes, 0, result, 3, strBytes.length);
          values[idx] = result;
        } else {
          byte[] result = new byte[4 + strBytes.length];
          result[0] = (byte) 0xFD;
          result[1] = (byte) (strBytes.length & 0xFF);
          result[2] = (byte) ((strBytes.length >> 8) & 0xFF);
          result[3] = (byte) ((strBytes.length >> 16) & 0xFF);
          System.arraycopy(strBytes, 0, result, 4, strBytes.length);
          values[idx] = result;
        }
      }
    }
  }

  // --- Inner classes ---

  private final class MysqlPreparedStmt implements PreparedStatement {

    private final int statementId;
    private final List<String> parameterNames;
    private boolean closed = false;

    MysqlPreparedStmt(int statementId, List<String> parameterNames) {
      this.statementId = statementId;
      this.parameterNames = parameterNames;
    }

    @Override
    public @NonNull RowSet query(Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      try {
        // Build binary-encoded parameter types and values
        Result result = getResult(params);

        encoder.resetSequenceId();
        encoder.writeComStmtExecute(statementId, result.paramTypeBytes(), result.paramValues());
        encoder.flush(out);

        return readBinaryQueryResponse();
      } catch (IOException e) {
        throw new DbConnectionException("I/O error executing prepared statement", e);
      }
    }

    private void encodeParam(int idx, Object val, byte[] types, byte[][] values) {
      switch (val) {
        case Integer v -> {
          types[idx * 2] = 0x03; // MYSQL_TYPE_LONG
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeInt4LE(v);
        }
        case Long v -> {
          types[idx * 2] = 0x08; // MYSQL_TYPE_LONGLONG
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeInt8LE(v);
        }
        case Double v -> {
          types[idx * 2] = 0x05; // MYSQL_TYPE_DOUBLE
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeFloat8LE(v);
        }
        case Float v -> {
          types[idx * 2] = 0x04; // MYSQL_TYPE_FLOAT
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeFloat4LE(v);
        }
        case Short v -> {
          types[idx * 2] = 0x02; // MYSQL_TYPE_SHORT
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeInt2LE(v);
        }
        case Boolean v -> {
          types[idx * 2] = 0x01; // MYSQL_TYPE_TINY
          types[idx * 2 + 1] = 0x00;
          values[idx] = MysqlBinaryCodec.encodeInt1(v ? 1 : 0);
        }
        default -> {
          // Everything else as length-encoded string (VARCHAR)
          types[idx * 2] = (byte) 0xFD; // MYSQL_TYPE_VAR_STRING
          types[idx * 2 + 1] = 0x00;
          byte[] strBytes = val.toString().getBytes(StandardCharsets.UTF_8);
          // Length-encoded: length prefix + data
          values[idx] = encodeLengthPrefixed(strBytes);
        }
      }
    }

    private byte[] encodeLengthPrefixed(byte[] data) {
      if (data.length < 251) {
        byte[] result = new byte[1 + data.length];
        result[0] = (byte) data.length;
        System.arraycopy(data, 0, result, 1, data.length);
        return result;
      } else if (data.length <= 0xFFFF) {
        byte[] result = new byte[3 + data.length];
        result[0] = (byte) 0xFC;
        result[1] = (byte) (data.length & 0xFF);
        result[2] = (byte) ((data.length >> 8) & 0xFF);
        System.arraycopy(data, 0, result, 3, data.length);
        return result;
      } else {
        byte[] result = new byte[4 + data.length];
        result[0] = (byte) 0xFD;
        result[1] = (byte) (data.length & 0xFF);
        result[2] = (byte) ((data.length >> 8) & 0xFF);
        result[3] = (byte) ((data.length >> 16) & 0xFF);
        System.arraycopy(data, 0, result, 4, data.length);
        return result;
      }
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (parameterNames == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      Object[] resolved = NamedParamParser.resolveParams(parameterNames, params);
      rejectCollections(resolved);
      return query(resolved);
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        try {
          encoder.resetSequenceId();
          encoder.writeComStmtClose(statementId);
          encoder.flush(out);
          // COM_STMT_CLOSE has no response
        } catch (IOException e) {
          // best effort
        }
      }
    }

    private RowSet readBinaryQueryResponse() throws IOException {
      byte[] payload = decoder.readPacket();
      int header = payload[0] & 0xFF;

      if (header == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
      }
      if (header == OK_PACKET) {
        var ok = decoder.readOkPacket(payload);
        if ((ok.serverStatus() & SERVER_MORE_RESULTS_EXISTS) != 0) {
          return readRemainingResults(ok.affectedRows());
        }
        return new RowSet(List.of(), List.of(), ok.affectedRows());
      }

      // Binary result set
      int columnCount = decoder.readColumnCount(payload);

      // Re-read column definitions (may differ from prepare response)
      ColumnDescriptor[] resultColumns = new ColumnDescriptor[columnCount];
      int[] resultTypes = new int[columnCount];
      for (int i = 0; i < columnCount; i++) {
        byte[] colDef = decoder.readPacket();
        resultColumns[i] = decoder.readColumnDefinition(colDef);
        resultTypes[i] = resultColumns[i].typeOID();
      }
      if (!decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF
      }

      // Read binary rows
      List<Row> rows = new ArrayList<>();
      while (true) {
        byte[] rowPayload = decoder.readPacket();
        int first = rowPayload[0] & 0xFF;

        if (first == ERR_PACKET) {
          return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload)));
        }
        if (first == EOF_PACKET
            && rowPayload.length < MysqlProtocolConstants.PACKET_PAYLOAD_LIMIT) {
          int serverStatus;
          int affected = 0;
          if (decoder.isDeprecateEof()) {
            var ok = decoder.readOkPacket(rowPayload);
            serverStatus = ok.serverStatus();
            affected = ok.affectedRows();
          } else {
            serverStatus = decoder.readEofPacket(rowPayload).serverStatus();
          }
          if ((serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0) {
            readRemainingResults(0);
          }
          return new RowSet(rows, List.of(resultColumns), affected);
        }

        // Binary row (starts with 0x00 header)
        byte[][] values = decoder.readBinaryRow(rowPayload, columnCount, resultTypes);
        rows.add(createRow(resultColumns, values));
      }
    }
  }

  /**
   * Wrapper around a cached MySQL prepared statement. close() evicts from cache and closes
   * server-side.
   */
  private final class CachedMysqlPreparedStmt implements PreparedStatement {

    private final PreparedStatementCache.CachedStatement cached;
    private boolean closed = false;

    CachedMysqlPreparedStmt(PreparedStatementCache.CachedStatement cached) {
      this.cached = cached;
    }

    @Override
    public @NonNull RowSet query(Object... params) {
      if (closed) {
        throw new IllegalStateException("PreparedStatement is closed");
      }
      try {
        Result result = getResult(params);

        encoder.resetSequenceId();
        encoder.writeComStmtExecute(
            cached.mysqlStatementId(), result.paramTypeBytes(), result.paramValues());
        encoder.flush(out);

        return readCachedBinaryQueryResponse();
      } catch (IOException e) {
        throw new DbConnectionException("I/O error executing prepared statement", e);
      }
    }

    private RowSet readCachedBinaryQueryResponse() throws IOException {
      byte[] payload = decoder.readPacket();
      int header = payload[0] & 0xFF;

      if (header == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
      }
      if (header == OK_PACKET) {
        var ok = decoder.readOkPacket(payload);
        return new RowSet(List.of(), List.of(), ok.affectedRows());
      }

      int columnCount = decoder.readColumnCount(payload);
      ColumnDescriptor[] resultColumns = new ColumnDescriptor[columnCount];
      int[] resultTypes = new int[columnCount];
      for (int i = 0; i < columnCount; i++) {
        byte[] colDef = decoder.readPacket();
        resultColumns[i] = decoder.readColumnDefinition(colDef);
        resultTypes[i] = resultColumns[i].typeOID();
      }
      if (!decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF
      }

      List<Row> rows = new ArrayList<>();
      while (true) {
        byte[] rowPayload = decoder.readPacket();
        int first = rowPayload[0] & 0xFF;

        if (first == ERR_PACKET) {
          return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload)));
        }
        if (first == EOF_PACKET
            && rowPayload.length < MysqlProtocolConstants.PACKET_PAYLOAD_LIMIT) {
          int affected = 0;
          if (decoder.isDeprecateEof()) {
            var ok = decoder.readOkPacket(rowPayload);
            affected = ok.affectedRows();
          } else {
            decoder.readEofPacket(rowPayload);
          }
          return new RowSet(rows, List.of(resultColumns), affected);
        }

        byte[][] values = decoder.readBinaryRow(rowPayload, columnCount, resultTypes);
        rows.add(createRow(resultColumns, values));
      }
    }

    @Override
    public @NonNull RowSet query(@NonNull Map<String, Object> params) {
      if (cached.parameterNames() == null) {
        throw new IllegalStateException(
            "This statement was not prepared with named parameters (:name syntax)");
      }
      Object[] resolved = NamedParamParser.resolveParams(cached.parameterNames(), params);
      rejectCollections(resolved);
      return query(resolved);
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        if (psCache != null) {
          psCache.remove(cached.sql());
        }
        closeCachedStatement(cached);
      }
    }
  }

  // --- BaseConnection protocol methods ---

  @Override
  protected @NonNull BinaryCodec binaryCodec() {
    return MysqlBinaryCodec.INSTANCE;
  }

  @Override
  protected @NonNull String placeholderPrefix() {
    return "?";
  }

  @Override
  protected @NonNull RowSet executeExtendedQuery(@NonNull String sql, @NonNull String[] params) {
    // Parameterless queries use COM_QUERY (text-format results) because MySQL's prepared
    // statement protocol (COM_STMT_PREPARE) rejects many SQL commands: BEGIN, COMMIT,
    // ROLLBACK, SET, CREATE, ALTER, DROP, and other session/DDL statements return
    // error 1295 "This command is not supported in the prepared statement protocol yet".
    if (params.length == 0) {
      return executeComQuery(sql);
    }

    if (isCacheable(sql)) {
      var cached = psCache.get(sql);
      if (cached != null) {
        // Cache hit: skip prepare, go straight to execute
        return executeWithStatementId(
            cached.mysqlStatementId(), params, cached.columns(), cached.columnTypes(), false);
      }
    }

    // Prepare the statement
    try {
      encoder.resetSequenceId();
      encoder.writeComStmtPrepare(sql, charset);
      encoder.flush(out);

      byte[] payload = decoder.readPacket();
      if ((payload[0] & 0xFF) == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
      }
      var prepResult = decoder.readPrepareResult(payload);

      // Read param definitions
      for (int i = 0; i < prepResult.numParams(); i++) {
        decoder.readPacket();
      }
      if (prepResult.numParams() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF
      }

      // Read column definitions
      ColumnDescriptor[] columns = new ColumnDescriptor[prepResult.numColumns()];
      int[] columnTypes = new int[prepResult.numColumns()];
      for (int i = 0; i < prepResult.numColumns(); i++) {
        byte[] colDef = decoder.readPacket();
        columns[i] = decoder.readColumnDefinition(colDef);
        columnTypes[i] = columns[i].typeOID();
      }
      if (prepResult.numColumns() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket(); // EOF
      }

      boolean shouldCache = isCacheable(sql);

      // Execute
      RowSet result =
          executeWithStatementId(
              prepResult.statementId(), params, columns, columnTypes, !shouldCache);

      // Cache on success (skip close since cache owns it)
      if (shouldCache && result.getError() == null) {
        var stmt =
            new PreparedStatementCache.CachedStatement(
                sql, null, prepResult.statementId(), columns, columnTypes);
        handleEvicted(psCache.cache(sql, stmt));
      }

      return result;
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during extended query", e);
    }
  }

  private RowSet executeWithStatementId(
      int statementId,
      String[] params,
      ColumnDescriptor[] columns,
      int[] columnTypes,
      boolean closeAfter) {
    try {
      int numParams = params == null ? 0 : params.length;
      byte[] paramTypeBytes = null;
      byte[][] paramValues = null;

      if (numParams > 0) {
        paramTypeBytes = new byte[numParams * 2];
        paramValues = new byte[numParams][];
        for (int i = 0; i < numParams; i++) {
          if (params[i] == null) {
            paramTypeBytes[i * 2] = 0x06; // NULL
            paramTypeBytes[i * 2 + 1] = 0x00;
            paramValues[i] = null;
          } else {
            // Encode all as VARCHAR (text) for pipeline path
            paramTypeBytes[i * 2] = (byte) 0xFD; // VAR_STRING
            paramTypeBytes[i * 2 + 1] = 0x00;
            byte[] strBytes = params[i].getBytes(StandardCharsets.UTF_8);
            byte[] result;
            if (strBytes.length < 251) {
              result = new byte[1 + strBytes.length];
              result[0] = (byte) strBytes.length;
              System.arraycopy(strBytes, 0, result, 1, strBytes.length);
            } else {
              result = new byte[3 + strBytes.length];
              result[0] = (byte) 0xFC;
              result[1] = (byte) (strBytes.length & 0xFF);
              result[2] = (byte) ((strBytes.length >> 8) & 0xFF);
              System.arraycopy(strBytes, 0, result, 3, strBytes.length);
            }
            paramValues[i] = result;
          }
        }
      }

      encoder.resetSequenceId();
      encoder.writeComStmtExecute(statementId, paramTypeBytes, paramValues);
      encoder.flush(out);

      RowSet result = readBinaryExecuteResponse(columns, columnTypes);

      if (closeAfter) {
        encoder.resetSequenceId();
        encoder.writeComStmtClose(statementId);
        encoder.flush(out);
      }

      return result;
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during extended query execution", e);
    }
  }

  private RowSet readBinaryExecuteResponse(ColumnDescriptor[] prepColumns, int[] prepColumnTypes)
      throws IOException {
    byte[] payload = decoder.readPacket();
    int header = payload[0] & 0xFF;

    if (header == ERR_PACKET) {
      return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
    }
    if (header == OK_PACKET && payload.length < 9_000_000) {
      var ok = decoder.readOkPacket(payload);
      return new RowSet(List.of(), List.of(), ok.affectedRows());
    }

    // Result set: re-read column definitions
    int columnCount = decoder.readColumnCount(payload);
    ColumnDescriptor[] resultColumns = new ColumnDescriptor[columnCount];
    int[] resultTypes = new int[columnCount];
    for (int i = 0; i < columnCount; i++) {
      byte[] colDef = decoder.readPacket();
      resultColumns[i] = decoder.readColumnDefinition(colDef);
      resultTypes[i] = resultColumns[i].typeOID();
    }
    if (!decoder.isDeprecateEof()) {
      decoder.readPacket(); // EOF
    }

    // Read binary rows into column buffers
    ColumnBuffer[] buffers = newColumnBuffers(columnCount);
    int rowCount = 0;
    while (true) {
      byte[] rowPayload = decoder.readPacket();
      int first = rowPayload[0] & 0xFF;

      if (first == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload)));
      }
      if (first == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
        return buildRowSet(resultColumns, buffers, rowCount, 0);
      }

      byte[][] values = decoder.readBinaryRow(rowPayload, columnCount, resultTypes);
      appendToBuffers(buffers, values);
      rowCount++;
    }
  }

  @Override
  protected void flushToNetwork() {
    try {
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during flush", e);
    }
  }

  // --- COM_QUERY path for parameterless queries ---

  private RowSet executeComQuery(String sql) {
    try {
      encoder.resetSequenceId();
      encoder.writeComQuery(sql, charset);
      encoder.flush(out);
      return readComQueryResponse();
    } catch (IOException e) {
      throw new DbConnectionException("I/O error executing query", e);
    }
  }

  private RowSet readComQueryResponse() throws IOException {
    byte[] payload = decoder.readPacket();
    int header = payload[0] & 0xFF;

    if (header == ERR_PACKET) {
      return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
    }

    if (header == OK_PACKET && payload.length < 9_000_000) {
      var ok = decoder.readOkPacket(payload);
      if ((ok.serverStatus() & SERVER_MORE_RESULTS_EXISTS) != 0) {
        return readRemainingResults(ok.affectedRows());
      }
      return new RowSet(List.of(), List.of(), ok.affectedRows());
    }

    // Result set
    int columnCount = decoder.readColumnCount(payload);
    return readTextResultSet(columnCount);
  }

  private RowSet readTextResultSet(int columnCount) throws IOException {
    ColumnDescriptor[] columns = new ColumnDescriptor[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = decoder.readColumnDefinition(decoder.readPacket());
    }
    if (!decoder.isDeprecateEof()) {
      decoder.readPacket();
    }

    ColumnBuffer[] buffers = newColumnBuffers(columnCount);
    int rowCount = 0;
    while (true) {
      byte[] rowPayload = decoder.readPacket();
      int firstByte = rowPayload[0] & 0xFF;

      if (firstByte == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload)));
      }
      if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
        int serverStatus;
        int affectedRows = 0;
        if (decoder.isDeprecateEof()) {
          var ok = decoder.readOkPacket(rowPayload);
          serverStatus = ok.serverStatus();
          affectedRows = ok.affectedRows();
        } else {
          serverStatus = decoder.readEofPacket(rowPayload).serverStatus();
        }
        if ((serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0) {
          readRemainingResults(0);
        }
        return buildTextRowSet(columns, buffers, rowCount, affectedRows);
      }

      appendToBuffers(buffers, decoder.readTextRow(rowPayload, columnCount));
      rowCount++;
    }
  }

  private RowSet readRemainingResults(int totalAffected) throws IOException {
    while (true) {
      byte[] payload = decoder.readPacket();
      int header = payload[0] & 0xFF;

      if (header == OK_PACKET) {
        var ok = decoder.readOkPacket(payload);
        totalAffected += ok.affectedRows();
        if ((ok.serverStatus() & SERVER_MORE_RESULTS_EXISTS) == 0) {
          return new RowSet(List.of(), List.of(), totalAffected);
        }
      } else if (header == ERR_PACKET) {
        return new RowSet(MysqlException.fromErrPacket(decoder.readErrPacket(payload)));
      } else {
        readTextResultSet(decoder.readColumnCount(payload)); // discard
      }
    }
  }

  private RowSet buildTextRowSet(
      ColumnDescriptor[] columns, ColumnBuffer[] buffers, int rowCount, int rowsAffected) {
    List<Row> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(createTextBufferedRow(columns, buffers, i));
    }
    return new RowSet(rows, List.of(columns), rowsAffected);
  }

  private void streamComQuery(String sql, Consumer<Row> consumer) throws IOException {
    encoder.resetSequenceId();
    encoder.writeComQuery(sql, charset);
    encoder.flush(out);

    byte[] payload = decoder.readPacket();
    int header = payload[0] & 0xFF;
    if (header == ERR_PACKET) {
      throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
    }
    if (header == OK_PACKET && payload.length < 9_000_000) {
      return;
    }

    int columnCount = decoder.readColumnCount(payload);
    ColumnDescriptor[] columns = new ColumnDescriptor[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = decoder.readColumnDefinition(decoder.readPacket());
    }
    if (!decoder.isDeprecateEof()) {
      decoder.readPacket();
    }

    while (true) {
      byte[] rowPayload = decoder.readPacket();
      int firstByte = rowPayload[0] & 0xFF;
      if (firstByte == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload));
      }
      if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
        return;
      }
      consumer.accept(createTextRow(columns, decoder.readTextRow(rowPayload, columnCount)));
    }
  }

  private RowStream createComQueryRowStream(String sql) {
    try {
      encoder.resetSequenceId();
      encoder.writeComQuery(sql, charset);
      encoder.flush(out);

      byte[] payload = decoder.readPacket();
      int header = payload[0] & 0xFF;
      if (header == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
      }
      if (header == OK_PACKET && payload.length < 9_000_000) {
        return new RowStream(() -> null, () -> {});
      }

      int columnCount = decoder.readColumnCount(payload);
      ColumnDescriptor[] columns = new ColumnDescriptor[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columns[i] = decoder.readColumnDefinition(decoder.readPacket());
      }
      if (!decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      var exhausted = new AtomicBoolean(false);
      return new RowStream(
          () -> {
            if (exhausted.get()) {
              return null;
            }
            try {
              byte[] rowPayload = decoder.readPacket();
              int firstByte = rowPayload[0] & 0xFF;
              if (firstByte == ERR_PACKET) {
                exhausted.set(true);
                throw MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload));
              }
              if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
                exhausted.set(true);
                return null;
              }
              return createTextRow(columns, decoder.readTextRow(rowPayload, columnCount));
            } catch (IOException e) {
              exhausted.set(true);
              throw new DbConnectionException("I/O error reading streaming row", e);
            }
          },
          () -> {
            if (!exhausted.get()) {
              try {
                while (true) {
                  byte[] rowPayload = decoder.readPacket();
                  int firstByte = rowPayload[0] & 0xFF;
                  if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
                    break;
                  }
                  if (firstByte == ERR_PACKET) {
                    break;
                  }
                }
              } catch (IOException e) {
                // best effort drain
              }
              exhausted.set(true);
            }
          });
    } catch (IOException e) {
      throw new DbConnectionException("I/O error setting up MySQL streaming", e);
    }
  }

  @Override
  protected void closeCachedStatement(PreparedStatementCache.CachedStatement stmt) {
    try {
      encoder.resetSequenceId();
      encoder.writeComStmtClose(stmt.mysqlStatementId());
      encoder.flush(out);
      // COM_STMT_CLOSE has no response
    } catch (IOException e) {
      // best effort
    }
  }

  @Override
  protected void executeExtendedQueryStreaming(
      @NonNull String sql, @NonNull String[] params, @NonNull Consumer<Row> consumer) {
    if (params.length == 0) {
      try {
        streamComQuery(sql, consumer);
      } catch (IOException e) {
        throw new DbConnectionException("I/O error reading MySQL streaming response", e);
      }
      return;
    }

    // Prepare the statement
    try {
      encoder.resetSequenceId();
      encoder.writeComStmtPrepare(sql, charset);
      encoder.flush(out);

      byte[] payload = decoder.readPacket();
      if ((payload[0] & 0xFF) == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
      }
      var prepResult = decoder.readPrepareResult(payload);

      for (int i = 0; i < prepResult.numParams(); i++) {
        decoder.readPacket();
      }
      if (prepResult.numParams() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      ColumnDescriptor[] columns = new ColumnDescriptor[prepResult.numColumns()];
      int[] columnTypes = new int[prepResult.numColumns()];
      for (int i = 0; i < prepResult.numColumns(); i++) {
        byte[] colDef = decoder.readPacket();
        columns[i] = decoder.readColumnDefinition(colDef);
        columnTypes[i] = columns[i].typeOID();
      }
      if (prepResult.numColumns() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      // Execute
      executeWithStatementIdStreaming(
          prepResult.statementId(), params, columns, columnTypes, consumer);

      // Close the statement (not caching in streaming path)
      encoder.resetSequenceId();
      encoder.writeComStmtClose(prepResult.statementId());
      encoder.flush(out);
    } catch (IOException e) {
      throw new DbConnectionException("I/O error during streaming extended query", e);
    }
  }

  private void executeWithStatementIdStreaming(
      int statementId,
      String[] params,
      ColumnDescriptor[] columns,
      int[] columnTypes,
      Consumer<Row> consumer)
      throws IOException {
    int numParams = params == null ? 0 : params.length;
    byte[] paramTypeBytes = null;
    byte[][] paramValues = null;

    if (numParams > 0) {
      paramTypeBytes = new byte[numParams * 2];
      paramValues = new byte[numParams][];
      for (int i = 0; i < numParams; i++) {
        if (params[i] == null) {
          paramTypeBytes[i * 2] = 0x06;
          paramTypeBytes[i * 2 + 1] = 0x00;
          paramValues[i] = null;
        } else {
          paramTypeBytes[i * 2] = (byte) 0xFD;
          paramTypeBytes[i * 2 + 1] = 0x00;
          byte[] strBytes = params[i].getBytes(StandardCharsets.UTF_8);
          byte[] result;
          if (strBytes.length < 251) {
            result = new byte[1 + strBytes.length];
            result[0] = (byte) strBytes.length;
            System.arraycopy(strBytes, 0, result, 1, strBytes.length);
          } else {
            result = new byte[3 + strBytes.length];
            result[0] = (byte) 0xFC;
            result[1] = (byte) (strBytes.length & 0xFF);
            result[2] = (byte) ((strBytes.length >> 8) & 0xFF);
            System.arraycopy(strBytes, 0, result, 3, strBytes.length);
          }
          paramValues[i] = result;
        }
      }
    }

    encoder.resetSequenceId();
    encoder.writeComStmtExecute(statementId, paramTypeBytes, paramValues);
    encoder.flush(out);

    streamBinaryExecuteResponse(columns, columnTypes, consumer);
  }

  private void streamBinaryExecuteResponse(
      ColumnDescriptor[] prepColumns, int[] prepColumnTypes, Consumer<Row> consumer)
      throws IOException {
    byte[] payload = decoder.readPacket();
    int header = payload[0] & 0xFF;

    if (header == ERR_PACKET) {
      throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
    }
    if (header == OK_PACKET && payload.length < 9_000_000) {
      return;
    }

    int columnCount = decoder.readColumnCount(payload);
    ColumnDescriptor[] resultColumns = new ColumnDescriptor[columnCount];
    int[] resultTypes = new int[columnCount];
    for (int i = 0; i < columnCount; i++) {
      byte[] colDef = decoder.readPacket();
      resultColumns[i] = decoder.readColumnDefinition(colDef);
      resultTypes[i] = resultColumns[i].typeOID();
    }
    if (!decoder.isDeprecateEof()) {
      decoder.readPacket();
    }

    while (true) {
      byte[] rowPayload = decoder.readPacket();
      int first = rowPayload[0] & 0xFF;
      if (first == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload));
      }
      if (first == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
        return;
      }
      byte[][] values = decoder.readBinaryRow(rowPayload, columnCount, resultTypes);
      consumer.accept(createRow(resultColumns, values));
    }
  }

  @Override
  protected @NonNull RowStream createExtendedQueryRowStream(
      @NonNull String sql, @NonNull String[] params) {
    if (params.length == 0) {
      return createComQueryRowStream(sql);
    }

    // For MySQL, prepare + execute + return a RowStream reading binary results
    try {
      encoder.resetSequenceId();
      encoder.writeComStmtPrepare(sql, charset);
      encoder.flush(out);

      byte[] payload = decoder.readPacket();
      if ((payload[0] & 0xFF) == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
      }
      var prepResult = decoder.readPrepareResult(payload);

      for (int i = 0; i < prepResult.numParams(); i++) {
        decoder.readPacket();
      }
      if (prepResult.numParams() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      ColumnDescriptor[] prepColumns = new ColumnDescriptor[prepResult.numColumns()];
      int[] columnTypes = new int[prepResult.numColumns()];
      for (int i = 0; i < prepResult.numColumns(); i++) {
        byte[] colDef = decoder.readPacket();
        prepColumns[i] = decoder.readColumnDefinition(colDef);
        columnTypes[i] = prepColumns[i].typeOID();
      }
      if (prepResult.numColumns() > 0 && !decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      // Execute
      int numParams = params == null ? 0 : params.length;
      byte[] paramTypeBytes = null;
      byte[][] paramValues = null;
      if (numParams > 0) {
        paramTypeBytes = new byte[numParams * 2];
        paramValues = new byte[numParams][];
        for (int i = 0; i < numParams; i++) {
          if (params[i] == null) {
            paramTypeBytes[i * 2] = 0x06;
            paramTypeBytes[i * 2 + 1] = 0x00;
            paramValues[i] = null;
          } else {
            paramTypeBytes[i * 2] = (byte) 0xFD;
            paramTypeBytes[i * 2 + 1] = 0x00;
            byte[] strBytes = params[i].getBytes(StandardCharsets.UTF_8);
            byte[] result;
            if (strBytes.length < 251) {
              result = new byte[1 + strBytes.length];
              result[0] = (byte) strBytes.length;
              System.arraycopy(strBytes, 0, result, 1, strBytes.length);
            } else {
              result = new byte[3 + strBytes.length];
              result[0] = (byte) 0xFC;
              result[1] = (byte) (strBytes.length & 0xFF);
              result[2] = (byte) ((strBytes.length >> 8) & 0xFF);
              System.arraycopy(strBytes, 0, result, 3, strBytes.length);
            }
            paramValues[i] = result;
          }
        }
      }

      encoder.resetSequenceId();
      encoder.writeComStmtExecute(prepResult.statementId(), paramTypeBytes, paramValues);
      encoder.flush(out);

      // Read response header
      payload = decoder.readPacket();
      int hdr = payload[0] & 0xFF;
      if (hdr == ERR_PACKET) {
        throw MysqlException.fromErrPacket(decoder.readErrPacket(payload));
      }
      if (hdr == OK_PACKET && payload.length < 9_000_000) {
        // Close statement, return empty stream
        encoder.resetSequenceId();
        encoder.writeComStmtClose(prepResult.statementId());
        encoder.flush(out);
        return new RowStream(() -> null, () -> {});
      }

      int columnCount = decoder.readColumnCount(payload);
      ColumnDescriptor[] resultColumns = new ColumnDescriptor[columnCount];
      int[] resultTypes = new int[columnCount];
      for (int i = 0; i < columnCount; i++) {
        byte[] colDef = decoder.readPacket();
        resultColumns[i] = decoder.readColumnDefinition(colDef);
        resultTypes[i] = resultColumns[i].typeOID();
      }
      if (!decoder.isDeprecateEof()) {
        decoder.readPacket();
      }

      int stmtId = prepResult.statementId();
      var exhausted = new AtomicBoolean(false);
      return new RowStream(
          () -> {
            if (exhausted.get()) {
              return null;
            }
            try {
              byte[] rowPayload = decoder.readPacket();
              int first = rowPayload[0] & 0xFF;
              if (first == ERR_PACKET) {
                exhausted.set(true);
                throw MysqlException.fromErrPacket(decoder.readErrPacket(rowPayload));
              }
              if (first == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
                exhausted.set(true);
                return null;
              }
              byte[][] values = decoder.readBinaryRow(rowPayload, columnCount, resultTypes);
              return createRow(resultColumns, values);
            } catch (IOException e) {
              exhausted.set(true);
              throw new DbConnectionException("I/O error reading streaming row", e);
            }
          },
          () -> {
            if (!exhausted.get()) {
              try {
                while (true) {
                  byte[] rowPayload = decoder.readPacket();
                  int firstByte = rowPayload[0] & 0xFF;
                  if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
                    break;
                  }
                  if (firstByte == ERR_PACKET) {
                    break;
                  }
                }
              } catch (IOException e) {
                // best effort drain
              }
              exhausted.set(true);
            }
            // Close the prepared statement
            try {
              encoder.resetSequenceId();
              encoder.writeComStmtClose(stmtId);
              encoder.flush(out);
            } catch (IOException e) {
              // best effort
            }
          });
    } catch (IOException e) {
      throw new DbConnectionException("I/O error setting up MySQL streaming", e);
    }
  }

  @Override
  protected void sendTerminate() {
    try {
      encoder.resetSequenceId();
      encoder.writeComQuit();
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

  // --- Handshake / Auth ---

  private void performHandshake(String user, String password, String database) throws IOException {
    // Step 1: Read initial handshake from server
    byte[] handshakePayload = decoder.readPacket();

    if ((handshakePayload[0] & 0xFF) == ERR_PACKET) {
      var err = decoder.readErrPacket(handshakePayload);
      throw MysqlException.fromErrPacket(err);
    }

    var handshake = decoder.readHandshake(handshakePayload);
    connectionId = handshake.connectionId();
    int serverCapabilities = handshake.serverCapabilities();
    authPluginData = handshake.authPluginData();

    parameters.put("server_version", handshake.serverVersion());

    // Determine client flags
    int clientFlags = CLIENT_SUPPORTED_FLAGS;
    if (database != null && !database.isEmpty()) {
      clientFlags |= CLIENT_CONNECT_WITH_DB;
    }
    clientFlags &= serverCapabilities;

    // Check if server supports DEPRECATE_EOF
    if ((serverCapabilities & CLIENT_DEPRECATE_EOF) != 0) {
      clientFlags |= CLIENT_DEPRECATE_EOF;
      decoder.setDeprecateEof(true);
    }

    // SSL upgrade: if requested and server supports it
    if (sslMode != SslMode.DISABLE && (serverCapabilities & CLIENT_SSL) != 0) {
      clientFlags |= CLIENT_SSL;
      // Send SSL request packet (short handshake response with just flags)
      encoder.setSequenceId(decoder.lastSequenceId() + 1);
      encoder.writeSslRequest(clientFlags, handshake.charset());
      encoder.flush(out);
      // Upgrade socket to SSL
      upgradeToSsl();
    } else if (sslMode == SslMode.REQUIRE && (serverCapabilities & CLIENT_SSL) == 0) {
      throw new MysqlException(0, "08000", "Server does not support SSL but sslMode=REQUIRE");
    }

    // Compute auth response
    String authPlugin = handshake.authPluginName();
    byte[] authResponse = computeAuthResponse(authPlugin, password, handshake.authPluginData());

    // Step 2: Send handshake response
    encoder.setSequenceId(decoder.lastSequenceId() + 1);
    encoder.writeHandshakeResponse(
        clientFlags, user, authResponse, database, authPlugin, handshake.charset(), null);
    encoder.flush(out);

    // Step 3: Read auth result
    handleAuthResult(password);
  }

  private void upgradeToSsl() throws IOException {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(
          null,
          new TrustManager[] {
            new X509TrustManager() {
              public void checkClientTrusted(
                  java.security.cert.X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(
                  java.security.cert.X509Certificate[] certs, String authType) {}

              public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[0];
              }
            }
          },
          null);
      SSLSocketFactory factory = ctx.getSocketFactory();
      SSLSocket sslSocket =
          (SSLSocket)
              factory.createSocket(
                  socket, socket.getInetAddress().getHostAddress(), socket.getPort(), true);
      sslSocket.setUseClientMode(true);
      sslSocket.startHandshake();

      // Replace socket and I/O streams for encrypted communication
      this.socket = sslSocket;
      this.out = new BufferedOutputStream(sslSocket.getOutputStream(), 8192);
      boolean wasDeprecateEof = decoder.isDeprecateEof();
      this.decoder = new MysqlDecoder(new BufferedInputStream(sslSocket.getInputStream(), 8192));
      if (wasDeprecateEof) {
        decoder.setDeprecateEof(true);
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("SSL upgrade failed", e);
    }
  }

  private void handleAuthResult(String password) throws IOException {
    byte[] payload = decoder.readPacket();
    int header = payload[0] & 0xFF;

    switch (header) {
      case OK_PACKET -> {
        // Authenticated successfully
      }
      case ERR_PACKET -> {
        var err = decoder.readErrPacket(payload);
        throw MysqlException.fromErrPacket(err);
      }
      case AUTH_SWITCH_REQUEST -> handleAuthSwitch(password, payload);
      case AUTH_MORE_DATA -> handleAuthMoreData(password, payload);
      default ->
          throw new DbConnectionException(
              "Unexpected auth response header: 0x" + Integer.toHexString(header));
    }
  }

  private static final int FAST_AUTH_STATUS = 0x03;
  private static final int FULL_AUTH_STATUS = 0x04;
  private static final int PUBLIC_KEY_REQUEST = 0x02;

  private void handleAuthMoreData(String password, byte[] payload) throws IOException {
    // caching_sha2_password sends 0x01 + status byte
    // status 0x03 = fast auth success, status 0x04 = full auth needed
    if (payload.length >= 2) {
      int status = payload[1] & 0xFF;
      if (status == FAST_AUTH_STATUS) {
        // Fast auth succeeded — read the final OK packet
        handleAuthResult(password);
        return;
      } else if (status == FULL_AUTH_STATUS) {
        // Full auth needed — request server's RSA public key and encrypt password
        sendRsaEncryptedPassword(password);
        handleAuthResult(password);
        return;
      }
    }
    // Read next packet for final result
    handleAuthResult(password);
  }

  private void sendRsaEncryptedPassword(String password) throws IOException {
    if (authPluginData == null) throw new IllegalStateException("No auth plugin 'nonce' data");

    // Step 1: Request the server's RSA public key
    encoder.setSequenceId(decoder.lastSequenceId() + 1);
    encoder.writeAuthResponse(new byte[] {PUBLIC_KEY_REQUEST});
    encoder.flush(out);

    // Step 2: Read the server's RSA public key (AUTH_MORE_DATA with key PEM)
    byte[] keyPayload = decoder.readPacket();
    int keyHeader = keyPayload[0] & 0xFF;
    if (keyHeader == MysqlProtocolConstants.ERR_PACKET) {
      throw MysqlException.fromErrPacket(decoder.readErrPacket(keyPayload));
    }
    // Skip the 0x01 header byte, rest is the PEM-encoded RSA public key
    String rsaPublicKey = new String(keyPayload, 1, keyPayload.length - 1, StandardCharsets.UTF_8);

    // Step 3: Encrypt the NULL-terminated password XOR'ed with the nonce
    byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
    byte[] passwordWithNull = new byte[passwordBytes.length + 1];
    System.arraycopy(passwordBytes, 0, passwordWithNull, 0, passwordBytes.length);
    byte[] encrypted =
        RsaPublicKeyEncryptor.encrypt(passwordWithNull, authPluginData, rsaPublicKey);

    // Step 4: Send the encrypted password
    encoder.setSequenceId(decoder.lastSequenceId() + 1);
    encoder.writeAuthResponse(encrypted);
    encoder.flush(out);
  }

  private void handleAuthSwitch(String password, byte[] payload) throws IOException {
    var buf = io.github.bpdbi.core.impl.ByteBuffer.wrap(payload);
    buf.readByte(); // skip 0xFE
    String pluginName = MysqlDecoder.readNullTerminatedString(buf, StandardCharsets.UTF_8);
    byte[] nonce = new byte[NONCE_LENGTH];
    buf.readBytes(nonce);
    authPluginData = nonce; // update nonce for potential RSA full auth

    byte[] authResponse = computeAuthResponse(pluginName, password, nonce);

    encoder.setSequenceId(decoder.lastSequenceId() + 1);
    encoder.writeAuthResponse(authResponse);
    encoder.flush(out);

    // Read final auth result
    handleAuthResult(password);
  }

  private byte[] computeAuthResponse(String authPlugin, String password, byte[] nonce) {
    if (password == null || password.isEmpty()) {
      return new byte[0];
    }
    byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
    return switch (authPlugin) {
      case "mysql_native_password" -> Native41Authenticator.encode(passwordBytes, nonce);
      case "caching_sha2_password" -> CachingSha2Authenticator.encode(passwordBytes, nonce);
      case "mysql_clear_password" -> {
        // null-terminated password
        byte[] result = new byte[passwordBytes.length + 1];
        System.arraycopy(passwordBytes, 0, result, 0, passwordBytes.length);
        yield result;
      }
      default -> // Fallback to mysql_native_password
          Native41Authenticator.encode(passwordBytes, nonce);
    };
  }
}
