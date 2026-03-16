package io.djb.mysql;

import io.djb.*;
import io.djb.impl.BaseConnection;
import io.djb.mysql.impl.auth.CachingSha2Authenticator;
import io.djb.mysql.impl.auth.Native41Authenticator;
import io.djb.mysql.impl.codec.MysqlDecoder;
import io.djb.mysql.impl.codec.MysqlEncoder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.djb.mysql.impl.codec.MysqlProtocolConstants.*;

/**
 * MySQL implementation of {@link io.djb.Connection}.
 */
public final class MysqlConnection extends BaseConnection {

    private final Socket socket;
    private final OutputStream out;
    private final MysqlEncoder encoder;
    private final MysqlDecoder decoder;
    private final Map<String, String> parameters = new HashMap<>();
    private final Charset charset = StandardCharsets.UTF_8;
    private int serverCapabilities;
    private int connectionId;

    private MysqlConnection(Socket socket, OutputStream out, MysqlEncoder encoder, MysqlDecoder decoder) {
        this.socket = socket;
        this.out = out;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public static MysqlConnection connect(String host, int port, String database, String user, String password) {
        return connect(host, port, database, user, password, null);
    }

    public static MysqlConnection connect(String host, int port, String database, String user, String password,
                                           Map<String, String> properties) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            var out = new BufferedOutputStream(socket.getOutputStream(), 8192);
            var in = new BufferedInputStream(socket.getInputStream(), 8192);
            var encoder = new MysqlEncoder();
            var decoder = new MysqlDecoder(in);

            var conn = new MysqlConnection(socket, out, encoder, decoder);
            conn.performHandshake(user, password, database);
            return conn;
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to MySQL at " + host + ":" + port, e);
        }
    }

    public static MysqlConnection connect(ConnectionConfig config) {
        return connect(config.host(), config.port() > 0 ? config.port() : 3306,
            config.database(), config.username(), config.password(), config.properties());
    }

    @Override
    public PreparedStatement prepare(String sql) {
        // MySQL MVP: prepared statements use text interpolation (same as query)
        // A real binary protocol implementation would use COM_STMT_PREPARE here
        return new PreparedStatement() {
            @Override
            public RowSet query(Object... params) {
                return MysqlConnection.this.query(sql, params);
            }
            @Override
            public void close() {
                // no-op for text protocol
            }
        };
    }

    @Override
    public io.djb.Cursor cursor(String sql, Object... params) {
        // MySQL cursors via COM_STMT_FETCH require binary protocol
        // MVP: fetch all rows and simulate cursor over them
        RowSet all = query(sql, params);
        return new io.djb.Cursor() {
            private int offset = 0;
            private final java.util.List<Row> rows = new ArrayList<>();
            {
                for (Row r : all) rows.add(r);
            }

            @Override
            public RowSet read(int count) {
                int end = Math.min(offset + count, rows.size());
                var batch = rows.subList(offset, end);
                offset = end;
                return new RowSet(new ArrayList<>(batch), all.columnDescriptors(),
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
    public Map<String, String> parameters() {
        return Collections.unmodifiableMap(parameters);
    }

    public int connectionId() {
        return connectionId;
    }

    // --- BaseConnection protocol methods ---

    @Override
    protected String placeholderPrefix() {
        return "?";
    }

    @Override
    protected void encodeSimpleQuery(String sql) {
        encoder.resetSequenceId();
        encoder.writeComQuery(sql, charset);
    }

    @Override
    protected void encodeExtendedQuery(String sql, String[] params) {
        // MySQL MVP: use COM_QUERY with parameter interpolation
        encoder.resetSequenceId();
        encoder.writeComStmtExecuteAsQuery(sql, params, charset);
    }

    @Override
    protected void flushToNetwork() {
        try {
            encoder.flush(out);
        } catch (IOException e) {
            throw new RuntimeException("I/O error during flush", e);
        }
    }

    @Override
    protected RowSet readSimpleQueryResponse() {
        return readQueryResponse();
    }

    @Override
    protected RowSet readExtendedQueryResponse() {
        return readQueryResponse();
    }

    private RowSet readQueryResponse() {
        try {
            byte[] payload = decoder.readPacket();
            int header = payload[0] & 0xFF;

            if (header == ERR_PACKET) {
                var err = decoder.readErrPacket(payload);
                return new RowSet(MysqlException.fromErrPacket(err));
            }

            if (header == OK_PACKET && payload.length < 9_000_000) {
                // OK packet (no result set)
                var ok = decoder.readOkPacket(payload);
                // Handle multi-result: if SERVER_MORE_RESULTS_EXISTS, read next result
                if ((ok.serverStatus() & SERVER_MORE_RESULTS_EXISTS) != 0) {
                    // For pipelining, we only want one result per enqueued statement.
                    // Multi-statement results from a single COM_QUERY are consumed here.
                    return readRemainingResults(ok.affectedRows());
                }
                return new RowSet(List.of(), List.of(), ok.affectedRows());
            }

            // Result set: first packet is column count
            int columnCount = decoder.readColumnCount(payload);
            return readResultSet(columnCount);

        } catch (IOException e) {
            throw new RuntimeException("I/O error reading MySQL response", e);
        }
    }

    private RowSet readResultSet(int columnCount) throws IOException {
        // Read column definitions
        ColumnDescriptor[] columns = new ColumnDescriptor[columnCount];
        for (int i = 0; i < columnCount; i++) {
            byte[] colPayload = decoder.readPacket();
            columns[i] = decoder.readColumnDefinition(colPayload);
        }

        // Read EOF after column definitions (unless CLIENT_DEPRECATE_EOF)
        if (!decoder.isDeprecateEof()) {
            byte[] eofPayload = decoder.readPacket();
            // Should be EOF packet (0xFE)
        }

        // Read rows until EOF or OK
        List<Row> rows = new ArrayList<>();
        while (true) {
            byte[] rowPayload = decoder.readPacket();
            int firstByte = rowPayload[0] & 0xFF;

            if (firstByte == ERR_PACKET) {
                var err = decoder.readErrPacket(rowPayload);
                return new RowSet(MysqlException.fromErrPacket(err));
            }

            if (firstByte == EOF_PACKET && rowPayload.length < PACKET_PAYLOAD_LIMIT) {
                // EOF or OK_with_EOF_header — end of rows
                int serverStatus;
                int affectedRows = 0;
                if (decoder.isDeprecateEof()) {
                    var ok = decoder.readOkPacket(rowPayload);
                    serverStatus = ok.serverStatus();
                    affectedRows = ok.affectedRows();
                } else {
                    var eof = decoder.readEofPacket(rowPayload);
                    serverStatus = eof.serverStatus();
                }
                return new RowSet(rows, List.of(columns), affectedRows);
            }

            // Data row
            byte[][] values = decoder.readTextRow(rowPayload, columnCount);
            rows.add(new Row(columns, values));
        }
    }

    private RowSet readRemainingResults(int totalAffected) throws IOException {
        // Consume remaining result sets from multi-statement
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
                var err = decoder.readErrPacket(payload);
                return new RowSet(MysqlException.fromErrPacket(err));
            } else {
                // Another result set — read and discard
                int colCount = decoder.readColumnCount(payload);
                readResultSet(colCount); // discard
            }
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
        serverCapabilities = handshake.serverCapabilities();

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

        // Compute auth response
        String authPlugin = handshake.authPluginName();
        byte[] authResponse = computeAuthResponse(authPlugin, password, handshake.authPluginData());

        // Step 2: Send handshake response
        encoder.setSequenceId(decoder.lastSequenceId() + 1);
        encoder.writeHandshakeResponse(clientFlags, user, authResponse, database,
            authPlugin, handshake.charset(), null);
        encoder.flush(out);

        // Step 3: Read auth result
        handleAuthResult(password);
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
            case AUTH_SWITCH_REQUEST -> {
                handleAuthSwitch(password, payload);
            }
            case AUTH_MORE_DATA -> {
                handleAuthMoreData(password, payload);
            }
            default -> throw new RuntimeException("Unexpected auth response header: 0x" + Integer.toHexString(header));
        }
    }

    private void handleAuthMoreData(String password, byte[] payload) throws IOException {
        // caching_sha2_password sends 0x01 + status byte
        // status 0x03 = fast auth success, status 0x04 = full auth needed
        if (payload.length >= 2) {
            int status = payload[1] & 0xFF;
            if (status == 0x03) {
                // Fast auth succeeded — read the final OK packet
                handleAuthResult(password);
                return;
            } else if (status == 0x04) {
                // Full auth needed — send cleartext password (only safe over SSL)
                // For now, send the password in cleartext (assumes secure connection)
                byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
                byte[] passwordWithNull = new byte[passwordBytes.length + 1];
                System.arraycopy(passwordBytes, 0, passwordWithNull, 0, passwordBytes.length);

                encoder.setSequenceId(decoder.lastSequenceId() + 1);
                encoder.writeAuthResponse(passwordWithNull);
                encoder.flush(out);

                handleAuthResult(password);
                return;
            }
        }
        // Read next packet for final result
        handleAuthResult(password);
    }

    private void handleAuthSwitch(String password, byte[] payload) throws IOException {
        var buf = io.djb.impl.ByteBuffer.wrap(payload);
        buf.readByte(); // skip 0xFE
        String pluginName = MysqlDecoder.readNullTerminatedString(buf, StandardCharsets.UTF_8);
        byte[] nonce = new byte[NONCE_LENGTH];
        buf.readBytes(nonce);

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
            default -> {
                // Fallback to mysql_native_password
                yield Native41Authenticator.encode(passwordBytes, nonce);
            }
        };
    }
}
