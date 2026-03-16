package io.djb.pg;

import io.djb.*;
import io.djb.impl.BaseConnection;
import io.djb.pg.impl.auth.MD5Authentication;
import io.djb.pg.impl.codec.BackendMessage;
import io.djb.pg.impl.codec.PgDecoder;
import io.djb.pg.impl.codec.PgEncoder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * PostgreSQL implementation of {@link io.djb.Connection}.
 */
public final class PgConnection extends BaseConnection {

    private final Socket socket;
    private final OutputStream out;
    private final PgEncoder encoder;
    private final PgDecoder decoder;
    private final Map<String, String> parameters = new HashMap<>();
    private int processId;
    private int secretKey;
    private int stmtCounter = 0;

    private PgConnection(Socket socket, OutputStream out, PgEncoder encoder, PgDecoder decoder) {
        this.socket = socket;
        this.out = out;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    /**
     * Connect to a PostgreSQL server.
     */
    public static PgConnection connect(String host, int port, String database, String user, String password) {
        return connect(host, port, database, user, password, null);
    }

    /**
     * Connect to a PostgreSQL server with optional properties.
     */
    public static PgConnection connect(String host, int port, String database, String user, String password,
                                       Map<String, String> properties) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            var out = new BufferedOutputStream(socket.getOutputStream(), 8192);
            var in = new BufferedInputStream(socket.getInputStream(), 8192);
            var encoder = new PgEncoder();
            var decoder = new PgDecoder(in);

            var conn = new PgConnection(socket, out, encoder, decoder);
            conn.performStartup(user, database, password, properties);
            return conn;
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to PostgreSQL at " + host + ":" + port, e);
        }
    }

    /**
     * Connect using a ConnectionConfig (supports URI parsing).
     */
    public static PgConnection connect(ConnectionConfig config) {
        return connect(config.host(), config.port() > 0 ? config.port() : 5432,
            config.database(), config.username(), config.password(), config.properties());
    }

    @Override
    public Map<String, String> parameters() {
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
    public PreparedStatement prepare(String sql) {
        String stmtName = "_djb_s" + (stmtCounter++);
        try {
            encoder.writeParse(stmtName, sql, null);
            encoder.writeDescribeStatement(stmtName);
            encoder.writeSync();
            encoder.flush(out);

            // Read ParseComplete, ParameterDescription/RowDescription/NoData, ReadyForQuery
            ColumnDescriptor[] columns = null;
            while (true) {
                BackendMessage msg = decoder.readMessage();
                switch (msg) {
                    case BackendMessage.ParseComplete pc -> {}
                    case BackendMessage.ParameterDescription pd -> {}
                    case BackendMessage.RowDescription rd -> columns = rd.columns();
                    case BackendMessage.NoData nd -> {}
                    case BackendMessage.ErrorResponse err -> {
                        drainUntilReady();
                        throw PgException.fromErrorResponse(err);
                    }
                    case BackendMessage.ReadyForQuery rq -> {
                        final ColumnDescriptor[] cols = columns;
                        return new PgPreparedStatement(stmtName, cols);
                    }
                    default -> {}
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error preparing statement", e);
        }
    }

    @Override
    public io.djb.Cursor cursor(String sql, Object... params) {
        String portalName = "_djb_p" + (stmtCounter++);
        String[] textParams = null;
        if (params != null && params.length > 0) {
            textParams = new String[params.length];
            for (int i = 0; i < params.length; i++) {
                textParams[i] = params[i] == null ? null : params[i].toString();
            }
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
                    case BackendMessage.ParseComplete pc -> {}
                    case BackendMessage.BindComplete bc -> {}
                    case BackendMessage.RowDescription rd -> columns = rd.columns();
                    case BackendMessage.NoData nd -> {}
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
            throw new RuntimeException("I/O error creating cursor", e);
        }
    }

    /**
     * Cancel a running query on this connection (sends cancel request via a new TCP connection).
     */
    public void cancelRequest() {
        try (var cancelSocket = new Socket(socket.getInetAddress(), socket.getPort())) {
            var cancelEncoder = new PgEncoder();
            cancelEncoder.writeCancelRequest(processId, secretKey);
            cancelEncoder.flush(cancelSocket.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Failed to send cancel request", e);
        }
    }

    // --- Inner classes for prepared statements and cursors ---

    private final class PgPreparedStatement implements PreparedStatement {
        private final String name;
        private final ColumnDescriptor[] columns;
        private boolean closed = false;

        PgPreparedStatement(String name, ColumnDescriptor[] columns) {
            this.name = name;
            this.columns = columns;
        }

        @Override
        public RowSet query(Object... params) {
            if (closed) throw new IllegalStateException("PreparedStatement is closed");
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
                throw new RuntimeException("I/O error executing prepared statement", e);
            }
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
                        if (msg instanceof BackendMessage.ReadyForQuery) break;
                    }
                } catch (IOException e) {
                    // best effort
                }
            }
        }
    }

    private final class PgCursor implements io.djb.Cursor {
        private final String portalName;
        private final ColumnDescriptor[] columns;
        private boolean hasMore = true;
        private boolean closed = false;

        PgCursor(String portalName, ColumnDescriptor[] columns) {
            this.portalName = portalName;
            this.columns = columns;
        }

        @Override
        public RowSet read(int count) {
            if (closed) throw new IllegalStateException("Cursor is closed");
            if (!hasMore) return new RowSet(List.of(), columns != null ? List.of(columns) : List.of(), 0);
            try {
                encoder.writeExecute(portalName, count);
                encoder.writeSync();
                encoder.flush(out);

                List<Row> rows = new ArrayList<>();
                int rowsAffected = 0;
                while (true) {
                    BackendMessage msg = decoder.readMessage();
                    switch (msg) {
                        case BackendMessage.DataRow dr -> rows.add(new Row(columns, dr.values()));
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
                            return new RowSet(rows, columns != null ? List.of(columns) : List.of(), rowsAffected);
                        }
                        default -> {}
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("I/O error reading cursor", e);
            }
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
                        if (msg instanceof BackendMessage.ReadyForQuery) break;
                    }
                } catch (IOException e) {
                    // best effort
                }
            }
        }
    }

    // --- BaseConnection protocol methods ---

    @Override
    protected String placeholderPrefix() {
        return "$";
    }

    @Override
    protected void encodeSimpleQuery(String sql) {
        encoder.writeQuery(sql);
    }

    @Override
    protected void encodeExtendedQuery(String sql, String[] params) {
        encoder.writeParse(sql, null);
        encoder.writeBind(params);
        encoder.writeDescribePortal();
        encoder.writeExecute();
        encoder.writeSync();
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
        try {
            ColumnDescriptor[] columns = null;
            List<Row> rows = new ArrayList<>();
            int rowsAffected = 0;

            while (true) {
                BackendMessage msg = decoder.readMessage();
                switch (msg) {
                    case BackendMessage.RowDescription rd -> columns = rd.columns();
                    case BackendMessage.DataRow dr -> rows.add(new Row(columns, dr.values()));
                    case BackendMessage.CommandComplete cc -> rowsAffected = cc.rowsAffected();
                    case BackendMessage.EmptyQueryResponse eq -> {}
                    case BackendMessage.ErrorResponse err -> {
                        drainUntilReady();
                        return new RowSet(PgException.fromErrorResponse(err));
                    }
                    case BackendMessage.NoticeResponse notice -> {}
                    case BackendMessage.ReadyForQuery rq -> {
                        return new RowSet(rows,
                            columns != null ? List.of(columns) : List.of(),
                            rowsAffected);
                    }
                    default -> {}
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error reading response", e);
        }
    }

    @Override
    protected RowSet readExtendedQueryResponse() {
        try {
            ColumnDescriptor[] columns = null;
            List<Row> rows = new ArrayList<>();
            int rowsAffected = 0;

            while (true) {
                BackendMessage msg = decoder.readMessage();
                switch (msg) {
                    case BackendMessage.ParseComplete pc -> {}
                    case BackendMessage.BindComplete bc -> {}
                    case BackendMessage.RowDescription rd -> columns = rd.columns();
                    case BackendMessage.NoData nd -> {}
                    case BackendMessage.DataRow dr -> rows.add(new Row(columns, dr.values()));
                    case BackendMessage.CommandComplete cc -> rowsAffected = cc.rowsAffected();
                    case BackendMessage.EmptyQueryResponse eq -> {}
                    case BackendMessage.PortalSuspended ps -> {}
                    case BackendMessage.ErrorResponse err -> {
                        drainUntilReady();
                        return new RowSet(PgException.fromErrorResponse(err));
                    }
                    case BackendMessage.NoticeResponse notice -> {}
                    case BackendMessage.ReadyForQuery rq -> {
                        return new RowSet(rows,
                            columns != null ? List.of(columns) : List.of(),
                            rowsAffected);
                    }
                    default -> {}
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("I/O error reading response", e);
        }
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

    private void performStartup(String user, String database, String password, Map<String, String> properties) throws IOException {
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
                case BackendMessage.AuthenticationSASL sasl -> {
                    handleSaslAuth(sasl, user, password);
                }
                case BackendMessage.ParameterStatus ps -> {
                    parameters.put(ps.name(), ps.value());
                }
                case BackendMessage.BackendKeyData kd -> {
                    processId = kd.processId();
                    secretKey = kd.secretKey();
                }
                case BackendMessage.ReadyForQuery rq -> {
                    String encoding = parameters.get("client_encoding");
                    if (encoding != null && !encoding.equalsIgnoreCase("UTF8")) {
                        throw new PgException("FATAL", "08000",
                            encoding + " encoding is not supported, only UTF8", null, null);
                    }
                    done = true;
                }
                case BackendMessage.ErrorResponse err -> {
                    throw PgException.fromErrorResponse(err);
                }
                case BackendMessage.NoticeResponse notice -> {}
                default -> throw new RuntimeException("Unexpected message during startup: " + msg.getClass().getSimpleName());
            }
        }
    }

    private void handleSaslAuth(BackendMessage.AuthenticationSASL sasl, String user, String password) throws IOException {
        try {
            var scramClient = com.ongres.scram.client.ScramClient.builder()
                .advertisedMechanisms(List.of(sasl.mechanisms().split(",")))
                .username(user)
                .password(password.toCharArray())
                .build();

            var clientFirstMsg = scramClient.clientFirstMessage();
            encoder.writeScramInitialMessage(scramClient.getScramMechanism().getName(),
                clientFirstMsg.toString());
            encoder.flush(out);

            BackendMessage msg = decoder.readMessage();
            if (msg instanceof BackendMessage.ErrorResponse err) {
                throw PgException.fromErrorResponse(err);
            }
            if (!(msg instanceof BackendMessage.AuthenticationSASLContinue cont)) {
                throw new RuntimeException("Expected AuthenticationSASLContinue, got: " + msg);
            }
            scramClient.serverFirstMessage(new String(cont.data(), StandardCharsets.UTF_8));
            var clientFinalMsg = scramClient.clientFinalMessage();

            encoder.writeScramFinalMessage(clientFinalMsg.toString());
            encoder.flush(out);

            msg = decoder.readMessage();
            if (msg instanceof BackendMessage.ErrorResponse err) {
                throw PgException.fromErrorResponse(err);
            }
            if (!(msg instanceof BackendMessage.AuthenticationSASLFinal fin)) {
                throw new RuntimeException("Expected AuthenticationSASLFinal, got: " + msg);
            }
            scramClient.serverFinalMessage(new String(fin.data(), StandardCharsets.UTF_8));

        } catch (com.ongres.scram.common.exception.ScramException e) {
            throw new PgException("FATAL", "28P01", "SCRAM authentication failed: " + e.getMessage(), null, null);
        }
    }

    private void drainUntilReady() throws IOException {
        while (true) {
            BackendMessage msg = decoder.readMessage();
            if (msg instanceof BackendMessage.ReadyForQuery) return;
        }
    }
}
