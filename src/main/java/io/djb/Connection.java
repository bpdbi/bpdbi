package io.djb;

import io.djb.impl.auth.MD5Authentication;
import io.djb.impl.codec.BackendMessage;
import io.djb.impl.codec.PgDecoder;
import io.djb.impl.codec.PgEncoder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A blocking PostgreSQL connection with first-class pipelining support.
 *
 * <p>Not thread-safe. Designed for one-connection-per-(virtual-)thread usage.</p>
 */
public final class Connection implements AutoCloseable {

    private final Socket socket;
    private final OutputStream out;
    private final PgEncoder encoder;
    private final PgDecoder decoder;
    private final Map<String, String> parameters = new HashMap<>();
    private int processId;
    private int secretKey;

    // Pipeline state
    private final List<PendingStatement> pending = new ArrayList<>();

    private Connection(Socket socket, OutputStream out, PgEncoder encoder, PgDecoder decoder) {
        this.socket = socket;
        this.out = out;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    /**
     * Connect to a PostgreSQL server.
     */
    public static Connection connect(String host, int port, String database, String user, String password) {
        return connect(host, port, database, user, password, null);
    }

    /**
     * Connect to a PostgreSQL server with optional properties.
     */
    public static Connection connect(String host, int port, String database, String user, String password,
                                     Map<String, String> properties) {
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            var out = new BufferedOutputStream(socket.getOutputStream(), 8192);
            var in = new BufferedInputStream(socket.getInputStream(), 8192);
            var encoder = new PgEncoder();
            var decoder = new PgDecoder(in);

            var conn = new Connection(socket, out, encoder, decoder);
            conn.performStartup(user, database, password, properties);
            return conn;
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to PostgreSQL at " + host + ":" + port, e);
        }
    }

    private void performStartup(String user, String database, String password, Map<String, String> properties) throws IOException {
        encoder.writeStartupMessage(user, database, properties);
        encoder.flush(out);

        boolean done = false;
        while (!done) {
            BackendMessage msg = decoder.readMessage();
            switch (msg) {
                case BackendMessage.AuthenticationOk ok -> {
                    // auth complete, continue reading parameter status etc.
                }
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
                    // Verify UTF-8 encoding
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
                case BackendMessage.NoticeResponse notice -> {
                    // ignore notices during startup
                }
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

            // Step 1: Send client-first-message
            var clientFirstMsg = scramClient.clientFirstMessage();
            encoder.writeScramInitialMessage(scramClient.getScramMechanism().getName(),
                clientFirstMsg.toString());
            encoder.flush(out);

            // Step 2: Receive server-first-message, send client-final-message
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

            // Step 3: Receive server-final-message (validates server signature)
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

    // --- Query API ---

    /**
     * Execute a single SQL statement and return the result.
     * Also flushes any previously enqueued pipeline statements.
     */
    public RowSet query(String sql) {
        enqueue(sql);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    /**
     * Execute a parameterized SQL statement and return the result.
     * Also flushes any previously enqueued pipeline statements.
     */
    public RowSet query(String sql, Object... params) {
        enqueue(sql, params);
        var result = flush().getLast();
        if (result.hasError()) throw result.getError();
        return result;
    }

    /**
     * Enqueue a simple (non-parameterized) SQL statement for pipelining.
     * @return the index into the results list that flush() will return
     */
    public int enqueue(String sql) {
        int index = pending.size();
        pending.add(new PendingStatement(sql, null, true));
        return index;
    }

    /**
     * Enqueue a parameterized SQL statement for pipelining.
     * @return the index into the results list that flush() will return
     */
    public int enqueue(String sql, Object... params) {
        int index = pending.size();
        String[] textParams = new String[params.length];
        for (int i = 0; i < params.length; i++) {
            textParams[i] = params[i] == null ? null : params[i].toString();
        }
        pending.add(new PendingStatement(sql, textParams, false));
        return index;
    }

    /**
     * Flush all enqueued statements in a single TCP write, then read all responses.
     * @return a list of RowSet, one per enqueued statement, in order
     */
    public List<RowSet> flush() {
        if (pending.isEmpty()) {
            return List.of();
        }

        List<PendingStatement> toFlush = new ArrayList<>(pending);
        pending.clear();

        try {
            // Phase 1: Encode all statements into one buffer
            for (PendingStatement stmt : toFlush) {
                if (stmt.simple) {
                    encoder.writeQuery(stmt.sql);
                } else {
                    encoder.writeParse(stmt.sql, null);
                    encoder.writeBind(stmt.paramValues);
                    encoder.writeDescribePortal();
                    encoder.writeExecute();
                    encoder.writeSync();
                }
            }

            // Phase 2: Single TCP write
            encoder.flush(out);

            // Phase 3: Read responses for each statement
            List<RowSet> results = new ArrayList<>(toFlush.size());
            for (PendingStatement stmt : toFlush) {
                if (stmt.simple) {
                    results.add(readSimpleQueryResponse());
                } else {
                    results.add(readExtendedQueryResponse());
                }
            }
            return results;

        } catch (IOException e) {
            throw new RuntimeException("I/O error during flush", e);
        }
    }

    private RowSet readSimpleQueryResponse() throws IOException {
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
                    // Read until ReadyForQuery, then throw
                    drainUntilReady();
                    return new RowSet(PgException.fromErrorResponse(err));
                }
                case BackendMessage.NoticeResponse notice -> {} // skip
                case BackendMessage.ReadyForQuery rq -> {
                    return new RowSet(rows,
                        columns != null ? List.of(columns) : List.of(),
                        rowsAffected);
                }
                default -> {}
            }
        }
    }

    private RowSet readExtendedQueryResponse() throws IOException {
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
                case BackendMessage.NoticeResponse notice -> {} // skip
                case BackendMessage.ReadyForQuery rq -> {
                    return new RowSet(rows,
                        columns != null ? List.of(columns) : List.of(),
                        rowsAffected);
                }
                default -> {}
            }
        }
    }

    private void drainUntilReady() throws IOException {
        while (true) {
            BackendMessage msg = decoder.readMessage();
            if (msg instanceof BackendMessage.ReadyForQuery) return;
        }
    }

    // --- Lifecycle ---

    @Override
    public void close() {
        try {
            encoder.writeTerminate();
            encoder.flush(out);
            socket.close();
        } catch (IOException e) {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    public Map<String, String> parameters() {
        return Collections.unmodifiableMap(parameters);
    }

    public int processId() {
        return processId;
    }

    // --- Internal ---

    private record PendingStatement(String sql, String[] paramValues, boolean simple) {}
}
