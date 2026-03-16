package io.djb.pg.impl.codec;

import io.djb.ColumnDescriptor;
import io.djb.impl.ByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Decodes PostgreSQL backend (server → client) protocol messages from an InputStream.
 * Blocking: each readMessage() call blocks until a complete message is available.
 */
public final class PgDecoder {

    private final InputStream in;

    public PgDecoder(InputStream in) {
        this.in = in;
    }

    /**
     * Read one complete backend message. Blocks until available.
     */
    public BackendMessage readMessage() throws IOException {
        int type = readByte();
        if (type == -1) {
            throw new IOException("Connection closed by server");
        }
        int length = readInt();
        byte[] payload = readExactly(length - 4);
        ByteBuffer buf = ByteBuffer.wrap(payload);
        return decode((byte) type, buf);
    }

    /**
     * Read a single byte response (used for SSL negotiation).
     */
    public int readSingleByte() throws IOException {
        return readByte();
    }

    private BackendMessage decode(byte type, ByteBuffer buf) {
        return switch (type) {
            case PgProtocolConstants.AUTHENTICATION -> decodeAuthentication(buf);
            case PgProtocolConstants.PARAMETER_STATUS -> decodeParameterStatus(buf);
            case PgProtocolConstants.BACKEND_KEY_DATA -> decodeBackendKeyData(buf);
            case PgProtocolConstants.READY_FOR_QUERY -> decodeReadyForQuery(buf);
            case PgProtocolConstants.ROW_DESCRIPTION -> decodeRowDescription(buf);
            case PgProtocolConstants.DATA_ROW -> decodeDataRow(buf);
            case PgProtocolConstants.COMMAND_COMPLETE -> decodeCommandComplete(buf);
            case PgProtocolConstants.EMPTY_QUERY_RESPONSE -> new BackendMessage.EmptyQueryResponse();
            case PgProtocolConstants.ERROR_RESPONSE -> decodeErrorResponse(buf);
            case PgProtocolConstants.NOTICE_RESPONSE -> decodeNoticeResponse(buf);
            case PgProtocolConstants.PARSE_COMPLETE -> new BackendMessage.ParseComplete();
            case PgProtocolConstants.BIND_COMPLETE -> new BackendMessage.BindComplete();
            case PgProtocolConstants.CLOSE_COMPLETE -> new BackendMessage.CloseComplete();
            case PgProtocolConstants.NO_DATA -> new BackendMessage.NoData();
            case PgProtocolConstants.PORTAL_SUSPENDED -> new BackendMessage.PortalSuspended();
            case PgProtocolConstants.PARAMETER_DESCRIPTION -> decodeParameterDescription(buf);
            case PgProtocolConstants.NOTIFICATION_RESPONSE -> decodeNotificationResponse(buf);
            default -> throw new UnsupportedOperationException(
                "Unknown backend message type: " + (char) type + " (0x" + Integer.toHexString(type) + ")");
        };
    }

    private BackendMessage decodeAuthentication(ByteBuffer buf) {
        int authType = buf.readInt();
        return switch (authType) {
            case PgProtocolConstants.AUTH_OK -> new BackendMessage.AuthenticationOk();
            case PgProtocolConstants.AUTH_CLEARTEXT_PASSWORD -> new BackendMessage.AuthenticationCleartextPassword();
            case PgProtocolConstants.AUTH_MD5_PASSWORD -> {
                byte[] salt = new byte[4];
                buf.readBytes(salt);
                yield new BackendMessage.AuthenticationMD5Password(salt);
            }
            case PgProtocolConstants.AUTH_SASL -> {
                // Read null-terminated list of mechanism names
                StringBuilder mechanisms = new StringBuilder();
                while (buf.readableBytes() > 1) {
                    String mech = buf.readCString();
                    if (!mech.isEmpty()) {
                        if (!mechanisms.isEmpty()) mechanisms.append(",");
                        mechanisms.append(mech);
                    }
                }
                if (buf.readableBytes() > 0) buf.readByte(); // trailing null
                yield new BackendMessage.AuthenticationSASL(mechanisms.toString());
            }
            case PgProtocolConstants.AUTH_SASL_CONTINUE -> {
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                yield new BackendMessage.AuthenticationSASLContinue(data);
            }
            case PgProtocolConstants.AUTH_SASL_FINAL -> {
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                yield new BackendMessage.AuthenticationSASLFinal(data);
            }
            default -> throw new UnsupportedOperationException(
                "Authentication type " + authType + " is not supported");
        };
    }

    private BackendMessage decodeParameterStatus(ByteBuffer buf) {
        return new BackendMessage.ParameterStatus(buf.readCString(), buf.readCString());
    }

    private BackendMessage decodeBackendKeyData(ByteBuffer buf) {
        return new BackendMessage.BackendKeyData(buf.readInt(), buf.readInt());
    }

    private BackendMessage decodeReadyForQuery(ByteBuffer buf) {
        return new BackendMessage.ReadyForQuery((char) buf.readByte());
    }

    private BackendMessage decodeRowDescription(ByteBuffer buf) {
        int columnCount = buf.readUnsignedShort();
        ColumnDescriptor[] columns = new ColumnDescriptor[columnCount];
        for (int i = 0; i < columnCount; i++) {
            String name = buf.readCString();
            int tableOID = buf.readInt();
            short colAttr = buf.readShort();
            int typeOID = buf.readInt();
            short typeSize = buf.readShort();
            int typeMod = buf.readInt();
            int format = buf.readUnsignedShort(); // 0=text, 1=binary
            columns[i] = new ColumnDescriptor(name, tableOID, colAttr, typeOID, typeSize, typeMod, format);
        }
        return new BackendMessage.RowDescription(columns);
    }

    private BackendMessage decodeDataRow(ByteBuffer buf) {
        int columnCount = buf.readUnsignedShort();
        byte[][] values = new byte[columnCount][];
        for (int i = 0; i < columnCount; i++) {
            int len = buf.readInt();
            if (len == -1) {
                values[i] = null; // SQL NULL
            } else {
                values[i] = new byte[len];
                buf.readBytes(values[i]);
            }
        }
        return new BackendMessage.DataRow(values);
    }

    private BackendMessage decodeCommandComplete(ByteBuffer buf) {
        // Tag format: "INSERT 0 5", "UPDATE 3", "DELETE 1", "SELECT 10", etc.
        // We want the last number
        byte[] raw = new byte[buf.readableBytes()];
        buf.readBytes(raw);
        // Remove trailing null
        int end = raw.length;
        if (end > 0 && raw[end - 1] == 0) end--;
        String tag = new String(raw, 0, end, StandardCharsets.UTF_8);

        int rows = 0;
        boolean afterSpace = false;
        for (int i = 0; i < tag.length(); i++) {
            char c = tag.charAt(i);
            if (c == ' ') {
                afterSpace = true;
                rows = 0;
            } else if (afterSpace && c >= '0' && c <= '9') {
                rows = rows * 10 + (c - '0');
            } else {
                afterSpace = false;
            }
        }
        return new BackendMessage.CommandComplete(rows);
    }

    private BackendMessage decodeErrorResponse(ByteBuffer buf) {
        String severity = null, code = null, message = null, detail = null, hint = null;
        String position = null, where = null, file = null, line = null, routine = null;
        String schema = null, table = null, column = null, dataType = null, constraint = null;

        byte fieldType;
        while ((fieldType = buf.readByte()) != 0) {
            String value = buf.readCString();
            switch (fieldType) {
                case PgProtocolConstants.FIELD_SEVERITY -> severity = value;
                case PgProtocolConstants.FIELD_CODE -> code = value;
                case PgProtocolConstants.FIELD_MESSAGE -> message = value;
                case PgProtocolConstants.FIELD_DETAIL -> detail = value;
                case PgProtocolConstants.FIELD_HINT -> hint = value;
                case PgProtocolConstants.FIELD_POSITION -> position = value;
                case PgProtocolConstants.FIELD_WHERE -> where = value;
                case PgProtocolConstants.FIELD_FILE -> file = value;
                case PgProtocolConstants.FIELD_LINE -> line = value;
                case PgProtocolConstants.FIELD_ROUTINE -> routine = value;
                case PgProtocolConstants.FIELD_SCHEMA -> schema = value;
                case PgProtocolConstants.FIELD_TABLE -> table = value;
                case PgProtocolConstants.FIELD_COLUMN -> column = value;
                case PgProtocolConstants.FIELD_DATA_TYPE -> dataType = value;
                case PgProtocolConstants.FIELD_CONSTRAINT -> constraint = value;
                default -> {} // skip unknown fields
            }
        }
        return new BackendMessage.ErrorResponse(severity, code, message, detail, hint,
            position, where, file, line, routine, schema, table, column, dataType, constraint);
    }

    private BackendMessage decodeNoticeResponse(ByteBuffer buf) {
        String severity = null, code = null, message = null, detail = null, hint = null;
        byte fieldType;
        while ((fieldType = buf.readByte()) != 0) {
            String value = buf.readCString();
            switch (fieldType) {
                case PgProtocolConstants.FIELD_SEVERITY -> severity = value;
                case PgProtocolConstants.FIELD_CODE -> code = value;
                case PgProtocolConstants.FIELD_MESSAGE -> message = value;
                case PgProtocolConstants.FIELD_DETAIL -> detail = value;
                case PgProtocolConstants.FIELD_HINT -> hint = value;
                default -> {} // skip
            }
        }
        return new BackendMessage.NoticeResponse(severity, code, message, detail, hint);
    }

    private BackendMessage decodeParameterDescription(ByteBuffer buf) {
        int count = buf.readUnsignedShort();
        int[] oids = new int[count];
        for (int i = 0; i < count; i++) {
            oids[i] = buf.readInt();
        }
        return new BackendMessage.ParameterDescription(oids);
    }

    private BackendMessage decodeNotificationResponse(ByteBuffer buf) {
        int pid = buf.readInt();
        String channel = buf.readCString();
        String payload = buf.readCString();
        return new BackendMessage.NotificationResponse(pid, channel, payload);
    }

    // --- Low-level I/O helpers ---

    private int readByte() throws IOException {
        return in.read();
    }

    private int readInt() throws IOException {
        byte[] b = readExactly(4);
        return (b[0] & 0xFF) << 24 | (b[1] & 0xFF) << 16 | (b[2] & 0xFF) << 8 | (b[3] & 0xFF);
    }

    private byte[] readExactly(int n) throws IOException {
        byte[] buf = new byte[n];
        int offset = 0;
        while (offset < n) {
            int read = in.read(buf, offset, n - offset);
            if (read == -1) {
                throw new IOException("Unexpected end of stream (needed " + n + " bytes, got " + offset + ")");
            }
            offset += read;
        }
        return buf;
    }
}
