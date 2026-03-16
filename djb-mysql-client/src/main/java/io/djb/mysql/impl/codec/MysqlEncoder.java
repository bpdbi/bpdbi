package io.djb.mysql.impl.codec;

import io.djb.impl.ByteBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.djb.mysql.impl.codec.MysqlProtocolConstants.*;

/**
 * Encodes MySQL frontend (client → server) protocol messages.
 * MySQL packets: [3-byte length LE] [1-byte sequence ID] [payload]
 */
public final class MysqlEncoder {

    private final ByteBuffer buf = new ByteBuffer(1024);
    private int sequenceId = 0;

    public void resetSequenceId() {
        this.sequenceId = 0;
    }

    public int sequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int id) {
        this.sequenceId = id;
    }

    // --- Packet writing helpers ---

    private void beginPacket() {
        // Write placeholder for 3-byte length (LE) + sequence ID
        buf.writeByte(0);
        buf.writeByte(0);
        buf.writeByte(0);
        buf.writeByte(sequenceId++);
    }

    private void finishPacket(int packetStart) {
        int payloadLength = buf.writerIndex() - packetStart - 4;
        // Set 3-byte little-endian length
        buf.array()[packetStart] = (byte) (payloadLength & 0xFF);
        buf.array()[packetStart + 1] = (byte) ((payloadLength >> 8) & 0xFF);
        buf.array()[packetStart + 2] = (byte) ((payloadLength >> 16) & 0xFF);
    }

    // --- MySQL-specific encoding helpers ---

    private void writeLengthEncodedInt(long value) {
        if (value < 251) {
            buf.writeByte((int) value);
        } else if (value <= 0xFFFF) {
            buf.writeByte(0xFC);
            buf.writeByte((int) (value & 0xFF));
            buf.writeByte((int) ((value >> 8) & 0xFF));
        } else if (value < 0xFFFFFF) {
            buf.writeByte(0xFD);
            buf.writeByte((int) (value & 0xFF));
            buf.writeByte((int) ((value >> 8) & 0xFF));
            buf.writeByte((int) ((value >> 16) & 0xFF));
        } else {
            buf.writeByte(0xFE);
            writeLongLE(value);
        }
    }

    private void writeIntLE(int value) {
        buf.writeByte(value & 0xFF);
        buf.writeByte((value >> 8) & 0xFF);
        buf.writeByte((value >> 16) & 0xFF);
        buf.writeByte((value >> 24) & 0xFF);
    }

    private void writeShortLE(int value) {
        buf.writeByte(value & 0xFF);
        buf.writeByte((value >> 8) & 0xFF);
    }

    private void writeLongLE(long value) {
        for (int i = 0; i < 8; i++) {
            buf.writeByte((int) ((value >> (i * 8)) & 0xFF));
        }
    }

    private void writeNullTerminatedString(String s, Charset charset) {
        byte[] bytes = s.getBytes(charset);
        buf.writeBytes(bytes);
        buf.writeByte(0);
    }

    private void writeZero(int count) {
        for (int i = 0; i < count; i++) {
            buf.writeByte(0);
        }
    }

    // --- Protocol messages ---

    /**
     * Write handshake response (authentication) packet.
     */
    public void writeHandshakeResponse(int clientFlags, String username, byte[] authResponse,
                                        String database, String authPluginName,
                                        int collationId, Map<String, String> connectionAttributes) {
        int start = buf.writerIndex();
        beginPacket();

        writeIntLE(clientFlags);
        writeIntLE(PACKET_PAYLOAD_LIMIT);
        buf.writeByte(collationId);
        writeZero(23); // filler

        writeNullTerminatedString(username, StandardCharsets.UTF_8);

        if (authResponse != null) {
            if ((clientFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                writeLengthEncodedInt(authResponse.length);
                buf.writeBytes(authResponse);
            } else if ((clientFlags & CLIENT_SECURE_CONNECTION) != 0) {
                buf.writeByte(authResponse.length);
                buf.writeBytes(authResponse);
            } else {
                buf.writeByte(0);
            }
        } else {
            buf.writeByte(0);
        }

        if ((clientFlags & CLIENT_CONNECT_WITH_DB) != 0 && database != null) {
            writeNullTerminatedString(database, StandardCharsets.UTF_8);
        }
        if ((clientFlags & CLIENT_PLUGIN_AUTH) != 0 && authPluginName != null) {
            writeNullTerminatedString(authPluginName, StandardCharsets.UTF_8);
        }

        finishPacket(start);
    }

    /**
     * Write a COM_QUERY packet (simple text query).
     */
    public void writeComQuery(String sql, Charset charset) {
        int start = buf.writerIndex();
        beginPacket();
        buf.writeByte(COM_QUERY);
        byte[] sqlBytes = sql.getBytes(charset);
        buf.writeBytes(sqlBytes);
        finishPacket(start);
    }

    /**
     * Write a COM_STMT_PREPARE packet.
     */
    public void writeComStmtPrepare(String sql, Charset charset) {
        int start = buf.writerIndex();
        beginPacket();
        buf.writeByte(COM_STMT_PREPARE);
        byte[] sqlBytes = sql.getBytes(charset);
        buf.writeBytes(sqlBytes);
        finishPacket(start);
    }

    /**
     * Write a COM_STMT_EXECUTE packet with text-encoded parameters.
     * For simplicity, we use COM_QUERY with parameter interpolation in the MVP.
     * This method sends a COM_QUERY with the SQL as-is (parameters already interpolated).
     */
    public void writeComStmtExecuteAsQuery(String sql, String[] params, Charset charset) {
        // For MVP: use COM_QUERY with MySQL ? placeholder replaced by escaped values
        // This avoids the complexity of the binary protocol for now
        String interpolated = interpolateParams(sql, params);
        writeComQuery(interpolated, charset);
    }

    /**
     * Write a COM_QUIT packet.
     */
    public void writeComQuit() {
        int start = buf.writerIndex();
        beginPacket();
        buf.writeByte(COM_QUIT);
        finishPacket(start);
    }

    /**
     * Write an auth response (during auth switch or auth more data).
     */
    public void writeAuthResponse(byte[] authData) {
        int start = buf.writerIndex();
        beginPacket();
        buf.writeBytes(authData);
        finishPacket(start);
    }

    /**
     * Flush all accumulated packets to the output stream as a single write.
     */
    public void flush(OutputStream out) throws IOException {
        if (buf.writerIndex() > 0) {
            out.write(buf.array(), 0, buf.writerIndex());
            out.flush();
            buf.clear();
        }
    }

    public void clear() {
        buf.clear();
    }

    // --- Helpers ---

    /**
     * Interpolate parameters into SQL by replacing ? placeholders.
     * Escapes strings for MySQL. Used for COM_QUERY-based parameterized queries.
     */
    static String interpolateParams(String sql, String[] params) {
        if (params == null || params.length == 0) return sql;

        var sb = new StringBuilder(sql.length() + params.length * 10);
        int paramIdx = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean escaped = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (escaped) {
                sb.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                sb.append(c);
                escaped = true;
                continue;
            }
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                sb.append(c);
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                sb.append(c);
            } else if (c == '?' && !inSingleQuote && !inDoubleQuote && paramIdx < params.length) {
                String val = params[paramIdx++];
                if (val == null) {
                    sb.append("NULL");
                } else {
                    sb.append('\'');
                    sb.append(escapeString(val));
                    sb.append('\'');
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    static String escapeString(String s) {
        var sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\0' -> sb.append("\\0");
                case '\'' -> sb.append("\\'");
                case '"' -> sb.append("\\\"");
                case '\b' -> sb.append("\\b");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\\' -> sb.append("\\\\");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
