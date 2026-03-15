package io.djb.impl.codec;

import io.djb.impl.buffer.PgBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Encodes PostgreSQL frontend (client → server) protocol messages into a PgBuffer.
 * Messages are accumulated and flushed as a single write.
 */
public final class PgEncoder {

    private final PgBuffer buf = new PgBuffer(1024);

    // --- Startup / Auth messages (no type byte prefix) ---

    public void writeStartupMessage(String user, String database, Map<String, String> properties) {
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder
        buf.writeShort(3); // protocol major
        buf.writeShort(0); // protocol minor
        buf.writeCString("user");
        buf.writeCString(user);
        buf.writeCString("database");
        buf.writeCString(database);
        if (properties != null) {
            for (var entry : properties.entrySet()) {
                buf.writeCString(entry.getKey());
                buf.writeCString(entry.getValue());
            }
        }
        buf.writeByte(0); // terminator
        buf.setInt(pos, buf.writerIndex() - pos);
    }

    public void writeSSLRequest() {
        buf.writeInt(8); // message length
        buf.writeInt(80877103); // SSL request code
    }

    // --- Frontend messages with type byte ---

    public void writePasswordMessage(String password) {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.PASSWORD);
        buf.writeInt(0); // length placeholder
        buf.writeCString(password);
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    public void writeScramInitialMessage(String mechanism, String clientFirstMessage) {
        buf.writeByte(PgProtocolConstants.PASSWORD);
        int totalPos = buf.writerIndex();
        buf.writeInt(0); // total length placeholder

        buf.writeCString(mechanism);
        int msgPos = buf.writerIndex();
        buf.writeInt(0); // client-first-message length placeholder
        buf.writeString(clientFirstMessage);

        buf.setInt(msgPos, buf.writerIndex() - msgPos - 4);
        buf.setInt(totalPos, buf.writerIndex() - totalPos);
    }

    public void writeScramFinalMessage(String clientFinalMessage) {
        buf.writeByte(PgProtocolConstants.PASSWORD);
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder
        buf.writeString(clientFinalMessage);
        buf.setInt(pos, buf.writerIndex() - pos);
    }

    public void writeQuery(String sql) {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.QUERY);
        buf.writeInt(0); // length placeholder
        buf.writeCString(sql);
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    public void writeParse(String sql, int[] paramTypeOIDs) {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.PARSE);
        buf.writeInt(0); // length placeholder
        buf.writeByte(0); // unnamed statement (empty string)
        buf.writeCString(sql);
        if (paramTypeOIDs == null) {
            buf.writeShort(0);
        } else {
            buf.writeShort(paramTypeOIDs.length);
            for (int oid : paramTypeOIDs) {
                buf.writeInt(oid);
            }
        }
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    /**
     * Write Bind message. Parameters are encoded as text format for simplicity in MVP.
     * Binary encoding can be added later for performance.
     */
    public void writeBind(String[] paramValues) {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.BIND);
        buf.writeInt(0); // length placeholder
        buf.writeByte(0); // unnamed portal
        buf.writeByte(0); // unnamed statement
        // Parameter format codes: all text (0)
        buf.writeShort(0); // 0 = use default (text) for all
        // Parameter values
        int paramCount = paramValues == null ? 0 : paramValues.length;
        buf.writeShort(paramCount);
        for (int i = 0; i < paramCount; i++) {
            if (paramValues[i] == null) {
                buf.writeInt(-1); // NULL
            } else {
                byte[] bytes = paramValues[i].getBytes(UTF_8);
                buf.writeInt(bytes.length);
                buf.writeBytes(bytes);
            }
        }
        // Result format codes: all text (0)
        buf.writeShort(0); // 0 = use default (text) for all results
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    public void writeDescribePortal() {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.DESCRIBE);
        buf.writeInt(0);
        buf.writeByte('P'); // Portal
        buf.writeByte(0); // unnamed
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    public void writeExecute() {
        int pos = buf.writerIndex();
        buf.writeByte(PgProtocolConstants.EXECUTE);
        buf.writeInt(0);
        buf.writeByte(0); // unnamed portal
        buf.writeInt(0); // no row limit
        buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
    }

    public void writeSync() {
        buf.writeByte(PgProtocolConstants.SYNC);
        buf.writeInt(4);
    }

    public void writeTerminate() {
        buf.writeByte(PgProtocolConstants.TERMINATE);
        buf.writeInt(4);
    }

    /**
     * Flush all accumulated messages to the output stream as a single write, then clear the buffer.
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
}
