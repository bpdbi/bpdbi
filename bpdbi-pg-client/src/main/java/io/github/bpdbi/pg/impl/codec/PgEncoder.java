package io.github.bpdbi.pg.impl.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Encodes Postgres frontend (client → server) protocol messages into a ByteBuffer. Messages are
 * accumulated and flushed as a single write.
 */
public final class PgEncoder {

  private final ByteBuffer buf = new ByteBuffer(1024);

  // --- Startup / Auth messages (no type byte prefix) ---

  public void writeStartupMessage(
      @NonNull String user, @NonNull String database, @Nullable Map<String, String> properties) {
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

  public void writePasswordMessage(@NonNull String password) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.PASSWORD);
    buf.writeInt(0); // length placeholder
    buf.writeCString(password);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeScramInitialMessage(
      @NonNull String mechanism, @NonNull String clientFirstMessage) {
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

  public void writeScramFinalMessage(@NonNull String clientFinalMessage) {
    buf.writeByte(PgProtocolConstants.PASSWORD);
    int pos = buf.writerIndex();
    buf.writeInt(0); // length placeholder
    buf.writeString(clientFinalMessage);
    buf.setInt(pos, buf.writerIndex() - pos);
  }

  public void writeQuery(@NonNull String sql) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.QUERY);
    buf.writeInt(0); // length placeholder
    buf.writeCString(sql);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeParse(@NonNull String sql, int @Nullable [] paramTypeOIDs) {
    writeParse("", sql, paramTypeOIDs);
  }

  public void writeParse(
      @NonNull String statementName, @NonNull String sql, int @Nullable [] paramTypeOIDs) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.PARSE);
    buf.writeInt(0); // length placeholder
    buf.writeCString(statementName);
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

  /** Write Bind message with text parameters and binary results. */
  public void writeBind(@Nullable String @Nullable [] paramValues) {
    writeBind("", "", paramValues);
  }

  public void writeBind(
      @NonNull String portal,
      @NonNull String statementName,
      @Nullable String @Nullable [] paramValues) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.BIND);
    buf.writeInt(0); // length placeholder
    buf.writeCString(portal);
    buf.writeCString(statementName);
    // Parameter format codes: all text (0) — text params for now
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
    // Result format codes: request all binary (1)
    buf.writeShort(1); // 1 format code applies to all columns
    buf.writeShort(1); // binary format
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeDescribePortal() {
    writeDescribePortal("");
  }

  public void writeDescribePortal(@NonNull String portalName) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.DESCRIBE);
    buf.writeInt(0);
    buf.writeByte('P'); // Portal
    buf.writeCString(portalName);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeExecute() {
    writeExecute("", 0);
  }

  public void writeExecute(@NonNull String portal, int maxRows) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.EXECUTE);
    buf.writeInt(0);
    buf.writeCString(portal);
    buf.writeInt(maxRows);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeCloseStatement(@NonNull String statementName) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.CLOSE);
    buf.writeInt(0);
    buf.writeByte('S');
    buf.writeCString(statementName);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeClosePortal(@NonNull String portalName) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.CLOSE);
    buf.writeInt(0);
    buf.writeByte('P');
    buf.writeCString(portalName);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeDescribeStatement(@NonNull String statementName) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.DESCRIBE);
    buf.writeInt(0);
    buf.writeByte('S');
    buf.writeCString(statementName);
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

  public void writeCancelRequest(int processId, int secretKey) {
    buf.writeInt(16); // message length
    buf.writeInt(80877102); // cancel request code
    buf.writeInt(processId);
    buf.writeInt(secretKey);
  }

  /**
   * Flush all accumulated messages to the output stream as a single write, then clear the buffer.
   */
  public void flush(@NonNull OutputStream out) throws IOException {
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
