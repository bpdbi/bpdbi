package io.github.bpdbi.pg.impl.codec;

import io.github.bpdbi.core.ColumnDescriptor;
import io.github.bpdbi.core.impl.ByteBuffer;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSasl;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslContinue;
import io.github.bpdbi.pg.impl.codec.BackendMessage.AuthenticationSaslFinal;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Decodes Postgres backend (server → client) protocol messages from an InputStream. Blocking: each
 * readMessage() call blocks until a complete message is available.
 */
public final class PgDecoder {

  private final InputStream in;

  // Reusable buffer for reading int/short values — avoids allocating byte[4] per call
  private final byte[] intBuf = new byte[4];

  /**
   * Per-connection cache for column names. Column names repeat across queries on the same
   * connection, so interning them reduces String allocation and improves HashMap lookup performance
   * in Row's columnNameIndex (interned strings share identity, speeding up equals()).
   */
  private final Map<String, String> columnNameCache = new HashMap<>();

  public PgDecoder(@NonNull InputStream in) {
    this.in = in;
  }

  /** Read one complete backend message. Blocks until available. */
  public @NonNull BackendMessage readMessage() throws IOException {
    int type = readByte();
    if (type == -1) {
      throw new IOException("Connection closed by server");
    }
    int length = readInt();
    // Fast path: decode DataRow directly from stream to avoid intermediate byte[] payload copy
    if (type == PgProtocolConstants.DATA_ROW) {
      return new BackendMessage.DataRow(readDataRowValues());
    }
    byte[] payload = readExactly(length - 4);
    ByteBuffer buf = ByteBuffer.wrap(payload);
    return decode((byte) type, buf);
  }

  /**
   * Read the next backend message. If it is a DataRow, read column values directly into the given
   * ColumnBuffers and return {@link BackendMessage.DataRow} with null values (as a sentinel). For
   * all other message types, behaves like {@link #readMessage()}.
   *
   * <p>This eliminates the intermediate byte[][] allocation for DataRow in the buffered query path.
   */
  public @NonNull BackendMessage readMessageIntoBuffers(
      io.github.bpdbi.core.impl.ColumnBuffer @Nullable [] buffers) throws IOException {
    int type = readByte();
    if (type == -1) {
      throw new IOException("Connection closed by server");
    }
    int length = readInt();
    if (type == PgProtocolConstants.DATA_ROW && buffers != null) {
      readDataRowIntoBuffers(buffers);
      return BackendMessage.DATA_ROW_SENTINEL;
    }
    if (type == PgProtocolConstants.DATA_ROW) {
      return new BackendMessage.DataRow(readDataRowValues());
    }
    byte[] payload = readExactly(length - 4);
    ByteBuffer buf = ByteBuffer.wrap(payload);
    return decode((byte) type, buf);
  }

  /** Read a single byte response (used for SSL negotiation). */
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
      case PgProtocolConstants.PARSE_COMPLETE -> BackendMessage.PARSE_COMPLETE;
      case PgProtocolConstants.BIND_COMPLETE -> BackendMessage.BIND_COMPLETE;
      case PgProtocolConstants.CLOSE_COMPLETE -> new BackendMessage.CloseComplete();
      case PgProtocolConstants.NO_DATA -> BackendMessage.NO_DATA;
      case PgProtocolConstants.PORTAL_SUSPENDED -> new BackendMessage.PortalSuspended();
      case PgProtocolConstants.PARAMETER_DESCRIPTION -> decodeParameterDescription(buf);
      case PgProtocolConstants.NOTIFICATION_RESPONSE -> decodeNotificationResponse(buf);
      default ->
          throw new UnsupportedOperationException(
              "Unknown backend message type: "
                  + (char) type
                  + " (0x"
                  + Integer.toHexString(type)
                  + ")");
    };
  }

  private BackendMessage decodeAuthentication(ByteBuffer buf) {
    int authType = buf.readInt();
    return switch (authType) {
      case PgProtocolConstants.AUTH_OK -> new BackendMessage.AuthenticationOk();
      case PgProtocolConstants.AUTH_CLEARTEXT_PASSWORD ->
          new BackendMessage.AuthenticationCleartextPassword();
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
            if (!mechanisms.isEmpty()) {
              mechanisms.append(",");
            }
            mechanisms.append(mech);
          }
        }
        if (buf.readableBytes() > 0) {
          buf.readByte(); // trailing null
        }
        yield new AuthenticationSasl(mechanisms.toString());
      }
      case PgProtocolConstants.AUTH_SASL_CONTINUE -> {
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        yield new AuthenticationSaslContinue(data);
      }
      case PgProtocolConstants.AUTH_SASL_FINAL -> {
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        yield new AuthenticationSaslFinal(data);
      }
      default ->
          throw new UnsupportedOperationException(
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
      String name = internColumnName(buf.readCString());
      int tableOID = buf.readInt();
      short colAttr = buf.readShort();
      int typeOID = buf.readInt();
      short typeSize = buf.readShort();
      int typeMod = buf.readInt();
      buf.skipBytes(2); // format code (always binary — we request it in Bind)
      columns[i] = new ColumnDescriptor(name, tableOID, colAttr, typeOID, typeSize, typeMod);
    }
    return new BackendMessage.RowDescription(columns);
  }

  /**
   * Intern a column name so that repeated queries on the same connection share String instances.
   * Reduces allocation and speeds up HashMap lookups via identity-equal keys.
   */
  private String internColumnName(String name) {
    String cached = columnNameCache.get(name);
    if (cached != null) {
      return cached;
    }
    columnNameCache.put(name, name);
    return name;
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

  /**
   * Parse CommandComplete tag directly from raw bytes without intermediate String allocation. Tag
   * format: "INSERT 0 5", "UPDATE 3", "DELETE 1", "SELECT 10", etc. We want the last number.
   */
  private BackendMessage decodeCommandComplete(ByteBuffer buf) {
    int start = buf.readerIndex();
    int readable = buf.readableBytes();
    byte[] data = buf.array();
    int end = start + readable;
    // Skip trailing null terminator
    if (end > start && data[end - 1] == 0) {
      end--;
    }
    buf.skipBytes(readable);

    int rows = 0;
    boolean afterSpace = false;
    for (int i = start; i < end; i++) {
      byte b = data[i];
      if (b == ' ') {
        afterSpace = true;
        rows = 0;
      } else if (afterSpace && b >= '0' && b <= '9') {
        rows = rows * 10 + (b - '0');
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
    return new BackendMessage.ErrorResponse(
        severity,
        code,
        message,
        detail,
        hint,
        position,
        where,
        file,
        line,
        routine,
        schema,
        table,
        column,
        dataType,
        constraint);
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
    readFully(intBuf, 0, 4);
    return (intBuf[0] & 0xFF) << 24
        | (intBuf[1] & 0xFF) << 16
        | (intBuf[2] & 0xFF) << 8
        | (intBuf[3] & 0xFF);
  }

  private int readShort() throws IOException {
    readFully(intBuf, 0, 2);
    return (intBuf[0] & 0xFF) << 8 | (intBuf[1] & 0xFF);
  }

  /**
   * Decode DataRow column values directly from the InputStream. Avoids allocating an intermediate
   * byte[] for the entire message payload — reads each column value straight from the stream.
   */
  private byte[][] readDataRowValues() throws IOException {
    int columnCount = readShort();
    byte[][] values = new byte[columnCount][];
    for (int i = 0; i < columnCount; i++) {
      int len = readInt();
      if (len == -1) {
        values[i] = null; // SQL NULL
      } else {
        values[i] = new byte[len];
        readFully(values[i], 0, len);
      }
    }
    return values;
  }

  /**
   * Read a DataRow directly into ColumnBuffer arrays, skipping the intermediate byte[][]
   * allocation. Each column value is read from the stream straight into the ColumnBuffer's backing
   * array.
   */
  public void readDataRowIntoBuffers(io.github.bpdbi.core.impl.ColumnBuffer @NonNull [] buffers)
      throws IOException {
    int columnCount = readShort();
    for (int i = 0; i < columnCount; i++) {
      int len = readInt();
      if (len == -1) {
        buffers[i].appendNull();
      } else {
        buffers[i].appendFromStream(in, len);
      }
    }
  }

  private byte[] readExactly(int n) throws IOException {
    byte[] buf = new byte[n];
    readFully(buf, 0, n);
    return buf;
  }

  private void readFully(byte[] buf, int offset, int n) throws IOException {
    int end = offset + n;
    while (offset < end) {
      int read = in.read(buf, offset, end - offset);
      if (read == -1) {
        throw new IOException(
            "Unexpected end of stream (needed " + n + " bytes, got " + (offset - (end - n)) + ")");
      }
      offset += read;
    }
  }
}
