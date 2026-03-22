package io.github.bpdbi.pg.impl.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Function;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Encodes Postgres frontend (client → server) protocol messages into a ByteBuffer. Messages are
 * accumulated and flushed as a single 'write'.
 */
public final class PgEncoder {

  /** Pre-encoded empty C string (just a null terminator). Used for unnamed portals/statements. */
  public static final byte[] EMPTY_CSTRING = {0};

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

  /**
   * Write the 'Bind' message with text parameters and binary results (unnamed portal and
   * statement).
   */
  public void writeBind(@Nullable String @NonNull [] paramValues) {
    writeBindBytes(EMPTY_CSTRING, EMPTY_CSTRING, paramValues);
  }

  /** Write the 'Bind' with String names (convenience, allocates byte[] for names). */
  public void writeBind(
      @NonNull String portal,
      @NonNull String statementName,
      @Nullable String @NonNull [] paramValues) {
    writeBindBytes(toCString(portal), toCString(statementName), paramValues);
  }

  /**
   * Write the 'Bind' with pre-encoded C string names (zero-allocation for names on the hot path).
   *
   * <p>Null elements in {@code paramValues} encode SQL NULL (wire format: length = -1).
   */
  public void writeBindBytes(
      byte @NonNull [] portal,
      byte @NonNull [] statementName,
      @Nullable String @NonNull [] paramValues) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.BIND);
    buf.writeInt(0); // length placeholder
    buf.writeCStringBytes(portal);
    buf.writeCStringBytes(statementName);
    // Parameter format codes: all text (0) — text params for now
    buf.writeShort(0); // 0 = use default (text) for all
    // Parameter values
    buf.writeShort(paramValues.length);
    for (String paramValue : paramValues) {
      if (paramValue == null) {
        buf.writeInt(-1); // NULL
      } else {
        byte[] bytes = paramValue.getBytes(UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
      }
    }
    // Result format codes: request all binary (1)
    buf.writeShort(1); // 1 format code applies to all columns
    buf.writeShort(1); // binary format
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  /**
   * Write the 'Bind' message with binary parameter encoding. Parameters are pre-encoded as byte
   * arrays.
   *
   * <p>Null elements in {@code binaryParams} encode SQL NULL (wire format: length = -1).
   */
  public void writeBindBinary(
      @NonNull String portal,
      @NonNull String statementName,
      byte @Nullable [] @NonNull [] binaryParams) {
    writeBindBinaryBytes(toCString(portal), toCString(statementName), binaryParams);
  }

  /**
   * Write the 'Bind' (binary params) with pre-encoded C string names.
   *
   * <p>Null elements in {@code binaryParams} encode SQL NULL (wire format: length = -1).
   */
  // IntelliJ misreads JSpecify type-use annotations on nested arrays — the outer @NonNull
  // guarantees non-null
  @SuppressWarnings({"DataFlowIssue", "ConstantValue"})
  public void writeBindBinaryBytes(
      byte @NonNull [] portal,
      byte @NonNull [] statementName,
      byte @Nullable [] @NonNull [] binaryParams) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.BIND);
    buf.writeInt(0); // length placeholder
    buf.writeCStringBytes(portal);
    buf.writeCStringBytes(statementName);
    // Parameter format codes: all binary (1)
    buf.writeShort(1); // 1 format code for all params
    buf.writeShort(1); // binary format
    // Parameter values
    buf.writeShort(binaryParams.length);
    for (byte[] binaryParam : binaryParams) {
      if (binaryParam == null) {
        buf.writeInt(-1); // NULL
      } else {
        buf.writeInt(binaryParam.length);
        buf.writeBytes(binaryParam);
      }
    }
    // Result format codes: request all binary (1)
    buf.writeShort(1);
    buf.writeShort(1); // binary format
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  /**
   * Write the 'Bind' message with inline binary parameter encoding. Encodes supported types (int,
   * long, short, float, double, boolean, byte[], UUID) directly into the buffer without
   * intermediate allocations. Unsupported types fall back to text encoding via the provided
   * function.
   *
   * <p>Uses a single pass over the params array: format codes are reserved upfront and backfilled
   * as each parameter is encoded, avoiding redundant type checks.
   *
   * @param portal pre-encoded C string portal name
   * @param statementName pre-encoded C string statement name
   * @param params raw Java parameter values; null elements encode SQL NULL (wire format: length =
   *     -1)
   * @param textEncoder function to text-encode params that cannot be binary-encoded (may be 'null'
   *     if all params are guaranteed binary-encodable)
   */
  public void writeBindInline(
      byte @NonNull [] portal,
      byte @NonNull [] statementName,
      @Nullable Object @NonNull [] params,
      @Nullable Function<Object, String> textEncoder) {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.BIND);
    buf.writeInt(0); // length placeholder
    buf.writeCStringBytes(portal);
    buf.writeCStringBytes(statementName);
    int paramCount = params.length;
    // Reserve space for per-parameter format codes (backfilled below)
    buf.writeShort(paramCount);
    int fmtCodesPos = buf.writerIndex();
    buf.skip(paramCount * 2);
    // Parameter values — single pass: encode value and backfill format code
    buf.writeShort(paramCount);
    for (int i = 0; i < paramCount; i++) {
      Object param = params[i];
      if (param == null) {
        buf.setShort(fmtCodesPos + i * 2, 0);
        buf.writeInt(-1);
      } else if (BinaryParamEncoder.writeParam(param, buf)) {
        buf.setShort(fmtCodesPos + i * 2, 1); // binary
      } else {
        buf.setShort(fmtCodesPos + i * 2, 0); // text
        String text = textEncoder != null ? textEncoder.apply(param) : param.toString();
        // Encode UTF-8 directly into buffer — no intermediate byte[] allocation
        buf.writeInt(0); // length placeholder
        int lenPos = buf.writerIndex() - 4;
        int bytesWritten = buf.writeStringUtf8(text);
        buf.setInt(lenPos, bytesWritten);
      }
    }
    // Result format codes: request all binary (1)
    buf.writeShort(1);
    buf.writeShort(1); // binary format
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
  }

  public void writeDescribePortal() {
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.DESCRIBE);
    buf.writeInt(0);
    buf.writeByte('P');
    buf.writeCStringBytes(EMPTY_CSTRING);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
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
    int pos = buf.writerIndex();
    buf.writeByte(PgProtocolConstants.EXECUTE);
    buf.writeInt(0);
    buf.writeCStringBytes(EMPTY_CSTRING);
    buf.writeInt(0);
    buf.setInt(pos + 1, buf.writerIndex() - pos - 1);
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
   * Estimate the byte size of an extended query sequence (Parse + Bind + Describe + Execute) for a
   * single statement. Used to pre-size the buffer before encoding a pipelined batch.
   *
   * <p>Overhead per message: type byte (1) + length int (4) = 5 bytes. Parse has a statement name
   * (1 null byte) + SQL + null + param count (2) + per-param OID (4). Bind has portal (1) +
   * statement (1) + per-param format codes (2 each) + param count (2) + per-param (4 length plus
   * value bytes) + result format (4). DescribePortal is 7 bytes. Execute is 10 bytes.
   */
  public static int estimateExtendedQuerySize(String sql, Object[] params) {
    // Parse: type(1) + len(4) + stmtName(1) + sql_bytes + null(1) + paramCount(2) + OIDs(4 each)
    int estimate = 1 + 4 + 1 + sql.length() + 1 + 2 + params.length * 4;
    // Bind: type(1) + len(4) + portal(1) + stmtName(1) + paramFmtCount(2)
    //       + per-param format(2 each) + paramCount(2) + per-param values + resultFmt(4)
    estimate += 1 + 4 + 1 + 1 + 2 + params.length * 2 + 2 + 4;
    for (Object p : params) {
      // 4-byte length prefix plus estimated value size
      estimate += 4 + estimateParamSize(p);
    }
    // DescribePortal: type(1) + len(4) + 'P'(1) + name(1)
    estimate += 7;
    // Execute: type(1) + len(4) + portal(1) + maxRows(4)
    estimate += 10;
    return estimate;
  }

  private static int estimateParamSize(@Nullable Object p) {
    return switch (p) {
      case null -> 0;
      case Integer ignored -> 4;
      case Long ignored -> 8;
      case Short ignored -> 2;
      case Float ignored -> 4;
      case Double ignored -> 8;
      case Boolean ignored -> 1;
      case String s -> s.length(); // approximate UTF-8 size
      case byte[] b -> b.length;
      default -> 16;
    };
  }

  /** Pre-size the internal buffer to avoid resizing during batch encoding. */
  public void ensureCapacity(int bytes) {
    buf.ensureWritable(bytes);
  }

  /**
   * Flush all accumulated messages to the output stream as a single 'write', then clear the buffer.
   */
  public void flush(@NonNull OutputStream out) throws IOException {
    if (buf.writerIndex() > 0) {
      out.write(buf.array(), 0, buf.writerIndex());
      out.flush();
      buf.clear();
    }
  }

  /** Convert a String to a null-terminated C string byte array. */
  public static byte[] toCString(@NonNull String s) {
    if (s.isEmpty()) {
      return EMPTY_CSTRING;
    }
    byte[] bytes = s.getBytes(UTF_8);
    byte[] cstring = new byte[bytes.length + 1];
    System.arraycopy(bytes, 0, cstring, 0, bytes.length);
    // cstring[bytes.length] is already 0 (null terminator)
    return cstring;
  }
}
