package io.github.bpdbi.mysql.impl.codec;

/** MySQL wire protocol constants. */
public final class MysqlProtocolConstants {

  private MysqlProtocolConstants() {}

  // Packet headers
  public static final int OK_PACKET = 0x00;
  public static final int EOF_PACKET = 0xFE;
  public static final int ERR_PACKET = 0xFF;
  public static final int LOCAL_INFILE = 0xFB;
  public static final int PACKET_PAYLOAD_LIMIT = 0xFFFFFF; // 16MB - 1

  // Command types
  public static final byte COM_QUIT = 0x01;
  public static final byte COM_INIT_DB = 0x02;
  public static final byte COM_QUERY = 0x03;
  public static final byte COM_PING = 0x0E;
  public static final byte COM_STMT_PREPARE = 0x16;
  public static final byte COM_STMT_EXECUTE = 0x17;
  public static final byte COM_STMT_CLOSE = 0x19;
  public static final byte COM_STMT_RESET = 0x1A;
  public static final byte COM_STMT_SEND_LONG_DATA = 0x18;
  public static final byte COM_STMT_FETCH = 0x1C;
  public static final byte COM_RESET_CONNECTION = 0x1F;

  // Capability flags
  public static final int CLIENT_LONG_PASSWORD = 0x00000001;
  public static final int CLIENT_LONG_FLAG = 0x00000004;
  public static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
  public static final int CLIENT_LOCAL_FILES = 0x00000080;
  public static final int CLIENT_PROTOCOL_41 = 0x00000200;
  public static final int CLIENT_SSL = 0x00000800;
  public static final int CLIENT_TRANSACTIONS = 0x00002000;
  public static final int CLIENT_SECURE_CONNECTION = 0x00008000;
  public static final int CLIENT_MULTI_STATEMENTS = 0x00010000;
  public static final int CLIENT_MULTI_RESULTS = 0x00020000;
  public static final int CLIENT_PS_MULTI_RESULTS = 0x00040000;
  public static final int CLIENT_PLUGIN_AUTH = 0x00080000;
  public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
  public static final int CLIENT_DEPRECATE_EOF = 0x01000000;

  public static final int CLIENT_SUPPORTED_FLAGS =
      CLIENT_PLUGIN_AUTH
          | CLIENT_LONG_PASSWORD
          | CLIENT_LONG_FLAG
          | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
          | CLIENT_SECURE_CONNECTION
          | CLIENT_PROTOCOL_41
          | CLIENT_TRANSACTIONS
          | CLIENT_MULTI_STATEMENTS
          | CLIENT_MULTI_RESULTS
          | CLIENT_PS_MULTI_RESULTS
          | CLIENT_LOCAL_FILES;

  // Server status flags
  public static final int SERVER_MORE_RESULTS_EXISTS = 0x0008;
  public static final int SERVER_STATUS_LAST_ROW_SENT = 0x0080;

  // Auth switch
  public static final int AUTH_SWITCH_REQUEST = 0xFE;
  public static final int AUTH_MORE_DATA = 0x01;

  // Nonce length
  public static final int NONCE_LENGTH = 20;
}
