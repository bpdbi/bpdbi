package io.djb.impl.codec;

/**
 * PostgreSQL wire protocol constants.
 */
public final class PgProtocolConstants {

    private PgProtocolConstants() {}

    // Authentication types
    public static final int AUTH_OK = 0;
    public static final int AUTH_KERBEROS_V5 = 2;
    public static final int AUTH_CLEARTEXT_PASSWORD = 3;
    public static final int AUTH_MD5_PASSWORD = 5;
    public static final int AUTH_SCM_CREDENTIAL = 6;
    public static final int AUTH_GSS = 7;
    public static final int AUTH_GSS_CONTINUE = 8;
    public static final int AUTH_SSPI = 9;
    public static final int AUTH_SASL = 10;
    public static final int AUTH_SASL_CONTINUE = 11;
    public static final int AUTH_SASL_FINAL = 12;

    // Backend message types
    public static final byte BACKEND_KEY_DATA = 'K';
    public static final byte AUTHENTICATION = 'R';
    public static final byte ERROR_RESPONSE = 'E';
    public static final byte NOTICE_RESPONSE = 'N';
    public static final byte NOTIFICATION_RESPONSE = 'A';
    public static final byte COMMAND_COMPLETE = 'C';
    public static final byte PARAMETER_STATUS = 'S';
    public static final byte READY_FOR_QUERY = 'Z';
    public static final byte PARAMETER_DESCRIPTION = 't';
    public static final byte ROW_DESCRIPTION = 'T';
    public static final byte DATA_ROW = 'D';
    public static final byte PORTAL_SUSPENDED = 's';
    public static final byte NO_DATA = 'n';
    public static final byte EMPTY_QUERY_RESPONSE = 'I';
    public static final byte PARSE_COMPLETE = '1';
    public static final byte BIND_COMPLETE = '2';
    public static final byte CLOSE_COMPLETE = '3';
    public static final byte FUNCTION_RESULT = 'V';

    // Frontend message types
    public static final byte QUERY = 'Q';
    public static final byte PARSE = 'P';
    public static final byte BIND = 'B';
    public static final byte DESCRIBE = 'D';
    public static final byte EXECUTE = 'E';
    public static final byte CLOSE = 'C';
    public static final byte SYNC = 'S';
    public static final byte TERMINATE = 'X';
    public static final byte PASSWORD = 'p';

    // Error/Notice field types
    public static final byte FIELD_SEVERITY = 'S';
    public static final byte FIELD_CODE = 'C';
    public static final byte FIELD_MESSAGE = 'M';
    public static final byte FIELD_DETAIL = 'D';
    public static final byte FIELD_HINT = 'H';
    public static final byte FIELD_POSITION = 'P';
    public static final byte FIELD_INTERNAL_POSITION = 'p';
    public static final byte FIELD_INTERNAL_QUERY = 'q';
    public static final byte FIELD_WHERE = 'W';
    public static final byte FIELD_FILE = 'F';
    public static final byte FIELD_LINE = 'L';
    public static final byte FIELD_ROUTINE = 'R';
    public static final byte FIELD_SCHEMA = 's';
    public static final byte FIELD_TABLE = 't';
    public static final byte FIELD_COLUMN = 'c';
    public static final byte FIELD_DATA_TYPE = 'd';
    public static final byte FIELD_CONSTRAINT = 'n';
}
