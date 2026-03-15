package io.djb.impl.codec;

import io.djb.ColumnDescriptor;

/**
 * Sealed interface representing all backend (server → client) PostgreSQL protocol messages.
 */
public sealed interface BackendMessage {

    record AuthenticationOk() implements BackendMessage {}
    record AuthenticationCleartextPassword() implements BackendMessage {}
    record AuthenticationMD5Password(byte[] salt) implements BackendMessage {}
    record AuthenticationSASL(String mechanisms) implements BackendMessage {}
    record AuthenticationSASLContinue(byte[] data) implements BackendMessage {}
    record AuthenticationSASLFinal(byte[] data) implements BackendMessage {}

    record ParameterStatus(String name, String value) implements BackendMessage {}
    record BackendKeyData(int processId, int secretKey) implements BackendMessage {}
    record ReadyForQuery(char txStatus) implements BackendMessage {}

    record RowDescription(ColumnDescriptor[] columns) implements BackendMessage {}
    record DataRow(byte[][] values) implements BackendMessage {}
    record CommandComplete(int rowsAffected) implements BackendMessage {}
    record EmptyQueryResponse() implements BackendMessage {}

    record ParseComplete() implements BackendMessage {}
    record BindComplete() implements BackendMessage {}
    record CloseComplete() implements BackendMessage {}
    record NoData() implements BackendMessage {}
    record PortalSuspended() implements BackendMessage {}
    record ParameterDescription(int[] typeOIDs) implements BackendMessage {}

    record ErrorResponse(String severity, String code, String message,
                         String detail, String hint, String position,
                         String where, String file, String line,
                         String routine, String schema, String table,
                         String column, String dataType, String constraint) implements BackendMessage {}

    record NoticeResponse(String severity, String code, String message,
                          String detail, String hint) implements BackendMessage {}

    record NotificationResponse(int processId, String channel, String payload) implements BackendMessage {}
}
