package io.github.bpdbi.pg.impl.codec;

import io.github.bpdbi.core.ColumnDescriptor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Sealed interface representing all backend (server → client) Postgres protocol messages. */
public sealed interface BackendMessage {

  record AuthenticationOk() implements BackendMessage {}

  record AuthenticationCleartextPassword() implements BackendMessage {}

  record AuthenticationMD5Password(byte @NonNull [] salt) implements BackendMessage {}

  record AuthenticationSasl(@NonNull String mechanisms) implements BackendMessage {}

  record AuthenticationSaslContinue(byte @NonNull [] data) implements BackendMessage {}

  record AuthenticationSaslFinal(byte @NonNull [] data) implements BackendMessage {}

  record ParameterStatus(@NonNull String name, @NonNull String value) implements BackendMessage {}

  record BackendKeyData(int processId, int secretKey) implements BackendMessage {}

  record ReadyForQuery(char txStatus) implements BackendMessage {}

  record RowDescription(ColumnDescriptor @NonNull [] columns) implements BackendMessage {}

  record DataRow(byte @NonNull [][] values) implements BackendMessage {}

  /** Sentinel returned by readMessageIntoBuffers when a DataRow was written directly to buffers. */
  DataRow DATA_ROW_SENTINEL = new DataRow(new byte[0][]);

  record CommandComplete(int rowsAffected) implements BackendMessage {}

  record EmptyQueryResponse() implements BackendMessage {}

  /** Singleton — avoids allocation on every empty query response. */
  EmptyQueryResponse EMPTY_QUERY_RESPONSE = new EmptyQueryResponse();

  record ParseComplete() implements BackendMessage {}

  /** Singleton — avoids allocation on every pipelined query response. */
  ParseComplete PARSE_COMPLETE = new ParseComplete();

  record BindComplete() implements BackendMessage {}

  /** Singleton — avoids allocation on every pipelined query response. */
  BindComplete BIND_COMPLETE = new BindComplete();

  record CloseComplete() implements BackendMessage {}

  /** Singleton — avoids allocation on every pipelined query response. */
  CloseComplete CLOSE_COMPLETE = new CloseComplete();

  record NoData() implements BackendMessage {}

  /** Singleton — avoids allocation on every pipelined query response. */
  NoData NO_DATA = new NoData();

  record PortalSuspended() implements BackendMessage {}

  /** Singleton — avoids allocation on every portal suspension. */
  PortalSuspended PORTAL_SUSPENDED = new PortalSuspended();

  record ParameterDescription(int @NonNull [] typeOIDs) implements BackendMessage {}

  record ErrorResponse(
      @Nullable String severity,
      @Nullable String code,
      @Nullable String message,
      @Nullable String detail,
      @Nullable String hint,
      @Nullable String position,
      @Nullable String where,
      @Nullable String file,
      @Nullable String line,
      @Nullable String routine,
      @Nullable String schema,
      @Nullable String table,
      @Nullable String column,
      @Nullable String dataType,
      @Nullable String constraint)
      implements BackendMessage {}

  record NoticeResponse(
      @Nullable String severity,
      @Nullable String code,
      @Nullable String message,
      @Nullable String detail,
      @Nullable String hint)
      implements BackendMessage {}

  record NotificationResponse(int processId, @NonNull String channel, @NonNull String payload)
      implements BackendMessage {}
}
