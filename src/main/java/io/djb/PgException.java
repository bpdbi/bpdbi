package io.djb;

import io.djb.impl.codec.BackendMessage;

/**
 * Exception thrown when PostgreSQL returns an error.
 */
public class PgException extends RuntimeException {

    private final String severity;
    private final String sqlState;
    private final String detail;
    private final String hint;

    public PgException(String severity, String sqlState, String message, String detail, String hint) {
        super(message);
        this.severity = severity;
        this.sqlState = sqlState;
        this.detail = detail;
        this.hint = hint;
    }

    public static PgException fromErrorResponse(BackendMessage.ErrorResponse err) {
        return new PgException(err.severity(), err.code(), err.message(), err.detail(), err.hint());
    }

    public String severity() { return severity; }
    public String sqlState() { return sqlState; }
    public String detail() { return detail; }
    public String hint() { return hint; }

    @Override
    public String toString() {
        var sb = new StringBuilder("PgException: ");
        if (severity != null) sb.append(severity).append(": ");
        sb.append(getMessage());
        if (sqlState != null) sb.append(" [").append(sqlState).append("]");
        if (detail != null) sb.append("\n  Detail: ").append(detail);
        if (hint != null) sb.append("\n  Hint: ").append(hint);
        return sb.toString();
    }
}
