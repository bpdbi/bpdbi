package io.djb;

/**
 * Base exception for database errors.
 * Database-specific drivers may extend this with additional fields.
 */
public class DbException extends RuntimeException {

    private final String severity;
    private final String sqlState;

    public DbException(String severity, String sqlState, String message) {
        super(message);
        this.severity = severity;
        this.sqlState = sqlState;
    }

    public String severity() { return severity; }
    public String sqlState() { return sqlState; }

    @Override
    public String toString() {
        var sb = new StringBuilder("DbException: ");
        if (severity != null) sb.append(severity).append(": ");
        sb.append(getMessage());
        if (sqlState != null) sb.append(" [").append(sqlState).append("]");
        return sb.toString();
    }
}
