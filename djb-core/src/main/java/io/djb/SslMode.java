package io.djb;

/**
 * SSL/TLS mode for database connections.
 */
public enum SslMode {

    /** No SSL. */
    DISABLE,

    /** Try SSL first, fall back to non-SSL if the server does not support it. */
    PREFER,

    /** Require SSL; fail if the server does not support it. */
    REQUIRE
}
