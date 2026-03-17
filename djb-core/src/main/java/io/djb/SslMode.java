package io.djb;

/** SSL/TLS mode for database connections. */
public enum SslMode {

  /** No SSL. */
  DISABLE,

  /** Try SSL first, fall back to non-SSL if the server does not support it. */
  PREFER,

  /** Require SSL; fail if the server does not support it. */
  REQUIRE,

  /** Require SSL and verify the server certificate against the trust store. */
  VERIFY_CA,

  /**
   * Require SSL, verify the server certificate, and verify the hostname matches the certificate.
   */
  VERIFY_FULL
}
