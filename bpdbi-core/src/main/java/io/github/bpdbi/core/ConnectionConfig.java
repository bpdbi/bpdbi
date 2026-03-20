package io.github.bpdbi.core;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Database connection configuration with a fluent builder API.
 *
 * <p>Supports both programmatic construction and URI parsing:
 *
 * <pre>{@code
 * // Programmatic
 * var config = new ConnectionConfig("localhost", 5432, "mydb", "user", "pass")
 *     .sslMode(SslMode.REQUIRE)
 *     .socketTimeoutMillis(5000)
 *     .cachePreparedStatements(true);
 *
 * // From URI
 * var config = ConnectionConfig.fromUri("postgresql://user:pass@host:5432/db?sslmode=require");
 * }</pre>
 *
 * <p>URI parsing supports {@code postgresql://}, {@code postgres://}, and {@code mysql://} schemes,
 * with optional query parameters for SSL mode, certificates, and application-specific properties.
 *
 * @see SslMode
 */
public final class ConnectionConfig {

  private String host = "localhost";
  private int port;
  private String database;
  private String username;
  private String password;
  private Map<String, String> properties;
  private SslMode sslMode = SslMode.DISABLE;
  private SSLContext sslContext;
  private String pemCertPath;
  private String trustStorePath;
  private String trustStorePassword;
  private boolean hostnameVerification = false;
  private int socketTimeoutMillis = 0; // 0 = no timeout
  private boolean cachePreparedStatements = false;

  /** Max cached prepared statements. 256 balances memory vs hit-rate for typical web apps. */
  private int preparedStatementCacheMaxSize = 256;

  /** SQL strings longer than this are not cached (likely one-off dynamic queries). */
  private int preparedStatementCacheSqlLimit = 2048;

  /**
   * Maximum bytes to buffer per cursor fetch. When set, cursor.read(count) adjusts the actual fetch
   * count based on observed average row size so that one batch does not exceed this limit. 0 =
   * disabled (use explicit fetch count). Default: 0.
   */
  private int maxResultBufferBytes = 0;

  public ConnectionConfig() {}

  public ConnectionConfig(
      @NonNull String host,
      int port,
      @Nullable String database,
      @Nullable String username,
      @Nullable String password) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
  }

  /**
   * Parse a connection URI. Supports formats:
   * postgresql://user:password@host:port/database?param=value
   * mysql://user:password@host:port/database
   */
  public static @NonNull ConnectionConfig fromUri(@NonNull String uri) {
    var config = new ConnectionConfig();
    var parsed = URI.create(uri);

    config.host = parsed.getHost() != null ? parsed.getHost() : "localhost";
    config.port = parsed.getPort();

    String path = parsed.getPath();
    if (path != null && path.startsWith("/")) {
      config.database = path.substring(1);
    }

    String userInfo = parsed.getUserInfo();
    if (userInfo != null) {
      int colon = userInfo.indexOf(':');
      if (colon >= 0) {
        config.username = userInfo.substring(0, colon);
        config.password = userInfo.substring(colon + 1);
      } else {
        config.username = userInfo;
      }
    }

    String query = parsed.getRawQuery();
    if (query != null && !query.isEmpty()) {
      config.properties = new LinkedHashMap<>();
      for (String param : query.split("&", -1)) {
        int eq = param.indexOf('=');
        if (eq >= 0) {
          String key = URLDecoder.decode(param.substring(0, eq), StandardCharsets.UTF_8);
          String value = URLDecoder.decode(param.substring(eq + 1), StandardCharsets.UTF_8);
          config.properties.put(key, value);
        }
      }
    }

    // Extract `sslMode` from properties if present
    if (config.properties != null && config.properties.containsKey("sslmode")) {
      String sslMode = config.properties.remove("sslmode");
      config.sslMode =
          switch (sslMode) {
            case "prefer" -> SslMode.PREFER;
            case "require" -> SslMode.REQUIRE;
            case "verify-ca" -> SslMode.VERIFY_CA;
            case "verify-full" -> SslMode.VERIFY_FULL;
            default -> SslMode.DISABLE;
          };
      if (config.properties.isEmpty()) {
        config.properties = null;
      }
    }

    // Extract SSL trust options from properties.
    // `sslRootCert` is the CA certificate (standard Postgres URI param).
    // `sslCert` is the client certificate — used as fallback if `sslRootCert` is absent.
    // If both are present, `sslRootCert` wins (it's the more commonly intended option).
    if (config.properties != null) {
      String sslCert = config.properties.remove("sslcert");
      String sslRootCert = config.properties.remove("sslrootcert");
      if (sslRootCert != null) {
        config.pemCertPath = sslRootCert;
      } else if (sslCert != null) {
        config.pemCertPath = sslCert;
      }
      if (config.properties.isEmpty()) {
        config.properties = null;
      }
    }

    // Set default port based on scheme if not specified
    if (config.port == -1) {
      String scheme = parsed.getScheme();
      if (scheme != null) {
        config.port =
            switch (scheme) {
              case "postgresql", "postgres" -> 5432;
              case "mysql" -> 3306;
              default -> -1;
            };
      }
    }

    return config;
  }

  public @NonNull String host() {
    return host;
  }

  public @NonNull ConnectionConfig host(@NonNull String host) {
    this.host = host;
    return this;
  }

  public int port() {
    return port;
  }

  public @NonNull ConnectionConfig port(int port) {
    this.port = port;
    return this;
  }

  public @Nullable String database() {
    return database;
  }

  public @NonNull ConnectionConfig database(@Nullable String database) {
    this.database = database;
    return this;
  }

  public @Nullable String username() {
    return username;
  }

  public @NonNull ConnectionConfig username(@Nullable String username) {
    this.username = username;
    return this;
  }

  public @Nullable String password() {
    return password;
  }

  public @NonNull ConnectionConfig password(@Nullable String password) {
    this.password = password;
    return this;
  }

  public @NonNull SslMode sslMode() {
    return sslMode;
  }

  public @NonNull ConnectionConfig sslMode(@NonNull SslMode sslMode) {
    this.sslMode = sslMode;
    return this;
  }

  public @Nullable SSLContext sslContext() {
    return sslContext;
  }

  public @NonNull ConnectionConfig sslContext(@Nullable SSLContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  public @Nullable String pemCertPath() {
    return pemCertPath;
  }

  public @NonNull ConnectionConfig pemCertPath(@Nullable String pemCertPath) {
    this.pemCertPath = pemCertPath;
    return this;
  }

  public @Nullable String trustStorePath() {
    return trustStorePath;
  }

  public @NonNull ConnectionConfig trustStorePath(@Nullable String trustStorePath) {
    this.trustStorePath = trustStorePath;
    return this;
  }

  public @Nullable String trustStorePassword() {
    return trustStorePassword;
  }

  public @NonNull ConnectionConfig trustStorePassword(@Nullable String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  public boolean hostnameVerification() {
    return hostnameVerification;
  }

  public @NonNull ConnectionConfig hostnameVerification(boolean hostnameVerification) {
    this.hostnameVerification = hostnameVerification;
    return this;
  }

  /**
   * Socket read timeout in milliseconds. When a read on the socket exceeds this duration, a {@link
   * java.net.SocketTimeoutException} is thrown (wrapped as {@link DbConnectionException}). {@code
   * 0} means no timeout (infinite wait). Default: 0.
   */
  public int socketTimeoutMillis() {
    return socketTimeoutMillis;
  }

  public @NonNull ConnectionConfig socketTimeoutMillis(int socketTimeoutMillis) {
    if (socketTimeoutMillis < 0) {
      throw new IllegalArgumentException("socketTimeoutMillis must be >= 0");
    }
    this.socketTimeoutMillis = socketTimeoutMillis;
    return this;
  }

  public @Nullable Map<String, String> properties() {
    return properties;
  }

  public @NonNull ConnectionConfig properties(@Nullable Map<String, String> properties) {
    this.properties = properties;
    return this;
  }

  public boolean cachePreparedStatements() {
    return cachePreparedStatements;
  }

  public @NonNull ConnectionConfig cachePreparedStatements(boolean cachePreparedStatements) {
    this.cachePreparedStatements = cachePreparedStatements;
    return this;
  }

  public int preparedStatementCacheMaxSize() {
    return preparedStatementCacheMaxSize;
  }

  public @NonNull ConnectionConfig preparedStatementCacheMaxSize(
      int preparedStatementCacheMaxSize) {
    this.preparedStatementCacheMaxSize = preparedStatementCacheMaxSize;
    return this;
  }

  public int preparedStatementCacheSqlLimit() {
    return preparedStatementCacheSqlLimit;
  }

  public @NonNull ConnectionConfig preparedStatementCacheSqlLimit(
      int preparedStatementCacheSqlLimit) {
    this.preparedStatementCacheSqlLimit = preparedStatementCacheSqlLimit;
    return this;
  }

  /**
   * Maximum bytes to buffer per cursor fetch. When set, cursor.read(count) adjusts the actual fetch
   * count based on observed average row size so that one batch does not exceed this limit. 0 =
   * disabled (use explicit fetch count). Default: 0.
   */
  public int maxResultBufferBytes() {
    return maxResultBufferBytes;
  }

  public @NonNull ConnectionConfig maxResultBufferBytes(int maxResultBufferBytes) {
    if (maxResultBufferBytes < 0) {
      throw new IllegalArgumentException("maxResultBufferBytes must be >= 0");
    }
    this.maxResultBufferBytes = maxResultBufferBytes;
    return this;
  }
}
