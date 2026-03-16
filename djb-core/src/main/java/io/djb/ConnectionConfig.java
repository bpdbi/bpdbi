package io.djb;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Database connection configuration.
 */
public final class ConnectionConfig {

    private String host = "localhost";
    private int port;
    private @Nullable String database;
    private @Nullable String username;
    private @Nullable String password;
    private @Nullable Map<String, String> properties;
    private SslMode sslMode = SslMode.DISABLE;
    private boolean cachePreparedStatements = false;
    private int preparedStatementCacheMaxSize = 256;
    private int preparedStatementCacheSqlLimit = 2048;

    public ConnectionConfig() {}

    public ConnectionConfig(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    /**
     * Parse a connection URI.
     * Supports formats:
     *   postgresql://user:password@host:port/database?param=value
     *   mysql://user:password@host:port/database
     */
    public static ConnectionConfig fromUri(String uri) {
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

        String query = parsed.getQuery();
        if (query != null && !query.isEmpty()) {
            config.properties = new LinkedHashMap<>();
            for (String param : query.split("&")) {
                int eq = param.indexOf('=');
                if (eq >= 0) {
                    config.properties.put(param.substring(0, eq), param.substring(eq + 1));
                }
            }
        }

        // Extract sslmode from properties if present
        if (config.properties != null && config.properties.containsKey("sslmode")) {
            String mode = config.properties.remove("sslmode");
            config.sslMode = switch (mode) {
                case "disable" -> SslMode.DISABLE;
                case "prefer" -> SslMode.PREFER;
                case "require" -> SslMode.REQUIRE;
                default -> SslMode.DISABLE;
            };
            if (config.properties.isEmpty()) config.properties = null;
        }

        // Set default port based on scheme if not specified
        if (config.port == -1) {
            String scheme = parsed.getScheme();
            if (scheme != null) {
                config.port = switch (scheme) {
                    case "postgresql", "postgres" -> 5432;
                    case "mysql" -> 3306;
                    default -> -1;
                };
            }
        }

        return config;
    }

    public String host() { return host; }
    public ConnectionConfig host(String host) { this.host = host; return this; }

    public int port() { return port; }
    public ConnectionConfig port(int port) { this.port = port; return this; }

    public @Nullable String database() { return database; }
    public ConnectionConfig database(String database) { this.database = database; return this; }

    public @Nullable String username() { return username; }
    public ConnectionConfig username(String username) { this.username = username; return this; }

    public @Nullable String password() { return password; }
    public ConnectionConfig password(String password) { this.password = password; return this; }

    public SslMode sslMode() { return sslMode; }
    public ConnectionConfig sslMode(SslMode sslMode) { this.sslMode = sslMode; return this; }

    public @Nullable Map<String, String> properties() { return properties; }
    public ConnectionConfig properties(@Nullable Map<String, String> properties) { this.properties = properties; return this; }

    public boolean cachePreparedStatements() { return cachePreparedStatements; }
    public ConnectionConfig cachePreparedStatements(boolean cachePreparedStatements) { this.cachePreparedStatements = cachePreparedStatements; return this; }

    public int preparedStatementCacheMaxSize() { return preparedStatementCacheMaxSize; }
    public ConnectionConfig preparedStatementCacheMaxSize(int preparedStatementCacheMaxSize) { this.preparedStatementCacheMaxSize = preparedStatementCacheMaxSize; return this; }

    public int preparedStatementCacheSqlLimit() { return preparedStatementCacheSqlLimit; }
    public ConnectionConfig preparedStatementCacheSqlLimit(int preparedStatementCacheSqlLimit) { this.preparedStatementCacheSqlLimit = preparedStatementCacheSqlLimit; return this; }
}
