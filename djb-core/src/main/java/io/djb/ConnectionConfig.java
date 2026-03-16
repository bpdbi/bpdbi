package io.djb;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Database connection configuration.
 */
public final class ConnectionConfig {

    private String host = "localhost";
    private int port;
    private String database;
    private String username;
    private String password;
    private Map<String, String> properties;

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

    public String database() { return database; }
    public ConnectionConfig database(String database) { this.database = database; return this; }

    public String username() { return username; }
    public ConnectionConfig username(String username) { this.username = username; return this; }

    public String password() { return password; }
    public ConnectionConfig password(String password) { this.password = password; return this; }

    public Map<String, String> properties() { return properties; }
    public ConnectionConfig properties(Map<String, String> properties) { this.properties = properties; return this; }
}
