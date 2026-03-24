package io.github.bpdbi.pg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.SslMode;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * SSL/TLS tests for PgSsl. Uses two Postgres containers: one with SSL enabled (for positive tests)
 * and the standard non-SSL container from PgTestBase (for fallback/rejection tests).
 */
class PgSslTest {

  // Non-SSL Postgres (standard)
  @SuppressWarnings("resource")
  static final PostgreSQLContainer<?> pg =
      new PostgreSQLContainer<>("postgres:16-alpine").withReuse(true);

  // SSL-enabled Postgres — certs are copied and permissions fixed via an init script
  @SuppressWarnings("resource")
  static final PostgreSQLContainer<?> sslPg = createSslContainer();

  @SuppressWarnings("resource")
  private static PostgreSQLContainer<?> createSslContainer() {
    var container =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("ssl/server.crt"),
                "/var/lib/postgresql/server.crt")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("ssl/server.key"),
                "/var/lib/postgresql/server.key");
    // Fix key file permissions + ownership, then start postgres with SSL
    container.setCommand(
        "bash",
        "-c",
        "chown postgres:postgres /var/lib/postgresql/server.key"
            + " && chmod 600 /var/lib/postgresql/server.key"
            + " && exec docker-entrypoint.sh postgres"
            + " -c ssl=on"
            + " -c ssl_cert_file=/var/lib/postgresql/server.crt"
            + " -c ssl_key_file=/var/lib/postgresql/server.key");
    return container;
  }

  @BeforeAll
  static void startContainers() {
    pg.start();
    sslPg.start();
  }

  // --- Helpers ---

  private ConnectionConfig sslConfig(SslMode mode) {
    return new ConnectionConfig()
        .host(sslPg.getHost())
        .port(sslPg.getMappedPort(5432))
        .database(sslPg.getDatabaseName())
        .username(sslPg.getUsername())
        .password(sslPg.getPassword())
        .sslMode(mode);
  }

  private ConnectionConfig nonSslConfig(SslMode mode) {
    return new ConnectionConfig()
        .host(pg.getHost())
        .port(pg.getMappedPort(5432))
        .database(pg.getDatabaseName())
        .username(pg.getUsername())
        .password(pg.getPassword())
        .sslMode(mode);
  }

  private static String resourcePath(String name) {
    var url = PgSslTest.class.getClassLoader().getResource(name);
    if (url == null) {
      throw new IllegalStateException("Resource not found: " + name);
    }
    return Path.of(url.getPath()).toString();
  }

  // --- Test 1: REQUIRE with trust-all (no cert config) ---

  @Test
  void sslRequireConnects() {
    try (var conn = PgConnection.connect(sslConfig(SslMode.REQUIRE))) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 2: PREFER connects with SSL when server supports it ---

  @Test
  void sslPreferConnects() {
    try (var conn = PgConnection.connect(sslConfig(SslMode.PREFER))) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 3: PREFER falls back to plain on non-SSL server ---

  @Test
  void sslPreferFallsBackToPlain() {
    try (var conn = PgConnection.connect(nonSslConfig(SslMode.PREFER))) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 4: REQUIRE fails on non-SSL server ---

  @Test
  void sslRequireFailsOnNonSslServer() {
    assertThrows(
        DbConnectionException.class, () -> PgConnection.connect(nonSslConfig(SslMode.REQUIRE)));
  }

  // --- Test 5: PEM certificate path ---

  @Test
  void sslWithPemCertPath() {
    var config = sslConfig(SslMode.VERIFY_CA).pemCertPath(resourcePath("ssl/ca.crt"));
    try (var conn = PgConnection.connect(config)) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 6: JKS trust store ---

  @Test
  void sslWithJksTrustStore() {
    var config =
        sslConfig(SslMode.VERIFY_CA)
            .trustStorePath(resourcePath("ssl/truststore.jks"))
            .trustStorePassword("changeit");
    try (var conn = PgConnection.connect(config)) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 7: User-provided SSLContext ---

  @Test
  void sslWithUserProvidedContext() throws Exception {
    // Build a trust-all SSLContext manually
    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(
        null,
        new TrustManager[] {
          new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] c, String t) {}

            public void checkServerTrusted(X509Certificate[] c, String t) {}

            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          }
        },
        null);
    var config = sslConfig(SslMode.REQUIRE).sslContext(ctx);
    try (var conn = PgConnection.connect(config)) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 8: VERIFY_FULL with hostname match (CN=localhost) ---

  @Test
  void sslVerifyFullWithHostnameMatch() {
    var config = sslConfig(SslMode.VERIFY_FULL).pemCertPath(resourcePath("ssl/ca.crt"));
    try (var conn = PgConnection.connect(config)) {
      assertEquals(1, conn.query("SELECT 1").first().getInteger(0));
    }
  }

  // --- Test 9: VERIFY_CA with system trust store (fails — self-signed cert not trusted) ---

  @Test
  void sslVerifyCaWithSystemTrustStoreFails() {
    // No pemCertPath or trustStorePath — falls through to system default trust store,
    // which won't trust our self-signed CA
    assertThrows(
        DbConnectionException.class, () -> PgConnection.connect(sslConfig(SslMode.VERIFY_CA)));
  }
}
