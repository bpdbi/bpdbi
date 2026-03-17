package io.github.bpdbi.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConnectionConfigTest {

  @Test
  void parsePostgresUri() {
    var c = ConnectionConfig.fromUri("postgresql://myuser:mypass@dbhost:5433/mydb");
    assertEquals("dbhost", c.host());
    assertEquals(5433, c.port());
    assertEquals("mydb", c.database());
    assertEquals("myuser", c.username());
    assertEquals("mypass", c.password());
  }

  @Test
  void parsePostgresUriDefaultPort() {
    var c = ConnectionConfig.fromUri("postgresql://user:pass@localhost/test");
    assertEquals("localhost", c.host());
    assertEquals(5432, c.port());
  }

  @Test
  void parseMysqlUri() {
    var c = ConnectionConfig.fromUri("mysql://root:secret@db.example.com:3307/appdb");
    assertEquals("db.example.com", c.host());
    assertEquals(3307, c.port());
    assertEquals("appdb", c.database());
    assertEquals("root", c.username());
    assertEquals("secret", c.password());
  }

  @Test
  void parseMysqlUriDefaultPort() {
    var c = ConnectionConfig.fromUri("mysql://user:pass@localhost/test");
    assertEquals(3306, c.port());
  }

  @Test
  void parseUriWithQueryParams() {
    var c =
        ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=require&application_name=myapp");
    assertEquals("db", c.database());
    assertEquals(SslMode.REQUIRE, c.sslMode());
    assertNotNull(c.properties());
    assertEquals("myapp", c.properties().get("application_name"));
    // sslmode is consumed and removed from properties
    assertNull(c.properties().get("sslmode"));
  }

  @Test
  void parseUriNoPassword() {
    var c = ConnectionConfig.fromUri("postgresql://admin@localhost/mydb");
    assertEquals("admin", c.username());
    assertNull(c.password());
  }

  @Test
  void parsePostgresScheme() {
    var c = ConnectionConfig.fromUri("postgres://u:p@host/db");
    assertEquals(5432, c.port());
    assertEquals("host", c.host());
  }

  @Test
  void builderStyle() {
    var c =
        new ConnectionConfig()
            .host("myhost")
            .port(5432)
            .database("mydb")
            .username("user")
            .password("pass");
    assertEquals("myhost", c.host());
    assertEquals(5432, c.port());
    assertEquals("mydb", c.database());
  }

  // ===== URI edge cases =====

  @Test
  void parseUriSpecialCharsInPassword() {
    var c = ConnectionConfig.fromUri("postgresql://user:p%40ss%3Aw0rd@host/db");
    assertEquals("user", c.username());
    assertEquals("p@ss:w0rd", c.password());
  }

  @Test
  void parseUriEmptyDatabase() {
    var c = ConnectionConfig.fromUri("postgresql://user:pass@host:5432/");
    assertEquals("", c.database());
  }

  @Test
  void parseUriNoDatabasePath() {
    var c = ConnectionConfig.fromUri("postgresql://user:pass@host:5432");
    assertNull(c.database());
  }

  @Test
  void parseUriSslModeVerifyCa() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=verify-ca");
    assertEquals(SslMode.VERIFY_CA, c.sslMode());
  }

  @Test
  void parseUriSslModeVerifyFull() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=verify-full");
    assertEquals(SslMode.VERIFY_FULL, c.sslMode());
  }

  @Test
  void parseUriSslModePrefer() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=prefer");
    assertEquals(SslMode.PREFER, c.sslMode());
  }

  @Test
  void parseUriSslModeDisable() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=disable");
    assertEquals(SslMode.DISABLE, c.sslMode());
  }

  @Test
  void parseUriUnknownSslModeDefaultsToDisable() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=bogus");
    assertEquals(SslMode.DISABLE, c.sslMode());
  }

  @Test
  void parseUriSslCertParam() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslcert=/path/to/cert.pem");
    assertEquals("/path/to/cert.pem", c.pemCertPath());
    // sslcert consumed from properties
    assertNull(c.properties());
  }

  @Test
  void parseUriSslRootCertParam() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslrootcert=/path/to/ca.pem");
    assertEquals("/path/to/ca.pem", c.pemCertPath());
  }

  @Test
  void parseUriMultipleQueryParams() {
    var c =
        ConnectionConfig.fromUri(
            "postgresql://u:p@host/db?sslmode=require&application_name=myapp&connect_timeout=10");
    assertEquals(SslMode.REQUIRE, c.sslMode());
    assertNotNull(c.properties());
    assertEquals("myapp", c.properties().get("application_name"));
    assertEquals("10", c.properties().get("connect_timeout"));
  }

  @Test
  void parseUriNoQueryParams() {
    var c = ConnectionConfig.fromUri("postgresql://u:p@host/db");
    assertNull(c.properties());
    assertEquals(SslMode.DISABLE, c.sslMode());
  }

  @Test
  void parseUriHostOnly() {
    var c = ConnectionConfig.fromUri("postgresql://localhost");
    assertEquals("localhost", c.host());
    assertEquals(5432, c.port());
    assertNull(c.username());
    assertNull(c.password());
  }

  @Test
  void parseUriIpv4Host() {
    var c = ConnectionConfig.fromUri("postgresql://user:pass@192.168.1.1:5432/db");
    assertEquals("192.168.1.1", c.host());
  }

  // ===== Builder validation =====

  @Test
  void socketTimeoutNegativeThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> new ConnectionConfig().socketTimeoutMillis(-1));
  }

  @Test
  void socketTimeoutZeroIsAllowed() {
    var c = new ConnectionConfig().socketTimeoutMillis(0);
    assertEquals(0, c.socketTimeoutMillis());
  }

  // ===== Fluent setters =====

  @Test
  void sslConfigFluent() {
    var c =
        new ConnectionConfig()
            .sslMode(SslMode.VERIFY_FULL)
            .pemCertPath("/cert.pem")
            .trustStorePath("/trust.jks")
            .trustStorePassword("changeit")
            .hostnameVerification(true);
    assertEquals(SslMode.VERIFY_FULL, c.sslMode());
    assertEquals("/cert.pem", c.pemCertPath());
    assertEquals("/trust.jks", c.trustStorePath());
    assertEquals("changeit", c.trustStorePassword());
    assertTrue(c.hostnameVerification());
  }

  @Test
  void cacheConfigFluent() {
    var c =
        new ConnectionConfig()
            .cachePreparedStatements(true)
            .preparedStatementCacheMaxSize(128)
            .preparedStatementCacheSqlLimit(1024);
    assertTrue(c.cachePreparedStatements());
    assertEquals(128, c.preparedStatementCacheMaxSize());
    assertEquals(1024, c.preparedStatementCacheSqlLimit());
  }

  @Test
  void cacheConfigDefaults() {
    var c = new ConnectionConfig();
    assertFalse(c.cachePreparedStatements());
    assertEquals(256, c.preparedStatementCacheMaxSize());
    assertEquals(2048, c.preparedStatementCacheSqlLimit());
  }

  @Test
  void defaultSslMode() {
    var c = new ConnectionConfig();
    assertEquals(SslMode.DISABLE, c.sslMode());
  }

  @Test
  void defaultSocketTimeout() {
    var c = new ConnectionConfig();
    assertEquals(0, c.socketTimeoutMillis());
  }
}
