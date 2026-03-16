package io.djb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        var c = ConnectionConfig.fromUri("postgresql://u:p@host/db?sslmode=require&application_name=myapp");
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
        var c = new ConnectionConfig()
            .host("myhost")
            .port(5432)
            .database("mydb")
            .username("user")
            .password("pass");
        assertEquals("myhost", c.host());
        assertEquals(5432, c.port());
        assertEquals("mydb", c.database());
    }
}
