package io.github.bpdbi.pg;

import io.github.bpdbi.core.ConnectionConfig;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.postgresql.PostgreSQLContainer;

abstract class PgTestBase {

  @SuppressWarnings("resource") // withReuse keeps container alive across test runs
  static final PostgreSQLContainer pg =
      new PostgreSQLContainer("postgres:16-alpine").withReuse(true);

  @BeforeAll
  static void startContainer() {
    pg.start();
  }

  protected PgConnection connect() {
    return PgConnection.connect(
        pg.getHost(),
        pg.getMappedPort(5432),
        pg.getDatabaseName(),
        pg.getUsername(),
        pg.getPassword());
  }

  protected PgConnection connect(ConnectionConfig config) {
    return PgConnection.connect(config);
  }

  protected String tempTableDDL(String name, String columns) {
    return "CREATE TEMP TABLE " + name + " (" + columns + ")";
  }
}
