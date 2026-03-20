package io.github.bpdbi.benchmark.infra;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.bpdbi.benchmark.model.EventEntity;
import io.github.bpdbi.benchmark.model.OrderEntity;
import io.github.bpdbi.benchmark.model.OrderItemEntity;
import io.github.bpdbi.benchmark.model.ProductEntity;
import io.github.bpdbi.benchmark.model.UserEntity;
import io.github.bpdbi.core.Connection;
import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.pg.PgConnection;
import io.github.bpdbi.pool.ConnectionPool;
import io.github.bpdbi.pool.PoolConfig;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.kotlin.KotlinPlugin;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

@State(Scope.Benchmark)
public class DatabaseState {

  private static final int POOL_SIZE = 10;
  private static final int USER_COUNT = 1000;
  private static final int PRODUCT_COUNT = 500;
  private static final int ORDER_COUNT = 5000;
  private static final String[] CATEGORIES = {"electronics", "books", "clothing", "home", "sports"};
  private static final String[] STATUSES = {"pending", "shipped", "delivered", "cancelled"};

  /** Latency in ms added to each direction (upstream + downstream). Set via -DbenchLatencyMs. */
  private static final int LATENCY_MS = Integer.parseInt(System.getProperty("benchLatencyMs", "0"));

  private static final String PG_NETWORK_ALIAS = "pg";

  private Network network;
  private PostgreSQLContainer<?> postgres;
  private ToxiproxyContainer toxiproxy;
  private Proxy pgProxy;

  // Connection coordinates (through proxy when latency > 0, direct otherwise)
  private String benchHost;
  private int benchPort;
  private String pgDatabase;
  private String pgUser;
  private String pgPassword;

  // Direct coordinates (for seeding, always bypass proxy)
  private String directHost;
  private int directPort;

  // bpdbi
  private ConnectionPool bpdbiPool;

  // Competitors
  private HikariDataSource hikariDataSource;
  private Jdbi jdbi;
  private SessionFactory sessionFactory;

  // Vert.x
  private Vertx vertx;
  private PgConnectOptions vertxConnectOptions;
  private Pool vertxPool;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    startContainers();
    createSchema();
    seedData();
    createPools();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (bpdbiPool != null) bpdbiPool.close();
    if (vertxPool != null) vertxPool.close().toCompletionStage().toCompletableFuture().join();
    if (vertx != null) vertx.close().toCompletionStage().toCompletableFuture().join();
    if (hikariDataSource != null) hikariDataSource.close();
    if (sessionFactory != null) sessionFactory.close();
    if (toxiproxy != null) toxiproxy.stop();
    if (postgres != null) postgres.stop();
    if (network != null) network.close();
  }

  public Connection newBpdbiConnection() {
    var config = new ConnectionConfig(benchHost, benchPort, pgDatabase, pgUser, pgPassword);
    config.cachePreparedStatements(true);
    return PgConnection.connect(config);
  }

  public ConnectionPool bpdbiPool() {
    return bpdbiPool;
  }

  public HikariDataSource hikariDataSource() {
    return hikariDataSource;
  }

  public Jdbi jdbi() {
    return jdbi;
  }

  public SessionFactory sessionFactory() {
    return sessionFactory;
  }

  public synchronized Vertx vertx() {
    if (vertx == null) {
      vertx = Vertx.vertx();
    }
    return vertx;
  }

  public PgConnectOptions vertxConnectOptions() {
    if (vertxConnectOptions == null) {
      vertxConnectOptions =
          new PgConnectOptions()
              .setHost(benchHost)
              .setPort(benchPort)
              .setDatabase(pgDatabase)
              .setUser(pgUser)
              .setPassword(pgPassword);
    }
    return vertxConnectOptions;
  }

  public synchronized Pool vertxPool() {
    if (vertxPool == null) {
      vertxPool =
          Pool.pool(vertx(), vertxConnectOptions(), new PoolOptions().setMaxSize(POOL_SIZE));
    }
    return vertxPool;
  }

  private Connection newDirectConnection() {
    return PgConnection.connect(directHost, directPort, pgDatabase, pgUser, pgPassword);
  }

  private void startContainers() throws IOException {
    network = Network.newNetwork();

    postgres =
        new PostgreSQLContainer<>("postgres:16-alpine")
            .withNetwork(network)
            .withNetworkAliases(PG_NETWORK_ALIAS);
    postgres.start();

    pgDatabase = postgres.getDatabaseName();
    pgUser = postgres.getUsername();
    pgPassword = postgres.getPassword();

    // Direct connection coordinates (bypass proxy, used for seeding)
    directHost = postgres.getHost();
    directPort = postgres.getMappedPort(5432);

    if (LATENCY_MS > 0) {
      toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0").withNetwork(network);
      toxiproxy.start();

      var toxiClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
      pgProxy = toxiClient.createProxy("pg", "0.0.0.0:8666", PG_NETWORK_ALIAS + ":5432");
      pgProxy.toxics().latency("latency-down", ToxicDirection.DOWNSTREAM, LATENCY_MS);
      pgProxy.toxics().latency("latency-up", ToxicDirection.UPSTREAM, LATENCY_MS);

      benchHost = toxiproxy.getHost();
      benchPort = toxiproxy.getMappedPort(8666);

      System.out.println(
          "[DatabaseState] Toxiproxy active: " + LATENCY_MS + "ms latency each direction");
    } else {
      benchHost = directHost;
      benchPort = directPort;
      System.out.println("[DatabaseState] No latency simulation (direct connection)");
    }
  }

  private void createSchema() {
    try (var conn = newDirectConnection()) {
      conn.query(
          """
          CREATE TABLE users (
              id SERIAL PRIMARY KEY,
              username VARCHAR(100) NOT NULL,
              email VARCHAR(200) NOT NULL,
              full_name VARCHAR(200) NOT NULL,
              bio TEXT NOT NULL,
              active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMP NOT NULL DEFAULT NOW()
          )""");
      conn.query(
          """
          CREATE TABLE products (
              id SERIAL PRIMARY KEY,
              name VARCHAR(200) NOT NULL,
              description TEXT NOT NULL,
              price NUMERIC(10,2) NOT NULL,
              category VARCHAR(50) NOT NULL,
              stock INT NOT NULL DEFAULT 0
          )""");
      conn.query(
          """
          CREATE TABLE orders (
              id SERIAL PRIMARY KEY,
              user_id INT NOT NULL REFERENCES users(id),
              total NUMERIC(10,2) NOT NULL,
              status VARCHAR(20) NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT NOW()
          )""");
      conn.query(
          """
          CREATE TABLE order_items (
              id SERIAL PRIMARY KEY,
              order_id INT NOT NULL REFERENCES orders(id),
              product_id INT NOT NULL REFERENCES products(id),
              quantity INT NOT NULL,
              price NUMERIC(10,2) NOT NULL
          )""");
      conn.query(
          "CREATE TABLE bench_orders (id SERIAL PRIMARY KEY,"
              + " user_id INT NOT NULL, total NUMERIC(10,2) NOT NULL, status VARCHAR(20) NOT NULL)");
      conn.query(
          """
          CREATE TABLE events (
              id SERIAL PRIMARY KEY,
              event_uuid UUID NOT NULL,
              event_type VARCHAR(50) NOT NULL,
              user_id INT NOT NULL,
              sequence_num BIGINT NOT NULL,
              amount NUMERIC(12,2) NOT NULL,
              discount NUMERIC(12,2) NOT NULL,
              processed BOOLEAN NOT NULL,
              flagged BOOLEAN NOT NULL,
              source VARCHAR(100) NOT NULL,
              category VARCHAR(50) NOT NULL,
              notes TEXT,
              reference_code VARCHAR(100),
              correlation_id UUID NOT NULL,
              created_at TIMESTAMP NOT NULL,
              updated_at TIMESTAMP NOT NULL
          )""");
      conn.query("CREATE INDEX idx_events_user_id ON events(user_id)");
      conn.query("CREATE INDEX idx_products_category ON products(category)");
      conn.query("CREATE INDEX idx_orders_user_id ON orders(user_id)");
      conn.query("CREATE INDEX idx_order_items_order_id ON order_items(order_id)");
    }
  }

  private void seedData() {
    var rng = new Random(42);

    try (var conn = newDirectConnection()) {
      // Seed users
      for (int i = 1; i <= USER_COUNT; i++) {
        conn.enqueue(
            "INSERT INTO users (username, email, full_name, bio, active) VALUES ($1, $2, $3, $4, $5)",
            "user" + i,
            "user" + i + "@example.com",
            "User Number " + i,
            "Bio for user " + i + ". This is some filler text to make the row realistic.",
            rng.nextBoolean());
      }
      conn.flush();

      // Seed products
      for (int i = 1; i <= PRODUCT_COUNT; i++) {
        var price =
            BigDecimal.valueOf(rng.nextDouble() * 999 + 1).setScale(2, RoundingMode.HALF_UP);
        conn.enqueue(
            "INSERT INTO products (name, description, price, category, stock) VALUES ($1, $2, $3, $4, $5)",
            "Product " + i,
            "Description for product " + i + ". High quality item with great reviews.",
            price,
            CATEGORIES[rng.nextInt(CATEGORIES.length)],
            rng.nextInt(1000));
      }
      conn.flush();

      // Seed orders and order_items
      for (int i = 1; i <= ORDER_COUNT; i++) {
        var total =
            BigDecimal.valueOf(rng.nextDouble() * 500 + 10).setScale(2, RoundingMode.HALF_UP);
        conn.enqueue(
            "INSERT INTO orders (user_id, total, status) VALUES ($1, $2, $3)",
            rng.nextInt(USER_COUNT) + 1,
            total,
            STATUSES[rng.nextInt(STATUSES.length)]);
      }
      conn.flush();

      // Seed order_items (1-5 items per order)
      for (int orderId = 1; orderId <= ORDER_COUNT; orderId++) {
        int itemCount = rng.nextInt(5) + 1;
        for (int j = 0; j < itemCount; j++) {
          var price =
              BigDecimal.valueOf(rng.nextDouble() * 100 + 1).setScale(2, RoundingMode.HALF_UP);
          conn.enqueue(
              "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)",
              orderId,
              rng.nextInt(PRODUCT_COUNT) + 1,
              rng.nextInt(10) + 1,
              price);
        }
        // Flush in batches to avoid excessive pipeline depth
        if (orderId % 500 == 0) conn.flush();
      }
      conn.flush();
    }
  }

  private void createPools() {
    // bpdbi pool
    var poolConfig = new PoolConfig().maxSize(POOL_SIZE);
    bpdbiPool = new ConnectionPool(() -> newBpdbiConnection(), poolConfig);

    // HikariCP
    var hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName("org.postgresql.Driver");
    hikariConfig.setJdbcUrl(benchJdbcUrl());
    hikariConfig.setUsername(pgUser);
    hikariConfig.setPassword(pgPassword);
    hikariConfig.setMaximumPoolSize(POOL_SIZE);
    hikariDataSource = new HikariDataSource(hikariConfig);

    // Jdbi
    jdbi = Jdbi.create(hikariDataSource);
    jdbi.installPlugin(new KotlinPlugin());

    // Hibernate SessionFactory (programmatic, no persistence.xml)
    var cfg = new Configuration();
    cfg.setProperty("hibernate.connection.url", benchJdbcUrl());
    cfg.setProperty("hibernate.connection.username", pgUser);
    cfg.setProperty("hibernate.connection.password", pgPassword);
    cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
    cfg.setProperty("hibernate.connection.pool_size", String.valueOf(POOL_SIZE));
    cfg.setProperty("hibernate.show_sql", "false");
    cfg.setProperty("hibernate.hbm2ddl.auto", "none");
    cfg.addAnnotatedClass(UserEntity.class);
    cfg.addAnnotatedClass(ProductEntity.class);
    cfg.addAnnotatedClass(OrderEntity.class);
    cfg.addAnnotatedClass(OrderItemEntity.class);
    cfg.addAnnotatedClass(EventEntity.class);
    sessionFactory = cfg.buildSessionFactory();
  }

  private String benchJdbcUrl() {
    return "jdbc:postgresql://" + benchHost + ":" + benchPort + "/" + pgDatabase;
  }

  /** Map from category name to count, useful for parameterized benchmarks. */
  public static String categoryForParam(int param) {
    return CATEGORIES[param % CATEGORIES.length];
  }

  /** Return a valid user ID for the given param. */
  public static int userIdForParam(int param) {
    return (param % USER_COUNT) + 1;
  }
}
