package io.djb.kotlin

import io.djb.pg.PgConnection
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.PostgreSQLContainer

/** Integration tests for djb-kotlin with a real PostgreSQL database. */
class PgIntegrationTest {

    companion object {
        lateinit var pg: PostgreSQLContainer<*>

        @BeforeAll
        @JvmStatic
        fun startContainer() {
            pg = PostgreSQLContainer("postgres:16-alpine")
            pg.start()
        }

        @AfterAll
        @JvmStatic
        fun stopContainer() {
            pg.stop()
        }
    }

    private fun connect(): PgConnection = PgConnection.connect(
        pg.host,
        pg.getMappedPort(5432),
        pg.databaseName,
        pg.username,
        pg.password
    )

    // --- Data classes ---

    @Serializable
    data class User(val id: Int, val name: String, val email: String)

    @Serializable
    data class UserNullable(val id: Int, val name: String, val email: String?)

    @Serializable
    data class UserAddress(val id: Int, val name: String, val street: String, val city: String)

    @Serializable
    data class Address(val street: String, val city: String)

    @Serializable
    data class UserWithAddress(val id: Int, val name: String, val address: Address)

    @Serializable
    data class OrderMeta(val source: String, val priority: Int)

    @Serializable
    data class OrderRow(val id: Int, val user_id: Int, @SqlJsonValue val meta: OrderMeta)

    @Serializable
    data class OrderRowNullable(val id: Int, val user_id: Int, @SqlJsonValue val meta: OrderMeta?)

    @Serializable
    enum class Status { ACTIVE, INACTIVE }

    @Serializable
    data class UserStatus(val id: Int, val name: String, val status: Status)

    // --- Tests ---

    @Test
    fun `deserializeToList simple data class`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com')")

            val users: List<User> = conn.query("SELECT id, name, email FROM users ORDER BY id")
                .deserializeToList<User>()

            assertEquals(2, users.size)
            assertEquals(User(1, "Alice", "alice@test.com"), users[0])
            assertEquals(User(2, "Bob", "bob@test.com"), users[1])
        }
    }

    @Test
    fun `deserializeFirst with parameters`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com')")

            val user: User = conn.query("SELECT id, name, email FROM users WHERE id = $1", 2)
                .deserializeFirst<User>()

            assertEquals(User(2, "Bob", "bob@test.com"), user)
        }
    }

    @Test
    fun `queryAs convenience`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')")

            val users: List<User> = conn.queryAs("SELECT id, name, email FROM users")
            assertEquals(1, users.size)
            assertEquals(User(1, "Alice", "alice@test.com"), users[0])
        }
    }

    @Test
    fun `queryOneAs convenience`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')")

            val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = $1", 1)
            assertEquals(User(1, "Alice", "alice@test.com"), user)
        }
    }

    @Test
    fun `queryAs with named params`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com')")

            val users: List<User> = conn.queryAs(
                "SELECT id, name, email FROM users WHERE id = :id",
                mapOf("id" to 1)
            )
            assertEquals(1, users.size)
            assertEquals(User(1, "Alice", "alice@test.com"), users[0])
        }
    }

    @Test
    fun `nested data class from JOIN`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text)")
            conn.query("CREATE TEMP TABLE addresses (user_id int, street text, city text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice')")
            conn.query("INSERT INTO addresses VALUES (1, '123 Main St', 'Springfield')")

            val results: List<UserWithAddress> = conn.query(
                "SELECT u.id, u.name, a.street, a.city FROM users u JOIN addresses a ON u.id = a.user_id"
            ).deserializeToList<UserWithAddress>()

            assertEquals(1, results.size)
            assertEquals(UserWithAddress(1, "Alice", Address("123 Main St", "Springfield")), results[0])
        }
    }

    @Test
    fun `SqlJsonValue from jsonb column`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE orders (id int, user_id int, meta jsonb)")
            conn.query(
                "INSERT INTO orders VALUES (1, 10, $1::jsonb)",
                """{"source":"web","priority":5}"""
            )

            val orders: List<OrderRow> = conn.query("SELECT id, user_id, meta FROM orders")
                .deserializeToList<OrderRow>()

            assertEquals(1, orders.size)
            assertEquals(OrderRow(1, 10, OrderMeta("web", 5)), orders[0])
        }
    }

    @Test
    fun `nullable SqlJsonValue column`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE orders (id int, user_id int, meta jsonb)")
            conn.query("INSERT INTO orders VALUES (1, 10, NULL)")

            val orders: List<OrderRowNullable> = conn.query("SELECT id, user_id, meta FROM orders")
                .deserializeToList<OrderRowNullable>()

            assertEquals(1, orders.size)
            assertEquals(OrderRowNullable(1, 10, null), orders[0])
        }
    }

    @Test
    fun `nullable fields`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")
            conn.query("INSERT INTO users VALUES (1, 'Alice', NULL)")

            val users: List<UserNullable> = conn.query("SELECT id, name, email FROM users")
                .deserializeToList<UserNullable>()

            assertEquals(1, users.size)
            assertEquals(UserNullable(1, "Alice", null), users[0])
        }
    }

    @Test
    fun `empty result set`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")

            val users: List<User> = conn.query("SELECT id, name, email FROM users")
                .deserializeToList<User>()

            assertTrue(users.isEmpty())
        }
    }

    @Test
    fun `queryOneAs returns null for empty result`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE users (id int, name text, email text)")

            val user: User? = conn.queryOneAs("SELECT id, name, email FROM users WHERE id = $1", 999)
            assertNull(user)
        }
    }

    @Test
    fun `enum deserialization`() {
        connect().use { conn ->
            conn.query("CREATE TEMP TABLE user_status (id int, name text, status text)")
            conn.query("INSERT INTO user_status VALUES (1, 'Alice', 'ACTIVE')")

            val result: List<UserStatus> = conn.query("SELECT id, name, status FROM user_status")
                .deserializeToList<UserStatus>()

            assertEquals(1, result.size)
            assertEquals(UserStatus(1, "Alice", Status.ACTIVE), result[0])
        }
    }

    @Test
    fun `kotlin Uuid type binder and mapper`() {
        connect().use { conn ->
            conn.useKotlinTypes()
            val uuid = kotlin.uuid.Uuid.random()

            conn.query("CREATE TEMP TABLE items (id uuid, name text)")
            conn.query("INSERT INTO items VALUES ($1, $2)", uuid, "test")

            val rs = conn.query("SELECT id FROM items")
            val retrieved = rs.first().getString(0)
            assertEquals(uuid.toString(), retrieved)
        }
    }
}
