package io.github.bpdbi.pg.impl.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PgEncoderTest {

  @Test
  void startupMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeStartupMessage("testuser", "testdb", null);
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    int length = buf.readInt();
    assertEquals(bytes.length, length); // startup message includes length in its own count
    assertEquals(3, buf.readShort()); // protocol major
    assertEquals(0, buf.readShort()); // protocol minor
    assertEquals("user", buf.readCString());
    assertEquals("testuser", buf.readCString());
    assertEquals("database", buf.readCString());
    assertEquals("testdb", buf.readCString());
    assertEquals(0, buf.readByte()); // terminator
  }

  @Test
  void startupMessageWithProperties() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeStartupMessage("u", "d", Map.of("application_name", "bpdbi-test"));
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    buf.readInt(); // length
    buf.readShort();
    buf.readShort(); // protocol version
    assertEquals("user", buf.readCString());
    assertEquals("u", buf.readCString());
    assertEquals("database", buf.readCString());
    assertEquals("d", buf.readCString());
    assertEquals("application_name", buf.readCString());
    assertEquals("bpdbi-test", buf.readCString());
    assertEquals(0, buf.readByte());
  }

  @Test
  void passwordMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writePasswordMessage("secret");
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    assertEquals('p', buf.readByte());
    int length = buf.readInt();
    assertEquals(bytes.length - 1, length);
    assertEquals("secret", buf.readCString());
  }

  @Test
  void parseMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeParse("SELECT $1::int", new int[] {23}); // OID 23 = int4
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    assertEquals('P', buf.readByte());
    int length = buf.readInt();
    assertEquals(bytes.length - 1, length);
    assertEquals(0, buf.readByte()); // unnamed statement
    assertEquals("SELECT $1::int", buf.readCString());
    assertEquals(1, buf.readShort()); // 1 parameter
    assertEquals(23, buf.readInt()); // OID
  }

  @Test
  void parseMessageNoParams() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeParse("SELECT 1", null);
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    assertEquals('P', buf.readByte());
    buf.readInt(); // length
    assertEquals(0, buf.readByte()); // unnamed
    assertEquals("SELECT 1", buf.readCString());
    assertEquals(0, buf.readShort()); // 0 parameters
  }

  @Test
  void executeMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeExecute();
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    assertEquals('E', buf.readByte());
    int length = buf.readInt();
    assertEquals(bytes.length - 1, length);
    assertEquals(0, buf.readByte()); // unnamed portal
    assertEquals(0, buf.readInt()); // no row limit
  }

  @Test
  void syncMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeSync();
    byte[] bytes = flush(encoder);

    assertEquals(5, bytes.length);
    var buf = ByteBuffer.wrap(bytes);
    assertEquals('S', buf.readByte());
    assertEquals(4, buf.readInt());
  }

  @Test
  void terminateMessage() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeTerminate();
    byte[] bytes = flush(encoder);

    assertEquals(5, bytes.length);
    var buf = ByteBuffer.wrap(bytes);
    assertEquals('X', buf.readByte());
    assertEquals(4, buf.readInt());
  }

  @Test
  void describePortal() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeDescribePortal();
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);
    assertEquals('D', buf.readByte());
    int length = buf.readInt();
    assertEquals(bytes.length - 1, length);
    assertEquals('P', buf.readByte());
    assertEquals(0, buf.readByte()); // unnamed
  }

  @Test
  void multipleMessagesInSingleFlush() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeParse("SELECT 1", null);
    encoder.writeParse("SELECT 2", null);
    encoder.writeSync();
    byte[] bytes = flush(encoder);

    var buf = ByteBuffer.wrap(bytes);

    // First parse
    assertEquals('P', buf.readByte());
    buf.readInt(); // length
    assertEquals(0, buf.readByte()); // unnamed
    assertEquals("SELECT 1", buf.readCString());
    assertEquals(0, buf.readShort()); // 0 params

    // Second parse
    assertEquals('P', buf.readByte());
    buf.readInt(); // length
    assertEquals(0, buf.readByte()); // unnamed
    assertEquals("SELECT 2", buf.readCString());
    assertEquals(0, buf.readShort()); // 0 params

    // Sync
    assertEquals('S', buf.readByte());
    assertEquals(4, buf.readInt());

    assertEquals(0, buf.readableBytes());
  }

  @Test
  void sslRequest() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeSSLRequest();
    byte[] bytes = flush(encoder);

    assertEquals(8, bytes.length);
    var buf = ByteBuffer.wrap(bytes);
    assertEquals(8, buf.readInt()); // length
    assertEquals(80877103, buf.readInt()); // SSL code
  }

  @Test
  void flushClearsBuffer() throws IOException {
    var encoder = new PgEncoder();
    encoder.writeParse("SELECT 1", null);
    flush(encoder);

    // Second flush should produce nothing
    var baos = new ByteArrayOutputStream();
    encoder.flush(baos);
    assertEquals(0, baos.size());
  }

  @Test
  void estimateExtendedQuerySize() throws IOException {
    // Verify the estimate is at least as large as the actual encoded bytes
    var encoder = new PgEncoder();
    String sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
    Object[] params = {42, "Alice"};

    int estimate = PgEncoder.estimateExtendedQuerySize(sql, params);

    // Encode the same sequence and measure actual size
    int[] typeOIDs = {23, 25}; // INT4, TEXT
    encoder.writeParse(sql, typeOIDs);
    encoder.writeBindInline(PgEncoder.EMPTY_CSTRING, PgEncoder.EMPTY_CSTRING, params, null);
    encoder.writeDescribePortal();
    encoder.writeExecute();
    byte[] actual = flush(encoder);

    // Estimate must be >= actual size (it's a lower bound for pre-sizing)
    assertTrue(
        estimate >= actual.length,
        "estimate ("
            + estimate
            + ") should be >= actual ("
            + actual.length
            + ") for pre-sizing to avoid resizing");
  }

  @Test
  void ensureCapacityPreventsResizing() throws IOException {
    var encoder = new PgEncoder();
    // Pre-size for a large batch
    encoder.ensureCapacity(8192);
    // Should still work correctly
    encoder.writeParse("SELECT 1", null);
    byte[] bytes = flush(encoder);
    var buf = ByteBuffer.wrap(bytes);
    assertEquals('P', buf.readByte());
  }

  private byte[] flush(PgEncoder encoder) throws IOException {
    var baos = new ByteArrayOutputStream();
    encoder.flush(baos);
    return baos.toByteArray();
  }
}
