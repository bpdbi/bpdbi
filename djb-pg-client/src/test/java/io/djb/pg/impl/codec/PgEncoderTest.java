package io.djb.pg.impl.codec;

import io.djb.impl.ByteBuffer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
        encoder.writeStartupMessage("u", "d", Map.of("application_name", "djb-test"));
        byte[] bytes = flush(encoder);

        var buf = ByteBuffer.wrap(bytes);
        buf.readInt(); // length
        buf.readShort(); buf.readShort(); // protocol version
        assertEquals("user", buf.readCString());
        assertEquals("u", buf.readCString());
        assertEquals("database", buf.readCString());
        assertEquals("d", buf.readCString());
        assertEquals("application_name", buf.readCString());
        assertEquals("djb-test", buf.readCString());
        assertEquals(0, buf.readByte());
    }

    @Test
    void queryMessage() throws IOException {
        var encoder = new PgEncoder();
        encoder.writeQuery("SELECT 1");
        byte[] bytes = flush(encoder);

        var buf = ByteBuffer.wrap(bytes);
        assertEquals('Q', buf.readByte());
        int length = buf.readInt();
        assertEquals(bytes.length - 1, length); // length doesn't include the type byte
        assertEquals("SELECT 1", buf.readCString());
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
        encoder.writeParse("SELECT $1::int", new int[]{23}); // OID 23 = int4
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
    void bindMessage() throws IOException {
        var encoder = new PgEncoder();
        encoder.writeBind(new String[]{"42", null, "hello"});
        byte[] bytes = flush(encoder);

        var buf = ByteBuffer.wrap(bytes);
        assertEquals('B', buf.readByte());
        int length = buf.readInt();
        assertEquals(bytes.length - 1, length);
        assertEquals(0, buf.readByte()); // unnamed portal
        assertEquals(0, buf.readByte()); // unnamed statement
        assertEquals(0, buf.readShort()); // format codes: default (text)
        assertEquals(3, buf.readShort()); // 3 parameters

        // param 0: "42"
        int len0 = buf.readInt();
        assertEquals(2, len0);
        byte[] p0 = new byte[2];
        buf.readBytes(p0);
        assertEquals("42", new String(p0));

        // param 1: NULL
        assertEquals(-1, buf.readInt());

        // param 2: "hello"
        int len2 = buf.readInt();
        assertEquals(5, len2);
        byte[] p2 = new byte[5];
        buf.readBytes(p2);
        assertEquals("hello", new String(p2));

        assertEquals(1, buf.readShort()); // 1 format code for all result columns
        assertEquals(1, buf.readShort()); // binary format (1)
    }

    @Test
    void bindMessageNoParams() throws IOException {
        var encoder = new PgEncoder();
        encoder.writeBind(null);
        byte[] bytes = flush(encoder);

        var buf = ByteBuffer.wrap(bytes);
        assertEquals('B', buf.readByte());
        buf.readInt(); // length
        assertEquals(0, buf.readByte()); // unnamed portal
        assertEquals(0, buf.readByte()); // unnamed statement
        assertEquals(0, buf.readShort()); // format codes
        assertEquals(0, buf.readShort()); // 0 parameters
        assertEquals(1, buf.readShort()); // 1 format code for all result columns
        assertEquals(1, buf.readShort()); // binary format (1)
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
        encoder.writeQuery("SELECT 1");
        encoder.writeQuery("SELECT 2");
        encoder.writeSync();
        byte[] bytes = flush(encoder);

        var buf = ByteBuffer.wrap(bytes);

        // First query
        assertEquals('Q', buf.readByte());
        int len1 = buf.readInt();
        assertEquals("SELECT 1", buf.readCString());

        // Second query
        assertEquals('Q', buf.readByte());
        int len2 = buf.readInt();
        assertEquals("SELECT 2", buf.readCString());

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
        encoder.writeQuery("SELECT 1");
        flush(encoder);

        // Second flush should produce nothing
        var baos = new ByteArrayOutputStream();
        encoder.flush(baos);
        assertEquals(0, baos.size());
    }

    private byte[] flush(PgEncoder encoder) throws IOException {
        var baos = new ByteArrayOutputStream();
        encoder.flush(baos);
        return baos.toByteArray();
    }
}
