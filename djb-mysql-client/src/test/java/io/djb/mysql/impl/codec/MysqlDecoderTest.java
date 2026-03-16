package io.djb.mysql.impl.codec;

import io.djb.ColumnDescriptor;
import io.djb.impl.ByteBuffer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MysqlDecoderTest {

    // =====================================================================
    // readPacket
    // =====================================================================

    @Test
    void readPacketNormal() throws IOException {
        byte[] payload = {0x01, 0x02, 0x03};
        byte[] packet = makePacket(payload, 0);
        var decoder = new MysqlDecoder(new ByteArrayInputStream(packet));

        byte[] result = decoder.readPacket();
        assertArrayEquals(payload, result);
        assertEquals(0, decoder.lastSequenceId());
    }

    @Test
    void readPacketSequenceId() throws IOException {
        byte[] payload = {0x42};
        byte[] packet = makePacket(payload, 5);
        var decoder = new MysqlDecoder(new ByteArrayInputStream(packet));

        decoder.readPacket();
        assertEquals(5, decoder.lastSequenceId());
    }

    @Test
    void readMultiplePackets() throws IOException {
        byte[] payload1 = {0x01};
        byte[] payload2 = {0x02, 0x03};
        byte[] combined = new byte[5 + 6]; // 4+1 + 4+2
        System.arraycopy(makePacket(payload1, 0), 0, combined, 0, 5);
        System.arraycopy(makePacket(payload2, 1), 0, combined, 5, 6);
        var decoder = new MysqlDecoder(new ByteArrayInputStream(combined));

        assertArrayEquals(payload1, decoder.readPacket());
        assertEquals(0, decoder.lastSequenceId());
        assertArrayEquals(payload2, decoder.readPacket());
        assertEquals(1, decoder.lastSequenceId());
    }

    @Test
    void readPacketUnexpectedEndOfStream() {
        // Only 2 bytes when 4 are needed for header
        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[]{0x01, 0x00}));
        assertThrows(IOException.class, decoder::readPacket);
    }

    // =====================================================================
    // readOkPacket
    // =====================================================================

    @Test
    void readOkPacket() {
        var buf = new ByteBuffer(32);
        buf.writeByte(0x00); // header
        buf.writeByte(3);    // affectedRows (length-encoded int)
        buf.writeByte(42);   // lastInsertId
        writeShortLE(buf, 0x0002); // serverStatus
        writeShortLE(buf, 1);      // warnings

        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        var ok = decoder.readOkPacket(buf.toByteArray());

        assertEquals(3, ok.affectedRows());
        assertEquals(42, ok.lastInsertId());
        assertEquals(0x0002, ok.serverStatus());
        assertEquals(1, ok.warnings());
    }

    // =====================================================================
    // readErrPacket
    // =====================================================================

    @Test
    void readErrPacket() {
        var buf = new ByteBuffer(64);
        buf.writeByte(0xFF); // header
        writeShortLE(buf, 1045); // errorCode
        buf.writeByte('#');
        buf.writeBytes("42000".getBytes(StandardCharsets.US_ASCII)); // sqlState
        buf.writeString("Access denied"); // message

        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        var err = decoder.readErrPacket(buf.toByteArray());

        assertEquals(1045, err.errorCode());
        assertEquals("42000", err.sqlState());
        assertEquals("Access denied", err.message());
    }

    // =====================================================================
    // readEofPacket
    // =====================================================================

    @Test
    void readEofPacket() {
        var buf = new ByteBuffer(16);
        buf.writeByte(0xFE); // header
        writeShortLE(buf, 2); // warnings
        writeShortLE(buf, 0x0022); // serverStatus

        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        var eof = decoder.readEofPacket(buf.toByteArray());

        assertEquals(2, eof.warnings());
        assertEquals(0x0022, eof.serverStatus());
    }

    // =====================================================================
    // readColumnCount
    // =====================================================================

    @Test
    void readColumnCount() {
        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        assertEquals(5, decoder.readColumnCount(new byte[]{5}));
    }

    @Test
    void readColumnCountTwoBytes() {
        // 0xFC prefix → 2-byte length-encoded int
        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        assertEquals(300, decoder.readColumnCount(new byte[]{(byte) 0xFC, 0x2C, 0x01}));
    }

    // =====================================================================
    // readTextRow
    // =====================================================================

    @Test
    void readTextRowNormal() {
        var buf = new ByteBuffer(64);
        // Column 0: "hello" (length-encoded string)
        buf.writeByte(5); // length
        buf.writeString("hello");
        // Column 1: NULL
        buf.writeByte(0xFB);
        // Column 2: "42"
        buf.writeByte(2);
        buf.writeString("42");

        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        byte[][] row = decoder.readTextRow(buf.toByteArray(), 3);

        assertEquals(3, row.length);
        assertEquals("hello", new String(row[0]));
        assertNull(row[1]);
        assertEquals("42", new String(row[2]));
    }

    // =====================================================================
    // readPrepareResult
    // =====================================================================

    @Test
    void readPrepareResult() {
        var buf = new ByteBuffer(16);
        buf.writeByte(0x00); // status
        writeIntLE(buf, 42); // statementId
        writeShortLE(buf, 3); // numColumns
        writeShortLE(buf, 2); // numParams
        buf.writeByte(0);    // filler
        writeShortLE(buf, 0); // numWarnings

        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        var result = decoder.readPrepareResult(buf.toByteArray());

        assertEquals(42, result.statementId());
        assertEquals(3, result.numColumns());
        assertEquals(2, result.numParams());
        assertEquals(0, result.numWarnings());
    }

    // =====================================================================
    // deprecateEof flag
    // =====================================================================

    @Test
    void deprecateEofDefault() {
        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        assertFalse(decoder.isDeprecateEof());
    }

    @Test
    void setDeprecateEof() {
        var decoder = new MysqlDecoder(new ByteArrayInputStream(new byte[0]));
        decoder.setDeprecateEof(true);
        assertTrue(decoder.isDeprecateEof());
    }

    // =====================================================================
    // Static helpers (length-encoded int)
    // =====================================================================

    @Test
    void readLengthEncodedIntSingleByte() {
        var buf = ByteBuffer.wrap(new byte[]{42});
        assertEquals(42, MysqlDecoder.readLengthEncodedInt(buf));
    }

    @Test
    void readLengthEncodedIntNull() {
        var buf = ByteBuffer.wrap(new byte[]{(byte) 0xFB});
        assertEquals(-1, MysqlDecoder.readLengthEncodedInt(buf));
    }

    @Test
    void readLengthEncodedIntTwoBytes() {
        // 0xFC + 2 bytes LE
        var buf = ByteBuffer.wrap(new byte[]{(byte) 0xFC, 0x2C, 0x01}); // 300
        assertEquals(300, MysqlDecoder.readLengthEncodedInt(buf));
    }

    @Test
    void readLengthEncodedIntThreeBytes() {
        // 0xFD + 3 bytes LE
        var buf = ByteBuffer.wrap(new byte[]{(byte) 0xFD, 0x01, 0x00, 0x01}); // 65537
        assertEquals(65537, MysqlDecoder.readLengthEncodedInt(buf));
    }

    @Test
    void readShortLE() {
        var buf = ByteBuffer.wrap(new byte[]{0x39, 0x05}); // 1337
        assertEquals(1337, MysqlDecoder.readShortLE(buf));
    }

    @Test
    void readIntLE() {
        var buf = ByteBuffer.wrap(new byte[]{0x01, 0x00, 0x00, 0x00});
        assertEquals(1, MysqlDecoder.readIntLE(buf));
    }

    @Test
    void readNullTerminatedString() {
        byte[] data = {'h', 'e', 'l', 'l', 'o', 0, 'x'};
        var buf = ByteBuffer.wrap(data);
        assertEquals("hello", MysqlDecoder.readNullTerminatedString(buf, StandardCharsets.UTF_8));
        assertEquals(6, buf.readerIndex()); // past the null
    }

    @Test
    void readLengthEncodedString() {
        var buf = ByteBuffer.wrap(new byte[]{5, 'h', 'e', 'l', 'l', 'o'});
        assertEquals("hello", MysqlDecoder.readLengthEncodedString(buf, StandardCharsets.UTF_8));
    }

    @Test
    void readLengthEncodedStringNull() {
        var buf = ByteBuffer.wrap(new byte[]{(byte) 0xFB});
        assertNull(MysqlDecoder.readLengthEncodedString(buf, StandardCharsets.UTF_8));
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private static byte[] makePacket(byte[] payload, int sequenceId) {
        byte[] packet = new byte[4 + payload.length];
        packet[0] = (byte) (payload.length & 0xFF);
        packet[1] = (byte) ((payload.length >> 8) & 0xFF);
        packet[2] = (byte) ((payload.length >> 16) & 0xFF);
        packet[3] = (byte) (sequenceId & 0xFF);
        System.arraycopy(payload, 0, packet, 4, payload.length);
        return packet;
    }

    private static void writeShortLE(ByteBuffer buf, int value) {
        buf.writeByte(value & 0xFF);
        buf.writeByte((value >> 8) & 0xFF);
    }

    private static void writeIntLE(ByteBuffer buf, int value) {
        buf.writeByte(value & 0xFF);
        buf.writeByte((value >> 8) & 0xFF);
        buf.writeByte((value >> 16) & 0xFF);
        buf.writeByte((value >> 24) & 0xFF);
    }
}
