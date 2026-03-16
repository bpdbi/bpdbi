package io.djb.mysql.impl.codec;

import io.djb.ColumnDescriptor;
import io.djb.impl.ByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.djb.mysql.impl.codec.MysqlProtocolConstants.*;

/**
 * Decodes MySQL backend (server → client) protocol packets from an InputStream.
 * MySQL packets: [3-byte length LE] [1-byte sequence ID] [payload]
 */
public final class MysqlDecoder {

    private final InputStream in;
    private int lastSequenceId;
    private boolean deprecateEof = false;

    public MysqlDecoder(InputStream in) {
        this.in = in;
    }

    public void setDeprecateEof(boolean deprecateEof) {
        this.deprecateEof = deprecateEof;
    }

    public int lastSequenceId() {
        return lastSequenceId;
    }

    /**
     * Read one complete MySQL packet. Returns raw payload bytes.
     */
    public byte[] readPacket() throws IOException {
        // Read 3-byte length (LE) + 1-byte sequence ID
        byte[] header = readExactly(4);
        int length = (header[0] & 0xFF) | ((header[1] & 0xFF) << 8) | ((header[2] & 0xFF) << 16);
        lastSequenceId = header[3] & 0xFF;

        byte[] payload = readExactly(length);

        // Handle large packets (>= 16MB - 1): accumulate continuations
        if (length == PACKET_PAYLOAD_LIMIT) {
            var accumulated = new java.io.ByteArrayOutputStream();
            accumulated.write(payload);
            while (length == PACKET_PAYLOAD_LIMIT) {
                header = readExactly(4);
                length = (header[0] & 0xFF) | ((header[1] & 0xFF) << 8) | ((header[2] & 0xFF) << 16);
                lastSequenceId = header[3] & 0xFF;
                payload = readExactly(length);
                accumulated.write(payload);
            }
            return accumulated.toByteArray();
        }
        return payload;
    }

    /**
     * Read an OK packet from payload. Assumes first byte is 0x00 or 0xFE.
     */
    public OkPacket readOkPacket(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        buf.readByte(); // header (0x00 or 0xFE)
        int affectedRows = (int) readLengthEncodedInt(buf);
        long lastInsertId = readLengthEncodedInt(buf);
        int serverStatus = readShortLE(buf);
        int warnings = readShortLE(buf);
        return new OkPacket(affectedRows, lastInsertId, serverStatus, warnings);
    }

    /**
     * Read an ERR packet from payload.
     */
    public ErrPacket readErrPacket(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        buf.readByte(); // header (0xFF)
        int errorCode = readShortLE(buf);
        buf.readByte(); // '#' marker
        byte[] stateBytes = new byte[5];
        buf.readBytes(stateBytes);
        String sqlState = new String(stateBytes, StandardCharsets.US_ASCII);
        String message = new String(payload, buf.readerIndex(), payload.length - buf.readerIndex(), StandardCharsets.UTF_8);
        return new ErrPacket(errorCode, sqlState, message);
    }

    /**
     * Read an EOF packet.
     */
    public EofPacket readEofPacket(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        buf.readByte(); // header (0xFE)
        int warnings = readShortLE(buf);
        int serverStatus = readShortLE(buf);
        return new EofPacket(warnings, serverStatus);
    }

    /**
     * Read column count from a result set response.
     */
    public int readColumnCount(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        return (int) readLengthEncodedInt(buf);
    }

    /**
     * Read a column definition packet.
     */
    public ColumnDescriptor readColumnDefinition(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        skipLengthEncodedString(buf); // catalog
        skipLengthEncodedString(buf); // schema
        skipLengthEncodedString(buf); // table
        skipLengthEncodedString(buf); // orgTable
        String name = readLengthEncodedString(buf, StandardCharsets.UTF_8);
        skipLengthEncodedString(buf); // orgName
        readLengthEncodedInt(buf); // length of fixed-length fields (always 0x0c)
        int characterSet = readShortLE(buf);
        int columnLength = readIntLE(buf);
        int typeId = buf.readUnsignedByte();
        int flags = readShortLE(buf);
        int decimals = buf.readUnsignedByte();
        // 2 bytes filler
        buf.skipBytes(2);

        return new ColumnDescriptor(name, 0, (short) 0, typeId, (short) 0, 0);
    }

    /**
     * Read a text result row (length-encoded strings, 0xFB = NULL).
     */
    public byte[][] readTextRow(byte[] payload, int columnCount) {
        var buf = ByteBuffer.wrap(payload);
        byte[][] values = new byte[columnCount][];
        for (int i = 0; i < columnCount; i++) {
            int firstByte = buf.readUnsignedByte();
            if (firstByte == 0xFB) {
                values[i] = null; // NULL
            } else {
                // Put the byte back and read length-encoded string
                buf.readerIndex(buf.readerIndex() - 1);
                long length = readLengthEncodedInt(buf);
                values[i] = new byte[(int) length];
                buf.readBytes(values[i]);
            }
        }
        return values;
    }

    /**
     * Parse the initial handshake packet sent by the server.
     */
    public HandshakePacket readHandshake(byte[] payload) {
        var buf = ByteBuffer.wrap(payload);
        int protocolVersion = buf.readUnsignedByte();
        String serverVersion = readNullTerminatedString(buf, StandardCharsets.US_ASCII);
        int connectionId = readIntLE(buf);

        byte[] authPluginData = new byte[NONCE_LENGTH];
        buf.readBytes(authPluginData, 0, 8); // auth-plugin-data-part-1

        buf.readByte(); // filler

        int serverCapsLow = readShortLE(buf);
        int charset = buf.readUnsignedByte();
        int statusFlags = readShortLE(buf);
        int serverCapsHigh = readShortLE(buf);
        int serverCapabilities = serverCapsLow | (serverCapsHigh << 16);

        int authPluginDataLen = 0;
        if ((serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
            authPluginDataLen = buf.readUnsignedByte();
        } else {
            buf.skipBytes(1);
        }

        buf.skipBytes(10); // reserved

        // auth-plugin-data-part-2
        int part2Len = Math.max(NONCE_LENGTH - 8, authPluginDataLen - 9);
        buf.readBytes(authPluginData, 8, part2Len);
        buf.readByte(); // reserved byte

        String authPluginName = "";
        if ((serverCapabilities & CLIENT_PLUGIN_AUTH) != 0 && buf.readableBytes() > 0) {
            authPluginName = readNullTerminatedString(buf, StandardCharsets.UTF_8);
        }

        return new HandshakePacket(protocolVersion, serverVersion, connectionId,
            authPluginData, serverCapabilities, charset, statusFlags, authPluginName);
    }

    public boolean isDeprecateEof() {
        return deprecateEof;
    }

    // --- Low-level helpers ---

    private byte[] readExactly(int n) throws IOException {
        byte[] buf = new byte[n];
        int offset = 0;
        while (offset < n) {
            int read = in.read(buf, offset, n - offset);
            if (read == -1) {
                throw new IOException("Unexpected end of stream (needed " + n + " bytes, got " + offset + ")");
            }
            offset += read;
        }
        return buf;
    }

    static long readLengthEncodedInt(ByteBuffer buf) {
        int firstByte = buf.readUnsignedByte();
        return switch (firstByte) {
            case 0xFB -> -1; // NULL
            case 0xFC -> (buf.readUnsignedByte()) | (buf.readUnsignedByte() << 8);
            case 0xFD -> (buf.readUnsignedByte()) | (buf.readUnsignedByte() << 8) | (buf.readUnsignedByte() << 16);
            case 0xFE -> {
                long val = 0;
                for (int i = 0; i < 8; i++) {
                    val |= ((long) buf.readUnsignedByte()) << (i * 8);
                }
                yield val;
            }
            default -> firstByte;
        };
    }

    static int readShortLE(ByteBuffer buf) {
        return (buf.readUnsignedByte()) | (buf.readUnsignedByte() << 8);
    }

    static int readIntLE(ByteBuffer buf) {
        return (buf.readUnsignedByte()) | (buf.readUnsignedByte() << 8)
             | (buf.readUnsignedByte() << 16) | (buf.readUnsignedByte() << 24);
    }

    public static String readNullTerminatedString(ByteBuffer buf, Charset charset) {
        int start = buf.readerIndex();
        while (buf.readByte() != 0) { }
        int end = buf.readerIndex() - 1;
        int len = end - start;
        byte[] bytes = new byte[len];
        buf.readerIndex(start);
        buf.readBytes(bytes);
        buf.readByte(); // skip null
        return new String(bytes, charset);
    }

    static String readLengthEncodedString(ByteBuffer buf, Charset charset) {
        long length = readLengthEncodedInt(buf);
        if (length < 0) return null;
        byte[] bytes = new byte[(int) length];
        buf.readBytes(bytes);
        return new String(bytes, charset);
    }

    static void skipLengthEncodedString(ByteBuffer buf) {
        long length = readLengthEncodedInt(buf);
        if (length > 0) buf.skipBytes((int) length);
    }

    // --- Record types ---

    public record HandshakePacket(int protocolVersion, String serverVersion, int connectionId,
                                   byte[] authPluginData, int serverCapabilities,
                                   int charset, int statusFlags, String authPluginName) {}

    public record OkPacket(int affectedRows, long lastInsertId, int serverStatus, int warnings) {}
    public record ErrPacket(int errorCode, String sqlState, String message) {}
    public record EofPacket(int warnings, int serverStatus) {}
}
