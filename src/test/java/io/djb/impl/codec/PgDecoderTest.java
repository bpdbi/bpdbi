package io.djb.impl.codec;

import io.djb.impl.buffer.PgBuffer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class PgDecoderTest {

    @Test
    void decodeAuthenticationOk() throws IOException {
        var msg = decode(authOkMessage());
        assertInstanceOf(BackendMessage.AuthenticationOk.class, msg);
    }

    @Test
    void decodeAuthenticationMD5Password() throws IOException {
        var buf = new PgBuffer(32);
        buf.writeByte('R'); // Authentication
        buf.writeInt(12);   // length: 4 (len) + 4 (type) + 4 (salt)
        buf.writeInt(PgProtocolConstants.AUTH_MD5_PASSWORD);
        buf.writeBytes(new byte[]{1, 2, 3, 4}); // salt

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.AuthenticationMD5Password.class, msg);
        var md5 = (BackendMessage.AuthenticationMD5Password) msg;
        assertArrayEquals(new byte[]{1, 2, 3, 4}, md5.salt());
    }

    @Test
    void decodeAuthenticationCleartextPassword() throws IOException {
        var buf = new PgBuffer(16);
        buf.writeByte('R');
        buf.writeInt(8);
        buf.writeInt(PgProtocolConstants.AUTH_CLEARTEXT_PASSWORD);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.AuthenticationCleartextPassword.class, msg);
    }

    @Test
    void decodeParameterStatus() throws IOException {
        var buf = new PgBuffer(64);
        buf.writeByte('S');
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder
        buf.writeCString("server_version");
        buf.writeCString("15.4");
        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.ParameterStatus.class, msg);
        var ps = (BackendMessage.ParameterStatus) msg;
        assertEquals("server_version", ps.name());
        assertEquals("15.4", ps.value());
    }

    @Test
    void decodeBackendKeyData() throws IOException {
        var buf = new PgBuffer(16);
        buf.writeByte('K');
        buf.writeInt(12); // 4 + 4 + 4
        buf.writeInt(12345); // processId
        buf.writeInt(67890); // secretKey

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.BackendKeyData.class, msg);
        var kd = (BackendMessage.BackendKeyData) msg;
        assertEquals(12345, kd.processId());
        assertEquals(67890, kd.secretKey());
    }

    @Test
    void decodeReadyForQuery() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('Z');
        buf.writeInt(5); // 4 + 1
        buf.writeByte('I'); // Idle

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.ReadyForQuery.class, msg);
        assertEquals('I', ((BackendMessage.ReadyForQuery) msg).txStatus());
    }

    @Test
    void decodeReadyForQueryInTransaction() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('Z');
        buf.writeInt(5);
        buf.writeByte('T'); // In transaction

        var msg = decode(buf.toByteArray());
        assertEquals('T', ((BackendMessage.ReadyForQuery) msg).txStatus());
    }

    @Test
    void decodeReadyForQueryFailed() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('Z');
        buf.writeInt(5);
        buf.writeByte('E'); // Failed transaction

        var msg = decode(buf.toByteArray());
        assertEquals('E', ((BackendMessage.ReadyForQuery) msg).txStatus());
    }

    @Test
    void decodeRowDescription() throws IOException {
        var buf = new PgBuffer(128);
        buf.writeByte('T');
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder
        buf.writeShort(2); // 2 columns

        // Column 1: "id"
        buf.writeCString("id");
        buf.writeInt(16384); // tableOID
        buf.writeShort(1);   // column attribute number
        buf.writeInt(23);    // typeOID (int4)
        buf.writeShort(4);   // type size
        buf.writeInt(-1);    // type modifier
        buf.writeShort(0);   // text format

        // Column 2: "name"
        buf.writeCString("name");
        buf.writeInt(16384);
        buf.writeShort(2);
        buf.writeInt(25);    // typeOID (text)
        buf.writeShort(-1);
        buf.writeInt(-1);
        buf.writeShort(0);

        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.RowDescription.class, msg);
        var rd = (BackendMessage.RowDescription) msg;
        assertEquals(2, rd.columns().length);
        assertEquals("id", rd.columns()[0].name());
        assertEquals(23, rd.columns()[0].typeOID());
        assertEquals("name", rd.columns()[1].name());
        assertEquals(25, rd.columns()[1].typeOID());
    }

    @Test
    void decodeDataRow() throws IOException {
        var buf = new PgBuffer(64);
        buf.writeByte('D');
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder
        buf.writeShort(3); // 3 columns

        // Column 0: "42"
        byte[] val0 = "42".getBytes();
        buf.writeInt(val0.length);
        buf.writeBytes(val0);

        // Column 1: NULL
        buf.writeInt(-1);

        // Column 2: "hello"
        byte[] val2 = "hello".getBytes();
        buf.writeInt(val2.length);
        buf.writeBytes(val2);

        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.DataRow.class, msg);
        var dr = (BackendMessage.DataRow) msg;
        assertEquals(3, dr.values().length);
        assertArrayEquals("42".getBytes(), dr.values()[0]);
        assertNull(dr.values()[1]); // SQL NULL
        assertArrayEquals("hello".getBytes(), dr.values()[2]);
    }

    @Test
    void decodeCommandCompleteInsert() throws IOException {
        var msg = decodeCommandComplete("INSERT 0 5");
        assertEquals(5, ((BackendMessage.CommandComplete) msg).rowsAffected());
    }

    @Test
    void decodeCommandCompleteUpdate() throws IOException {
        var msg = decodeCommandComplete("UPDATE 3");
        assertEquals(3, ((BackendMessage.CommandComplete) msg).rowsAffected());
    }

    @Test
    void decodeCommandCompleteDelete() throws IOException {
        var msg = decodeCommandComplete("DELETE 1");
        assertEquals(1, ((BackendMessage.CommandComplete) msg).rowsAffected());
    }

    @Test
    void decodeCommandCompleteSelect() throws IOException {
        var msg = decodeCommandComplete("SELECT 10");
        assertEquals(10, ((BackendMessage.CommandComplete) msg).rowsAffected());
    }

    @Test
    void decodeCommandCompleteCreate() throws IOException {
        var msg = decodeCommandComplete("CREATE TABLE");
        assertEquals(0, ((BackendMessage.CommandComplete) msg).rowsAffected());
    }

    @Test
    void decodeErrorResponse() throws IOException {
        var buf = new PgBuffer(256);
        buf.writeByte('E');
        int pos = buf.writerIndex();
        buf.writeInt(0); // length placeholder

        buf.writeByte('S'); buf.writeCString("ERROR");
        buf.writeByte('C'); buf.writeCString("42P01");
        buf.writeByte('M'); buf.writeCString("relation \"foo\" does not exist");
        buf.writeByte('D'); buf.writeCString("some detail");
        buf.writeByte('H'); buf.writeCString("some hint");
        buf.writeByte(0); // terminator

        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.ErrorResponse.class, msg);
        var err = (BackendMessage.ErrorResponse) msg;
        assertEquals("ERROR", err.severity());
        assertEquals("42P01", err.code());
        assertEquals("relation \"foo\" does not exist", err.message());
        assertEquals("some detail", err.detail());
        assertEquals("some hint", err.hint());
    }

    @Test
    void decodeNoticeResponse() throws IOException {
        var buf = new PgBuffer(128);
        buf.writeByte('N');
        int pos = buf.writerIndex();
        buf.writeInt(0);

        buf.writeByte('S'); buf.writeCString("WARNING");
        buf.writeByte('C'); buf.writeCString("01000");
        buf.writeByte('M'); buf.writeCString("some warning");
        buf.writeByte(0);

        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.NoticeResponse.class, msg);
        var notice = (BackendMessage.NoticeResponse) msg;
        assertEquals("WARNING", notice.severity());
        assertEquals("01000", notice.code());
        assertEquals("some warning", notice.message());
    }

    @Test
    void decodeParseComplete() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('1');
        buf.writeInt(4);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.ParseComplete.class, msg);
    }

    @Test
    void decodeBindComplete() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('2');
        buf.writeInt(4);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.BindComplete.class, msg);
    }

    @Test
    void decodeNoData() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('n');
        buf.writeInt(4);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.NoData.class, msg);
    }

    @Test
    void decodeEmptyQueryResponse() throws IOException {
        var buf = new PgBuffer(8);
        buf.writeByte('I');
        buf.writeInt(4);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.EmptyQueryResponse.class, msg);
    }

    @Test
    void decodeParameterDescription() throws IOException {
        var buf = new PgBuffer(32);
        buf.writeByte('t');
        int pos = buf.writerIndex();
        buf.writeInt(0);
        buf.writeShort(2); // 2 params
        buf.writeInt(23);  // int4
        buf.writeInt(25);  // text
        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.ParameterDescription.class, msg);
        var pd = (BackendMessage.ParameterDescription) msg;
        assertArrayEquals(new int[]{23, 25}, pd.typeOIDs());
    }

    @Test
    void decodeNotificationResponse() throws IOException {
        var buf = new PgBuffer(64);
        buf.writeByte('A');
        int pos = buf.writerIndex();
        buf.writeInt(0);
        buf.writeInt(1234); // process ID
        buf.writeCString("my_channel");
        buf.writeCString("payload data");
        buf.setInt(pos, buf.writerIndex() - pos);

        var msg = decode(buf.toByteArray());
        assertInstanceOf(BackendMessage.NotificationResponse.class, msg);
        var nr = (BackendMessage.NotificationResponse) msg;
        assertEquals(1234, nr.processId());
        assertEquals("my_channel", nr.channel());
        assertEquals("payload data", nr.payload());
    }

    @Test
    void decodeMultipleMessages() throws IOException {
        // Build a stream with AuthOk + ParameterStatus + ReadyForQuery
        var buf = new PgBuffer(128);

        // AuthOk
        buf.writeByte('R'); buf.writeInt(8); buf.writeInt(0);

        // ParameterStatus
        buf.writeByte('S');
        int psPos = buf.writerIndex();
        buf.writeInt(0);
        buf.writeCString("client_encoding");
        buf.writeCString("UTF8");
        buf.setInt(psPos, buf.writerIndex() - psPos);

        // ReadyForQuery
        buf.writeByte('Z'); buf.writeInt(5); buf.writeByte('I');

        var decoder = new PgDecoder(new ByteArrayInputStream(buf.toByteArray()));

        var msg1 = decoder.readMessage();
        assertInstanceOf(BackendMessage.AuthenticationOk.class, msg1);

        var msg2 = decoder.readMessage();
        assertInstanceOf(BackendMessage.ParameterStatus.class, msg2);
        assertEquals("UTF8", ((BackendMessage.ParameterStatus) msg2).value());

        var msg3 = decoder.readMessage();
        assertInstanceOf(BackendMessage.ReadyForQuery.class, msg3);
    }

    // --- Helpers ---

    private BackendMessage decode(byte[] bytes) throws IOException {
        return new PgDecoder(new ByteArrayInputStream(bytes)).readMessage();
    }

    private BackendMessage decodeCommandComplete(String tag) throws IOException {
        var buf = new PgBuffer(64);
        buf.writeByte('C');
        int pos = buf.writerIndex();
        buf.writeInt(0);
        buf.writeCString(tag);
        buf.setInt(pos, buf.writerIndex() - pos);
        return decode(buf.toByteArray());
    }

    private byte[] authOkMessage() {
        var buf = new PgBuffer(16);
        buf.writeByte('R');
        buf.writeInt(8); // length: 4 + 4
        buf.writeInt(0); // OK
        return buf.toByteArray();
    }
}
