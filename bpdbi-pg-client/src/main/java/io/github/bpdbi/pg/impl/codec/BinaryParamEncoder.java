package io.github.bpdbi.pg.impl.codec;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Encodes Java objects directly into Postgres binary wire format. For supported types, this avoids
 * the intermediate String allocation that text encoding requires.
 *
 * <p>Unsupported types (including String) fall back to text encoding via BinderRegistry.
 */
public final class BinaryParamEncoder {

  private BinaryParamEncoder() {}

  /**
   * Returns the Postgres type OID for a Java object, or 0 if the type is not supported for binary
   * encoding (will fall back to text).
   */
  public static int typeOID(@Nullable Object value) {
    return switch (value) {
      case Integer ignored -> PgOIDs.INT4;
      case Long ignored -> PgOIDs.INT8;
      case Short ignored -> PgOIDs.INT2;
      case Float ignored -> PgOIDs.FLOAT4;
      case Double ignored -> PgOIDs.FLOAT8;
      case Boolean ignored -> PgOIDs.BOOL;
      case byte[] ignored -> PgOIDs.BYTEA;
      case UUID ignored -> PgOIDs.UUID;
      case null, default -> 0;
    };
  }

  /** Returns true if the given object can be binary-encoded without text fallback. */
  public static boolean canEncodeBinary(@Nullable Object value) {
    return typeOID(value) != 0;
  }

  /**
   * Write a single parameter value in binary format directly into the buffer. Writes the 4-byte
   * length prefix followed by the binary value. For null values, writes -1 (no value bytes).
   *
   * @return true if binary encoding was used, false if the caller should use text fallback
   */
  public static boolean writeParam(@Nullable Object value, ByteBuffer buf) {
    switch (value) {
      case null -> {
        buf.writeInt(-1);
        return true;
      }
      case Integer v -> {
        buf.writeInt(4);
        buf.writeInt(v);
        return true;
      }
      case Long v -> {
        buf.writeInt(8);
        buf.writeLong(v);
        return true;
      }
      case Short v -> {
        buf.writeInt(2);
        buf.writeShort(v);
        return true;
      }
      case Float v -> {
        buf.writeInt(4);
        buf.writeFloat(v);
        return true;
      }
      case Double v -> {
        buf.writeInt(8);
        buf.writeDouble(v);
        return true;
      }
      case Boolean v -> {
        buf.writeInt(1);
        buf.writeByte(v ? 1 : 0);
        return true;
      }
      case byte[] v -> {
        buf.writeInt(v.length);
        buf.writeBytes(v);
        return true;
      }
      case UUID v -> {
        buf.writeInt(16);
        buf.writeLong(v.getMostSignificantBits());
        buf.writeLong(v.getLeastSignificantBits());
        return true;
      }
      default -> {}
    }
    return false; // caller should use text fallback
  }
}
