package io.github.bpdbi.pg.impl.codec;

import io.github.bpdbi.core.impl.ByteBuffer;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Encodes Java objects directly into Postgres binary wire format. For supported types, this avoids
 * the intermediate String allocation that text encoding requires.
 *
 * <p>Unsupported types (including String) fall back to text encoding.
 */
public final class BinaryParamEncoder {

  private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
  private static final LocalDateTime PG_EPOCH_DATETIME = PG_EPOCH_DATE.atStartOfDay();

  private BinaryParamEncoder() {}

  /**
   * Returns the Postgres type OID for a Java object, or 0 if the type is not supported for binary
   * encoding (will fall back to text).
   */
  public static int typeOID(@Nullable Object value) {
    return switch (value) {
      case null -> 0;
      case Integer ignored -> PgOIDs.INT4;
      case Long ignored -> PgOIDs.INT8;
      case Short ignored -> PgOIDs.INT2;
      case Float ignored -> PgOIDs.FLOAT4;
      case Double ignored -> PgOIDs.FLOAT8;
      case Boolean ignored -> PgOIDs.BOOL;
      case byte[] ignored -> PgOIDs.BYTEA;
      case UUID ignored -> PgOIDs.UUID;
      case LocalDate ignored -> PgOIDs.DATE;
      case LocalTime ignored -> PgOIDs.TIME;
      case LocalDateTime ignored -> PgOIDs.TIMESTAMP;
      case OffsetDateTime ignored -> PgOIDs.TIMESTAMPTZ;
      case Instant ignored -> PgOIDs.TIMESTAMPTZ;
      case BigDecimal ignored -> PgOIDs.NUMERIC;
      case OffsetTime ignored -> PgOIDs.TIMETZ;
      case int[] ignored -> PgOIDs.INT4_ARRAY;
      case long[] ignored -> PgOIDs.INT8_ARRAY;
      case short[] ignored -> PgOIDs.INT2_ARRAY;
      case float[] ignored -> PgOIDs.FLOAT4_ARRAY;
      case double[] ignored -> PgOIDs.FLOAT8_ARRAY;
      case boolean[] ignored -> PgOIDs.BOOL_ARRAY;
      case Collection<?> c -> collectionArrayOID(c);
      case Object[] a -> objectArrayOID(a);
      default -> 0;
    };
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
      case LocalDate v -> {
        buf.writeInt(4);
        writeDate(v, buf);
        return true;
      }
      case LocalTime v -> {
        buf.writeInt(8);
        buf.writeLong(v.getLong(ChronoField.MICRO_OF_DAY));
        return true;
      }
      case LocalDateTime v -> {
        buf.writeInt(8);
        writeTimestamp(v, buf);
        return true;
      }
      case OffsetDateTime v -> {
        buf.writeInt(8);
        writeTimestamptz(v, buf);
        return true;
      }
      case Instant v -> {
        buf.writeInt(8);
        writeTimestamp(v.atOffset(ZoneOffset.UTC).toLocalDateTime(), buf);
        return true;
      }
      case BigDecimal v -> {
        PgBinaryCodec.encodeNumeric(v, buf);
        return true;
      }
      case OffsetTime v -> {
        buf.writeInt(12);
        buf.writeLong(v.toLocalTime().getLong(ChronoField.MICRO_OF_DAY));
        buf.writeInt(-v.getOffset().getTotalSeconds());
        return true;
      }
      case int[] a -> {
        writeInt4Array(a, buf);
        return true;
      }
      case long[] a -> {
        writeInt8Array(a, buf);
        return true;
      }
      case short[] a -> {
        writeInt2Array(a, buf);
        return true;
      }
      case float[] a -> {
        writeFloat4Array(a, buf);
        return true;
      }
      case double[] a -> {
        writeFloat8Array(a, buf);
        return true;
      }
      case boolean[] a -> {
        writeBoolArray(a, buf);
        return true;
      }
      case Collection<?> c -> {
        return writeObjectArrayValues(c.toArray(), buf);
      }
      case Object[] a -> {
        return writeObjectArrayValues(a, buf);
      }
      default -> {}
    }
    return false;
  }

  // --- Date/time helpers ---

  private static void writeDate(LocalDate date, ByteBuffer buf) {
    if (date == LocalDate.MAX) {
      buf.writeInt(Integer.MAX_VALUE);
    } else if (date == LocalDate.MIN) {
      buf.writeInt(Integer.MIN_VALUE);
    } else {
      buf.writeInt((int) ChronoUnit.DAYS.between(PG_EPOCH_DATE, date));
    }
  }

  private static void writeTimestamp(LocalDateTime ts, ByteBuffer buf) {
    buf.writeLong(ChronoUnit.MICROS.between(PG_EPOCH_DATETIME, ts));
  }

  private static void writeTimestamptz(OffsetDateTime ts, ByteBuffer buf) {
    LocalDateTime utc =
        ts.getOffset().equals(ZoneOffset.UTC)
            ? ts.toLocalDateTime()
            : ts.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
    writeTimestamp(utc, buf);
  }

  // --- Primitive array encoding ---

  /**
   * Binary array wire format: ndim(4) + flags(4) + elemOID(4) + dim_size(4) + dim_lower(4) +
   * elements (each: length(4) + data).
   */
  private static void writeArrayHeader(ByteBuffer buf, int elemOID, int count) {
    buf.writeInt(1); // ndim = 1
    buf.writeInt(0); // flags (no nulls for primitive arrays)
    buf.writeInt(elemOID);
    buf.writeInt(count); // dimension size
    buf.writeInt(1); // lower bound (1-based)
  }

  private static void writeInt4Array(int[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0); // total length placeholder
    writeArrayHeader(buf, PgOIDs.INT4, a.length);
    for (int v : a) {
      buf.writeInt(4);
      buf.writeInt(v);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  private static void writeInt8Array(long[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0);
    writeArrayHeader(buf, PgOIDs.INT8, a.length);
    for (long v : a) {
      buf.writeInt(8);
      buf.writeLong(v);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  private static void writeInt2Array(short[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0);
    writeArrayHeader(buf, PgOIDs.INT2, a.length);
    for (short v : a) {
      buf.writeInt(2);
      buf.writeShort(v);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  private static void writeFloat4Array(float[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0);
    writeArrayHeader(buf, PgOIDs.FLOAT4, a.length);
    for (float v : a) {
      buf.writeInt(4);
      buf.writeFloat(v);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  private static void writeFloat8Array(double[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0);
    writeArrayHeader(buf, PgOIDs.FLOAT8, a.length);
    for (double v : a) {
      buf.writeInt(8);
      buf.writeDouble(v);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  private static void writeBoolArray(boolean[] a, ByteBuffer buf) {
    int lenPos = buf.writerIndex();
    buf.writeInt(0);
    writeArrayHeader(buf, PgOIDs.BOOL, a.length);
    for (boolean v : a) {
      buf.writeInt(1);
      buf.writeByte(v ? 1 : 0);
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
  }

  // --- Object array / Collection encoding ---

  /**
   * Returns the scalar OID for an element inside an array. Unlike {@link #typeOID}, this also
   * returns TEXT for String since text[] arrays are valid.
   */
  private static int elementOID(Object value) {
    if (value instanceof String) return PgOIDs.TEXT;
    return typeOID(value);
  }

  private static int collectionArrayOID(Collection<?> c) {
    for (Object e : c) {
      if (e != null) return arrayOIDFor(elementOID(e));
    }
    return 0;
  }

  private static int objectArrayOID(Object[] a) {
    for (Object e : a) {
      if (e != null) return arrayOIDFor(elementOID(e));
    }
    return 0;
  }

  private static int arrayOIDFor(int elemOID) {
    return switch (elemOID) {
      case PgOIDs.BOOL -> PgOIDs.BOOL_ARRAY;
      case PgOIDs.BYTEA -> PgOIDs.BYTEA_ARRAY;
      case PgOIDs.INT2 -> PgOIDs.INT2_ARRAY;
      case PgOIDs.INT4 -> PgOIDs.INT4_ARRAY;
      case PgOIDs.INT8 -> PgOIDs.INT8_ARRAY;
      case PgOIDs.FLOAT4 -> PgOIDs.FLOAT4_ARRAY;
      case PgOIDs.FLOAT8 -> PgOIDs.FLOAT8_ARRAY;
      case PgOIDs.TEXT -> PgOIDs.TEXT_ARRAY;
      case PgOIDs.UUID -> PgOIDs.UUID_ARRAY;
      case PgOIDs.DATE -> PgOIDs.DATE_ARRAY;
      case PgOIDs.TIME -> PgOIDs.TIME_ARRAY;
      case PgOIDs.TIMESTAMP -> PgOIDs.TIMESTAMP_ARRAY;
      case PgOIDs.TIMESTAMPTZ -> PgOIDs.TIMESTAMPTZ_ARRAY;
      case PgOIDs.NUMERIC -> PgOIDs.NUMERIC_ARRAY;
      case PgOIDs.TIMETZ -> PgOIDs.TIMETZ_ARRAY;
      default -> 0;
    };
  }

  /**
   * Write a binary-encoded array from Object[]. Returns false if the element type is not supported
   * for binary encoding (caller should fall back to text).
   */
  private static boolean writeObjectArrayValues(Object[] elements, ByteBuffer buf) {
    // Find element OID from first non-null element
    int elemOID = 0;
    boolean hasNulls = false;
    for (Object e : elements) {
      if (e == null) {
        hasNulls = true;
      } else if (elemOID == 0) {
        elemOID = elementOID(e);
      }
    }
    if (elemOID == 0 && !hasNulls) {
      // Empty array with no type info — fall back to text
      return elements.length == 0 && writeEmptyArray(buf);
    }
    if (elemOID == 0) {
      return false; // all nulls, no type info
    }

    int lenPos = buf.writerIndex();
    buf.writeInt(0); // total length placeholder
    buf.writeInt(1); // ndim = 1
    buf.writeInt(hasNulls ? 1 : 0);
    buf.writeInt(elemOID);
    buf.writeInt(elements.length);
    buf.writeInt(1); // lower bound

    for (Object e : elements) {
      if (e == null) {
        buf.writeInt(-1);
      } else if (e instanceof String s) {
        int strLenPos = buf.writerIndex();
        buf.writeInt(0);
        int written = buf.writeStringUtf8(s);
        buf.setInt(strLenPos, written);
      } else if (!writeParam(e, buf)) {
        // Unsupported element type — can't complete this array.
        // In practice this shouldn't happen since elementOID checks first.
        return false;
      }
    }
    buf.setInt(lenPos, buf.writerIndex() - lenPos - 4);
    return true;
  }

  /**
   * Write an empty array. Uses INT4 as element type since the array is empty and the type doesn't
   * matter.
   */
  private static boolean writeEmptyArray(ByteBuffer buf) {
    buf.writeInt(12); // 12 bytes: ndim(4) + flags(4) + elemOID(4)
    buf.writeInt(0); // ndim = 0 (empty)
    buf.writeInt(0); // flags
    buf.writeInt(PgOIDs.INT4); // element OID (arbitrary for empty)
    return true;
  }
}
