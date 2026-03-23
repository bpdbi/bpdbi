package io.github.bpdbi.pg.impl.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpdbi.core.ColumnDescriptor;
import org.junit.jupiter.api.Test;

/** Verifies that OID constants in ColumnDescriptor match PgOIDs to prevent silent divergence. */
class OIDConsistencyTest {

  @Test
  void jsonOIDsMatch() {
    // ColumnDescriptor uses these for isJsonType()
    assertEquals(PgOIDs.JSON, ColumnDescriptor.OID_JSON);
    assertEquals(PgOIDs.JSONB, ColumnDescriptor.OID_JSONB);
  }

  @Test
  void textLikeOIDsMatchIsTextLikeType() {
    // ColumnDescriptor's text-like OIDs are private, so verify via isTextLikeType()
    assertTrue(new ColumnDescriptor("c", 0, (short) 0, PgOIDs.TEXT, (short) 0, 0).isTextLikeType());
    assertTrue(
        new ColumnDescriptor("c", 0, (short) 0, PgOIDs.VARCHAR, (short) 0, 0).isTextLikeType());
    assertTrue(
        new ColumnDescriptor("c", 0, (short) 0, PgOIDs.BPCHAR, (short) 0, 0).isTextLikeType());
    assertTrue(new ColumnDescriptor("c", 0, (short) 0, PgOIDs.CHAR, (short) 0, 0).isTextLikeType());
    assertTrue(new ColumnDescriptor("c", 0, (short) 0, PgOIDs.NAME, (short) 0, 0).isTextLikeType());
    assertTrue(new ColumnDescriptor("c", 0, (short) 0, PgOIDs.XML, (short) 0, 0).isTextLikeType());
  }
}
