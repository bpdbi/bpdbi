package io.github.bpdbi.core;

import static io.github.bpdbi.core.test.TestRows.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.bpdbi.core.spi.RowExtractors;
import io.github.bpdbi.core.test.StubBinaryCodec;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Unit tests for RowExtractors. No database required. */
class RowExtractorsTest {

  private Row textRow(String value) {
    ColumnDescriptor[] cols = {col("v")};
    byte[][] vals = {value == null ? null : value.getBytes(StandardCharsets.UTF_8)};
    return new Row(cols, vals, StubBinaryCodec.INSTANCE, new TypeRegistry());
  }

  @Test
  void extractorForString() {
    var extractor = RowExtractors.extractorFor(String.class);
    assertNotNull(extractor);
    assertEquals("hello", extractor.apply(textRow("hello"), 0));
  }

  @Test
  void extractorForPrimitiveIntReturnsZeroForNull() {
    var extractor = RowExtractors.extractorFor(int.class);
    assertNotNull(extractor);
    assertEquals(0, extractor.apply(textRow(null), 0));
  }

  @Test
  void extractorForBoxedIntegerReturnsNull() {
    var extractor = RowExtractors.extractorFor(Integer.class);
    assertNotNull(extractor);
    assertNull(extractor.apply(textRow(null), 0));
  }

  @Test
  void extractorForPrimitiveBooleanReturnsFalseForNull() {
    var extractor = RowExtractors.extractorFor(boolean.class);
    assertNotNull(extractor);
    assertEquals(false, extractor.apply(textRow(null), 0));
  }

  enum TestColor {
    RED,
    GREEN,
    BLUE
  }

  @Test
  void extractorForEnumType() {
    var extractor = RowExtractors.extractorFor(TestColor.class);
    assertNotNull(extractor);
    assertEquals(TestColor.GREEN, extractor.apply(textRow("GREEN"), 0));
  }

  @Test
  void extractorForEnumReturnsNullForNullString() {
    var extractor = RowExtractors.extractorFor(TestColor.class);
    assertNotNull(extractor);
    assertNull(extractor.apply(textRow(null), 0));
  }

  @Test
  void extractorForUnsupportedTypeReturnsNull() {
    assertNull(RowExtractors.extractorFor(RowExtractorsTest.class));
  }
}
