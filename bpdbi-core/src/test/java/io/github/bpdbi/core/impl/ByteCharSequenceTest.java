package io.github.bpdbi.core.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Unit tests for ByteCharSequence, including multi-byte UTF-8 paths. */
class ByteCharSequenceTest {

  private ByteCharSequence of(String s) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    return new ByteCharSequence(bytes, 0, bytes.length);
  }

  // ===== ASCII path =====

  @Test
  void asciiLength() {
    assertEquals(5, of("hello").length());
  }

  @Test
  void asciiCharAt() {
    var seq = of("abc");
    assertEquals('a', seq.charAt(0));
    assertEquals('c', seq.charAt(2));
  }

  @Test
  void asciiCharAtOutOfBounds() {
    var seq = of("ab");
    assertThrows(IndexOutOfBoundsException.class, () -> seq.charAt(2));
    assertThrows(IndexOutOfBoundsException.class, () -> seq.charAt(-1));
  }

  @Test
  void asciiSubSequence() {
    var seq = of("hello");
    assertEquals("ell", seq.subSequence(1, 4).toString());
  }

  @Test
  void asciiToString() {
    assertEquals("hello", of("hello").toString());
  }

  // ===== Multi-byte UTF-8 path =====

  @Test
  void multiByteLengthCountsCharsNotBytes() {
    // \u00E9 (é) is 2 bytes in UTF-8, but 1 char in Java
    var seq = of("caf\u00E9");
    assertEquals(4, seq.length()); // 4 characters, not 5 bytes
  }

  @Test
  void multiByteCharAt() {
    var seq = of("caf\u00E9");
    assertEquals('c', seq.charAt(0));
    assertEquals('\u00E9', seq.charAt(3));
  }

  @Test
  void multiByteSubSequence() {
    var seq = of("caf\u00E9!");
    assertEquals("f\u00E9", seq.subSequence(2, 4).toString());
  }

  @Test
  void emojiLength() {
    // 🎉 is U+1F389, a supplementary character: 4 bytes in UTF-8, 2 chars in Java (surrogate pair)
    var seq = of("\uD83C\uDF89");
    assertEquals(2, seq.length()); // 2 Java chars (surrogate pair)
  }

  @Test
  void emojiToString() {
    assertEquals("\uD83C\uDF89 party", of("\uD83C\uDF89 party").toString());
  }

  @Test
  void chineseCharacters() {
    var seq = of("\u4F60\u597D"); // 你好
    assertEquals(2, seq.length());
    assertEquals('\u4F60', seq.charAt(0));
    assertEquals('\u597D', seq.charAt(1));
  }

  // ===== Offset constructor =====

  @Test
  void offsetSubarray() {
    byte[] buf = "XXhelloYY".getBytes(StandardCharsets.UTF_8);
    var seq = new ByteCharSequence(buf, 2, 5);
    assertEquals(5, seq.length());
    assertEquals("hello", seq.toString());
  }

  @Test
  void offsetWithMultiByte() {
    byte[] full = "A\u00E9B".getBytes(StandardCharsets.UTF_8); // A(1) é(2) B(1) = 4 bytes
    // Extract just "é" starting at offset 1, length 2
    var seq = new ByteCharSequence(full, 1, 2);
    assertEquals("\u00E9", seq.toString());
    assertEquals(1, seq.length());
  }

  // ===== equals / hashCode =====

  @Test
  void equalsString() {
    assertTrue(of("hello").equals("hello"));
  }

  @Test
  void equalsStringMultiByte() {
    assertTrue(of("caf\u00E9").equals("caf\u00E9"));
  }

  @Test
  void equalsSelf() {
    var seq = of("hello");
    assertTrue(seq.equals(seq));
  }

  @Test
  void equalsOtherByteCharSequenceAscii() {
    assertTrue(of("hello").equals(of("hello")));
  }

  @Test
  void equalsOtherByteCharSequenceMultiByte() {
    assertTrue(of("\u4F60\u597D").equals(of("\u4F60\u597D")));
  }

  @Test
  void notEqualsDifferentLength() {
    assertFalse(of("ab").equals(of("abc")));
  }

  @Test
  void notEqualsDifferentContent() {
    assertFalse(of("abc").equals(of("abd")));
  }

  @Test
  void notEqualsNull() {
    assertFalse(of("hello").equals(null));
  }

  @Test
  void notEqualsOtherType() {
    assertFalse(of("hello").equals(42));
  }

  @Test
  void hashCodeConsistentWithString() {
    assertEquals("hello".hashCode(), of("hello").hashCode());
    assertEquals("caf\u00E9".hashCode(), of("caf\u00E9").hashCode());
  }

  @Test
  void hashCodeDifferentValues() {
    assertNotEquals(of("abc").hashCode(), of("xyz").hashCode());
  }

  // ===== toString caching =====

  @Test
  void toStringIsCached() {
    var seq = of("test");
    String s1 = seq.toString();
    String s2 = seq.toString();
    // Same instance (not just equal), proving caching
    assertTrue(s1 == s2);
  }
}
