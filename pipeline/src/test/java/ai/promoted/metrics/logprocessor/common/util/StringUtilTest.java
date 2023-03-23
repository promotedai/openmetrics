package ai.promoted.metrics.logprocessor.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class StringUtilTest {

  @ParameterizedTest(name = "StringUtil.replace({0}) = {1}")
  @CsvSource({
    "\"apple broccoli\",    \"apple broccoli\"",
    "\"{apple} broccoli\",    \"apricot broccoli\"",
    "\"apple {broccoli}\",    \"apple banana\"",
    "\"peach croissant broccoli apple\",    \"peach croissant broccoli apple\"",
    "\"  {apple} {broccoli} {croissant} {peach} \",    \"  apricot banana chocolate pineapple \"",
    "\"{peach} {croissant} {broccoli} {apple}\",    \"pineapple chocolate banana apricot\"",
  })
  public void replace(String input, String expected) throws Exception {
    assertEquals(
        expected,
        StringUtil.replace(
            input,
            ImmutableMap.of(
                "apple", "apricot",
                "broccoli", "banana",
                "croissant", "chocolate",
                "peach", "pineapple")),
        () -> String.format("StringUtil.replace(\"%s\") should equal \"%s\"", input, expected));
  }

  @Test
  public void firstNotEmpty() {
    assertEquals("a", StringUtil.firstNotEmpty("a", "b", "c"));
    assertEquals("b", StringUtil.firstNotEmpty("", "b", "c"));
    assertEquals("c", StringUtil.firstNotEmpty("", "", "c"));
    assertEquals("", StringUtil.firstNotEmpty("", "", ""));
  }

  @Test
  public void isBlank() {
    assertTrue(StringUtil.isBlank(null));
    assertTrue(StringUtil.isBlank(""));
    assertTrue(StringUtil.isBlank("   "));
    assertFalse(StringUtil.isBlank("a   "));
    assertFalse(StringUtil.isBlank("   a"));
  }

  // Get the test cases from cespare/xxhash/blob/main/xxhash_test.go.
  @ParameterizedTest
  @CsvSource({
    ", ef46db3751d8e999",
    "'', ef46db3751d8e999",
    "a, d24ec4f1a98c6e5b",
    "as, 1c330fb2d66be179",
    "asd, 631c37ce72a97393",
    "asdf, 415872f599cea71e",
    "shoes, d5654a15f9b5efb1",
    "prom dresses, 2633a713effe6bda",
    "Call me Ishmael. Some years ago--never mind how long precisely-, 02a2e85470d6fd96"
  })
  public void getXxhash(String in, String expectedHex) {
    assertEquals(Long.parseUnsignedLong(expectedHex, 16), StringUtil.xxhash(in));
    if (in != null) {
      assertEquals(
          Long.parseUnsignedLong(expectedHex, 16),
          StringUtil.xxhash(ByteBuffer.wrap(in.getBytes())));
    }
  }

  @ParameterizedTest
  @CsvSource({
    ", ef46db3751d8e999",
    "'', ef46db3751d8e999",
    "a, d24ec4f1a98c6e5b",
    "as, 1c330fb2d66be179",
    "asd, 631c37ce72a97393",
    "asdf, 415872f599cea71e",
    "shoes, d5654a15f9b5efb1",
    "prom dresses, 2633a713effe6bda",
    // Note the missing leading 0.
    "Call me Ishmael. Some years ago--never mind how long precisely-, 2a2e85470d6fd96"
  })
  public void getXxhashHex(String in, String expectedHex) {
    assertEquals(expectedHex, StringUtil.xxhashHex(in));
    if (in != null) {
      assertEquals(expectedHex, StringUtil.xxhashHex(ByteBuffer.wrap(in.getBytes())));
    }
  }

  @Test
  public void hashCode_JavaCompatibility() {
    assertEquals(java8HashCode(""), StringUtil.hash(""));
    assertEquals(java8HashCode("landId"), StringUtil.hash("landId"));
    assertNotEquals(java8HashCode(""), StringUtil.hash("landId"));
    assertEquals(StringUtil.hash(new String("foo")), StringUtil.hash(new String("foo")));
  }

  // The java8 implementation of hash code.  Just being paranoid.
  public int java8HashCode(String value) {
    int h = 0;
    for (int i = 0; i < value.length(); i++) {
      h = 31 * h + value.charAt(i);
    }
    return h;
  }
}
