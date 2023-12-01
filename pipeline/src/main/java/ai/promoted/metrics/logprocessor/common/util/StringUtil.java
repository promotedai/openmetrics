package ai.promoted.metrics.logprocessor.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

/** Utilities for Strings. */
public final class StringUtil {
  private static final XXHash64 XXHASH = XXHashFactory.fastestInstance().hash64();
  private static final long EMPTY_XXHASH_VALUE = XXHASH.hash(ByteBuffer.allocate(0), 0);

  // Inlining a string followed by a replace causes the replace to not work. I don't know why.
  public static String replace(String source, Map<String, String> replacementMap) {
    for (Map.Entry<String, String> entry : replacementMap.entrySet()) {
      source = source.replace("{" + entry.getKey() + "}", entry.getValue());
    }
    return source;
  }

  public static boolean isBlank(String in) {
    return in == null || in.isBlank();
  }

  public static boolean isNonEmptyAndEqual(String a, String b) {
    return a != null && b != null && !isBlank(a) && !isBlank(b) && a.equals(b);
  }

  public static String firstNotEmpty(String... values) {
    for (int i = 0; i < values.length - 1; i++) {
      String value = values[i];
      if (!value.isEmpty()) {
        return value;
      }
    }
    return values[values.length - 1];
  }

  public static String firstNotNull(String... values) {
    for (String value : values) {
      if (null != value) {
        return value;
      }
    }
    return null;
  }

  /**
   * Gets the xxhash64 value for a string.
   *
   * <p>This is not compatible with our go-hashlib or java hashCode implementation. It MUST be
   * compatible with cespare/xxhash's Sum64String golang impl.
   */
  public static long xxhash(String in) {
    return in != null ? xxhash(in.getBytes(StandardCharsets.UTF_8)) : EMPTY_XXHASH_VALUE;
  }

  /** Gets the hex xxhash64 value for a string. */
  public static String xxhashHex(String in) {
    return Long.toHexString(xxhash(in));
  }

  /** Gets the xxhash64 value for a byte array. */
  public static long xxhash(byte[] in) {
    return XXHASH.hash(in, 0, in.length, 0);
  }

  /** Gets the hex xxhash64 value for a byte array. */
  public static String xxhashHex(byte[] in) {
    return Long.toHexString(xxhash(in));
  }

  /** Gets the xxhash64 value for a byte buffer. */
  public static long xxhash(ByteBuffer in) {
    return XXHASH.hash(in, 0);
  }

  /** Gets the hex xxhash64 value for a byte buffer. */
  public static String xxhashHex(ByteBuffer in) {
    return Long.toHexString(xxhash(in));
  }

  /**
   * Returns a hash for {@code value}. This method has a test to make sure the hashcode impl doesn't
   * change out from under us in future Java versions. This is not the same hashing used for our
   * Feature ID hashing.
   */
  public static int hash(String value) {
    return value.hashCode();
  }

  private StringUtil() {}
  ;
}
