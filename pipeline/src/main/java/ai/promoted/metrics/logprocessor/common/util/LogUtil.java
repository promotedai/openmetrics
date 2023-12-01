package ai.promoted.metrics.logprocessor.common.util;

import com.google.common.annotations.VisibleForTesting;

public class LogUtil {
  // Text log systems cannot support super long strings.  Just truncate.
  private static final int MAX_LENGTH = 2000;

  public static String truncate(String value) {
    return truncate(value, MAX_LENGTH);
  }

  @VisibleForTesting
  static String truncate(String value, int maxLength) {
    if (value.length() > maxLength) {
      return value.substring(0, maxLength) + " (TRUNCATED)";
    } else {
      return value;
    }
  }
}
