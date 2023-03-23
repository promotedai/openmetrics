package ai.promoted.metrics.logprocessor.common.records;

import java.util.Objects;

public class OptionalUtils {

  /**
   * @return if {@code value} is an empty string, return null
   */
  public static String toNullable(String value) {
    return !Objects.equals("", value) ? value : null;
  }

  /**
   * @return if {@code value} is 0L, return null
   */
  public static Long toNullable(long value) {
    return value != 0L ? value : null;
  }

  /**
   * @return if {@code value} is 0, return null
   */
  public static Integer toNullable(int value) {
    return value != 0 ? value : null;
  }

  /**
   * @return if {@code value} is 0.0, return null
   */
  public static Double toNullable(double value) {
    return value != 0.0 ? value : null;
  }
}
