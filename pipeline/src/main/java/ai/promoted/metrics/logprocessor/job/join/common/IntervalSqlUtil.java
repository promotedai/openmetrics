package ai.promoted.metrics.logprocessor.job.join.common;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class IntervalSqlUtil {

  /**
   * Returns a Flink SQL "INTERVAL ..." to be used in the interval joins
   *
   * <p>Calcite has issues with converting milli durations to INTERVAL.
   */
  public static String fromDurationToSQLInterval(Duration duration) {
    Preconditions.checkArgument(
        !duration.isNegative(), "fromDurationToSQLInterval only supports non-negative durations");

    Preconditions.checkArgument(
        duration.toMillis() % 1000L == 0L,
        "fromDurationToSQLInterval only supports durations in seconds (not millis)");

    long days = duration.toDays();
    // Get remainders.
    long hours = duration.toHours() % 24;
    long minutes = duration.toMinutes() % 60;
    long seconds = duration.getSeconds() % 60;

    List<String> parts = new ArrayList<>(4);
    if (days > 0) {
      int digits = countDigits(days);
      Preconditions.checkArgument(
          digits <= 3,
          "This function does not support day durations with more than 3 digits, days=%s",
          days);
      parts.add(String.format("INTERVAL '%d' DAY(%d)", days, digits));
    }
    if (hours > 0) {
      parts.add(String.format("INTERVAL '%d' HOUR", hours));
    }
    if (minutes > 0) {
      parts.add(String.format("INTERVAL '%d' MINUTE", minutes));
    }
    // TODO - improve the Flink SQL when duration is zero.
    if (seconds > 0 || parts.isEmpty()) {
      parts.add(String.format("INTERVAL '%d' SECOND", seconds));
    }

    return "(" + Joiner.on(" + ").join(parts) + ")";
  }

  private static int countDigits(long number) {
    if (number == 0) return 1;
    int count = 0;
    while (number != 0) {
      number /= 10;
      count++;
    }
    return count;
  }
}
