package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a Flat string value to AnonymizerConfig.
 *
 * <p>Format: "implementation{,options}|<timevaluesmap>"
 *
 * <p>For AWS values
 *
 * <ul>
 *   <li>"prm-dev-2023-10-01"
 *   <li>"prm-dev-2023-01-01=2023-01-01,prm-dev-2023-10-01=2023-10-01"
 * </ul>
 */
public class TimedStringMapConverter {
  private static final Splitter commaSplitter = Splitter.on(",");
  private static final Splitter equalSplitter = Splitter.on("=");

  public static List<TimedString> convert(String value) {
    Preconditions.checkArgument(!value.isEmpty(), "TimedString list must not be empty");
    return commaSplitter
        .splitToStream(value)
        .map(TimedStringMapConverter::convertTimeValue)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public static TimedString convertTimeValue(String value) {
    List<String> parts = equalSplitter.splitToList(value);
    switch (parts.size()) {
      case 1:
        return TimedString.create(parts.get(0), 0L);
      case 2:
        return TimedString.create(parts.get(0), parseDate(parts.get(1)).toEpochMilli());
      default:
        throw new IllegalArgumentException("The TimedString should only have one = sign");
    }
  }

  private static Instant parseDate(String timeString) {
    try {
      return LocalDate.parse(timeString).atStartOfDay(ZoneOffset.UTC).toInstant();
    } catch (DateTimeParseException e) {
      return Instant.parse(timeString);
    }
  }
}
