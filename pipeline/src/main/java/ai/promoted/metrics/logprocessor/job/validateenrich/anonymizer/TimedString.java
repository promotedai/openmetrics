package ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer;

import com.google.auto.value.AutoValue;

/** A tuple of a start time and value. Used to time range values. */
@AutoValue
public abstract class TimedString {
  public abstract String value();

  public abstract long startMillis();

  static TimedString create(String value, long startMillis) {
    return new AutoValue_TimedString(value, startMillis);
  }
}
