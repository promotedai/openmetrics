package ai.promoted.metrics.logprocessor.job.validateenrich;

import com.google.auto.value.AutoValue;
import java.io.Serializable;

@AutoValue
public abstract class TimeRange implements Serializable {
  public abstract long startTimeInclusive();

  public abstract long endTimeExclusive();

  static TimeRange create(long startTimeInclusive, long endTimeExclusive) {
    return new AutoValue_TimeRange(startTimeInclusive, endTimeExclusive);
  }
}
