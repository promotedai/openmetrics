package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

/** Sliding counter that counts T's for an hour over 15m windows. */
public class SlidingHourlyCounter<KEY, T> extends SlidingCounter<KEY, T> {
  public SlidingHourlyCounter(
      TypeInformation<KEY> keyTypeInfo,
      Duration windowSlide,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    this(false, keyTypeInfo, windowSlide, eventTimeGetter, countGetter, sideOutputDebugLogging);
  }

  public SlidingHourlyCounter(
      boolean queryableStateEnabled,
      TypeInformation<KEY> keyTypeInfo,
      Duration windowSlide,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    // Hourly counts need to be "fresh", but using too small of a bucket would cause much more
    // state and write overhead.  1hr by 15m windows results in at most 4 concurrent window
    // states to track and 15m of staleness in the worst-case.
    super(
        queryableStateEnabled,
        ChronoUnit.HOURS,
        ImmutableList.of(1),
        keyTypeInfo,
        windowSlide,
        eventTimeGetter,
        countGetter,
        sideOutputDebugLogging);
    int minutes = (int) windowSlide.toMinutes();
    Preconditions.checkArgument(
        minutes >= 1 && minutes <= 60,
        "windowSlide should be between 1 and 60 minutes; emitWindowMinutes=%s",
        minutes);
    Preconditions.checkArgument(
        Duration.ofHours(1).toMillis() % windowSlide.toMillis() == 0,
        "60 minutes divided by windowSlide should have no remainder.  windowSlide should create even buckets; windowSlide",
        windowSlide);
  }
}
