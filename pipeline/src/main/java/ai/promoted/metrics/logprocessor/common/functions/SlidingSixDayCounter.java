package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Sliding counter that counts using 6 day windows. This divides 90d evenly. */
public class SlidingSixDayCounter<KEY, T> extends SlidingCounter<KEY, T> {
  @VisibleForTesting
  public static final int EXPIRE_TTL_SECONDS = Math.toIntExact(Duration.ofDays(120).toSeconds());

  public SlidingSixDayCounter(
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    this(false, keyTypeInfo, eventTimeGetter, countGetter, sideOutputDebugLogging);
  }

  public SlidingSixDayCounter(
      boolean queryableStateEnabled,
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    super(
        queryableStateEnabled,
        ChronoUnit.DAYS,
        ImmutableList.of(90),
        keyTypeInfo,
        Duration.ofDays(6),
        eventTimeGetter,
        countGetter,
        sideOutputDebugLogging);
  }

  @Override
  int expiry(int bucket) {
    return EXPIRE_TTL_SECONDS;
  }
}
