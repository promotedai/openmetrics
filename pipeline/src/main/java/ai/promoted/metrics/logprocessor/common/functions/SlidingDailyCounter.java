package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Sliding counter that counts T's on 30, 7 and 1 day buckets using 4h windows. */
public class SlidingDailyCounter<KEY, T> extends SlidingCounter<KEY, T> {
  @VisibleForTesting
  public static final int EXPIRE_TTL_SECONDS = Math.toIntExact(Duration.ofDays(60).toSeconds());

  public SlidingDailyCounter(
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    this(false, keyTypeInfo, eventTimeGetter, countGetter, sideOutputDebugLogging);
  }

  public SlidingDailyCounter(
      boolean queryableStateEnabled,
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    super(
        queryableStateEnabled,
        ChronoUnit.DAYS,
        ImmutableList.of(30, 7, 1),
        keyTypeInfo,
        Duration.ofHours(4),
        eventTimeGetter,
        countGetter,
        sideOutputDebugLogging);
  }

  /** Use expire TTLs relative to the 30d bucket. */
  @Override
  int expiry(int bucket) {
    return bucket == 30 ? EXPIRE_TTL_SECONDS : 0;
  }
}
