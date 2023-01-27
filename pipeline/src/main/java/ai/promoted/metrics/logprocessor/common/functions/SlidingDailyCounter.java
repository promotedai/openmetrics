package ai.promoted.metrics.logprocessor.common.functions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/** Sliding counter that counts T's on 30, 7 and 1 day buckets using 4h windows. */
public class SlidingDailyCounter<KEY, T> extends SlidingCounter<KEY, T> {
    @VisibleForTesting
    public static final int EXPIRE_TTL_SECONDS = Math.toIntExact(Duration.ofDays(60).toSeconds());

    public SlidingDailyCounter(
            TypeInformation<KEY> keyTypeInfo,
            SerializableFunction<T, Long> eventTimeGetter,
            SerializableFunction<T, Long> countGetter,
            boolean sideOutputDebugLogging) {
        super(ChronoUnit.DAYS, ImmutableList.of(30, 7, 1), keyTypeInfo, Duration.ofHours(4), eventTimeGetter, countGetter, sideOutputDebugLogging);
    }

    /** Returns a 4h window key long. */
    long toWindowKey(LocalDateTime datetime) {
        int hour = datetime.getHour();
        int mod = hour % 4;
        datetime = datetime.plusHours(4 - mod);
        long window = 1000000L * datetime.getYear()
            + 10000L * datetime.getMonthValue()
            + 100L * datetime.getDayOfMonth()
            + datetime.getHour();
        return window;
    }

    /** Returns a datetime for our 4h window key. */
    LocalDateTime windowDateTime(long windowKey) {
        int hour = (int) (windowKey % 100);
        int day = (int) ((windowKey / 100) % 100);
        int month = (int) ((windowKey / 10000) % 100);
        int year = (int) (windowKey / 1000000L);
        return LocalDateTime.of(year, month, day, hour, 0);
    }

    /** Use expire TTLs relative to the 30d bucket. */
    @Override
    int expiry(int bucket) {
        return bucket == 30 ? EXPIRE_TTL_SECONDS : 0;
    }
}
