package ai.promoted.metrics.logprocessor.common.functions;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/** Sliding counter that counts T's for an hour over 15m windows. */
public class SlidingHourlyCounter<KEY, T> extends SlidingCounter<KEY, T> {
    private final int emitWindowInMinutes;

    public SlidingHourlyCounter(
            TypeInformation<KEY> keyTypeInfo,
            Duration emitWindow,
            SerializableFunction<T, Long> eventTimeGetter,
            SerializableFunction<T, Long> countGetter,
            boolean sideOutputDebugLogging) {
        // Hourly counts need to be "fresh", but using too small of a bucket would cause much more
        // state and write overhead.  1hr by 15m windows results in at most 4 concurrent window
        // states to track and 15m of staleness in the worst-case.
        super(ChronoUnit.HOURS, ImmutableList.of(1), keyTypeInfo, emitWindow, eventTimeGetter, countGetter, sideOutputDebugLogging);
        int minutes = (int) emitWindow.toMinutes();
        Preconditions.checkArgument(minutes >= 1 && minutes <= 60,
                "emitWindow should be between 1 and 60 minutes; emitWindowMinutes=%s", minutes);
        Preconditions.checkArgument(Duration.ofHours(1).toMillis() % emitWindow.toMillis() == 0,
                "60 minutes divided by emitWindow should have no remainder.  emitWindow should create even buckets; emitWindow",
                emitWindow);
        this.emitWindowInMinutes = minutes;
    }

    /** Returns a window key that ceilings to an emitWindow multiple. */
    long toWindowKey(LocalDateTime datetime) {
        int minute = datetime.getMinute();
        int mod = minute % emitWindowInMinutes;
        // Rounds up to 15 minutes.
        // TODO - limit the flag range.
        datetime = datetime.plusMinutes(emitWindowInMinutes - mod);
        long window = 100000000L * datetime.getYear()
            + 1000000L * datetime.getMonthValue()
            + 10000L * datetime.getDayOfMonth()
            + 100L * datetime.getHour()
            + datetime.getMinute();
        return window;
    }

    /** Returns a datetime for our 15m window key. */
    LocalDateTime windowDateTime(long windowKey) {
        int min = (int) (windowKey % 100);
        int hour = (int) ((windowKey / 100) % 100);
        int day = (int) ((windowKey / 10000) % 100);
        int month = (int) ((windowKey / 1000000) % 100);
        int year = (int) (windowKey / 100000000L);
        return LocalDateTime.of(year, month, day, hour, min);
    }
}
