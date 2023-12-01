package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO - Implement a version that caches.  This date rendering is probably expensive.

/**
 * Assigns items to a bucket using a Hadoop supported "/dt={yyyy-MM-dd}/hour={HH}" format.
 *
 * @param <T> Type of element
 */
public final class DateHourBucketAssigner<T> extends BasePathBucketAssigner<T> {
  private static final Logger LOGGER = LogManager.getLogger(DateHourBucketAssigner.class);
  private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("HH");
  private static final Random RANDOM = new Random();
  private static final float LOG_RATE = 0.01f;

  private final SerializableToLongFunction<T> getTimestampLong;

  public DateHourBucketAssigner(SerializableToLongFunction<T> getTimestamp) {
    this.getTimestampLong = getTimestamp;
  }

  @Override
  public String getBucketId(T element, Context context) {
    // Assumes millis.
    long timestamp = this.getTimestampLong.applyAsLong(element);
    // TODO - Use a rate limiting logger instead.
    if (timestamp == 0 && RANDOM.nextFloat() <= LOG_RATE) {
      LOGGER.warn("BucketId timestamp is 0 for element: {}", element);
    }
    LocalDateTime datetime =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    // TODO - we can probably create a caching version of this that uses time ranges.
    return "/dt="
        + DateTimeFormatter.ISO_LOCAL_DATE.format(datetime)
        + "/hour="
        + HOUR_FORMAT.format(datetime);
  }

  @Override
  public String toString() {
    return DateHourBucketAssigner.class.getSimpleName();
  }
}
