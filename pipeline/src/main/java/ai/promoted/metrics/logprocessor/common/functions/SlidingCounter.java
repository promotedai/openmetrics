package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.counter.WindowAggResult;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.queryablestate.KeyedProcessFunctionWithQueryableState;
import ai.promoted.proto.flinkqueryablestate.MetricsValues;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Keyed process function that counts T's on a (bucketed) basis using their event watermark.
 * Produces Tuple4 of <KEY, time bucket, count within the bucket, expiry>
 *
 * @param <KEY> Flink partitioning key that we are counting.
 * @param <T> Object type of the flink stream.
 */
public class SlidingCounter<KEY, T>
    extends KeyedProcessFunctionWithQueryableState<KEY, T, WindowAggResult<KEY>>
    implements ResultTypeQueryable<WindowAggResult<KEY>> {
  // TODO: generalize to a standalone logging sideoutput class
  /** Type info for logging. */
  private static final String LOG_SCHEMA_STRING =
      "{ \"type\": \"record\","
          + " \"name\": \"sliding_counter_log\", \"namespace\": \"ai.promoted.metrics\","
          + " \"fields\": ["
          + " {\"name\": \"class\", \"type\": \"string\"},"
          + " {\"name\": \"method\", \"type\": \"string\"},"
          + " {\"name\": \"timestamp\", \"type\": \"long\"},"
          + " {\"name\": \"watermark\", \"type\": \"long\"},"
          + " {\"name\": \"key\", \"type\": \"string\"},"
          + " {\"name\": \"eventApiTimestamp\", \"type\": [\"null\", \"long\"]},"
          + " {\"name\": \"message\", \"type\": [\"null\", \"string\"]},"
          + " {\"name\": \"windowCounts\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]},"
          + " {\"name\": \"bucketSum\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]},"
          + " {\"name\": \"removed\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"long\"}]}"
          + "] }";

  public static final Schema LOG_SCHEMA = new Schema.Parser().parse(LOG_SCHEMA_STRING);
  public static final TypeInformation<GenericRecord> LOG_ROW_TYPEINFO =
      new GenericRecordAvroTypeInfo(LOG_SCHEMA);

  /** OutputTag to get logging maps in a side channel. */
  public static final OutputTag<GenericRecord> LOGGING_TAG =
      new OutputTag<GenericRecord>("logging", LOG_ROW_TYPEINFO);

  @VisibleForTesting protected final long windowSlide;
  private final ChronoUnit bucketUnit;
  private final List<Integer> buckets;
  private final TypeInformation<KEY> keyTypeInfo;
  private final int maxBucket;
  private final long bucketUnitMillis;
  private final SerializableFunction<T, Long> eventTimeGetter;
  private final SerializableFunction<T, Long> countGetter;
  private final boolean sideOutputDebugLogging;

  // Window key long (see toWindowKey) => count.
  private transient MapState<Long, Long> windowCounts;

  /**
   * Creates a SlidingCounter instance.
   *
   * @param bucketUnit Granularity of buckets.
   * @param buckets Time buckets to aggregate over. *MUST* have largest bucket first.
   * @param windowSlide Window slide. For example, 4 hrs = 6 updates/day.
   * @param eventTimeGetter Function to extract event timestamps from input types.
   */
  public SlidingCounter(
      boolean queryableStateEnabled,
      ChronoUnit bucketUnit,
      List<Integer> buckets,
      TypeInformation<KEY> keyTypeInfo,
      Duration windowSlide,
      SerializableFunction<T, Long> eventTimeGetter,
      SerializableFunction<T, Long> countGetter,
      boolean sideOutputDebugLogging) {
    super(queryableStateEnabled);
    this.bucketUnit = bucketUnit;
    this.bucketUnitMillis = bucketUnit.getDuration().toMillis();
    this.buckets = buckets;
    this.maxBucket = Collections.max(buckets);
    this.keyTypeInfo = keyTypeInfo;
    this.windowSlide = windowSlide.toMillis();
    this.eventTimeGetter = eventTimeGetter;
    this.countGetter = countGetter;
    this.sideOutputDebugLogging = sideOutputDebugLogging;
  }

  @VisibleForTesting
  static Instant toDateTime(long millis) {
    return Instant.ofEpochMilli(millis);
  }

  /** Returns a key for the counts. Rounds up based on the windowSlide. */
  @VisibleForTesting
  long toWindowKey(Instant datetime) {
    return datetime.toEpochMilli() + windowSlide - (datetime.toEpochMilli() % windowSlide);
  }

  @VisibleForTesting
  Instant windowDateTime(long windowKey) {
    return Instant.ofEpochMilli(windowKey);
  }

  /**
   * Return the expiry TTL in seconds for the redis key.
   *
   * <p>It is important to know that redis expiry happens on the redis key lavel, NOT the individual
   * hash keys/values. Positive values will set an expiry. Negative values clears any expiry. 0 does
   * no ttl modification.
   */
  int expiry(int bucket) {
    return 0;
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    windowCounts =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("windowCounts", Types.LONG, Types.LONG));
  }

  @Override
  public TypeInformation<WindowAggResult<KEY>> getProducedType() {
    return WindowAggResult.getTypeInformation(keyTypeInfo);
  }

  @Override
  public void processElement(T in, Context ctx, Collector<WindowAggResult<KEY>> out)
      throws Exception {
    // Using the event timestamp instead of the flink processing timestamp (ctx.timestamp).
    // This is because the flink timestamp can be delayed by the compounded inference delay
    // due to inferred refs.
    long timestamp = eventTimeGetter.apply(in);

    long window = toWindowKey(toDateTime(timestamp));
    Long count = windowCounts.get(window);
    if (count == null) {
      count = 0L;
    }
    count += countGetter.apply(in);
    windowCounts.put(window, count);

    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "processElement");
      logRow.put("timestamp", ctx.timestamp());
      logRow.put("watermark", ctx.timerService().currentWatermark());
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put("eventApiTimestamp", timestamp);
      logRow.put("message", in.toString());
      logRow.put(
          "windowCounts",
          StreamSupport.stream(windowCounts.entries().spliterator(), false)
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
      ctx.output(LOGGING_TAG, logRow);
    }

    // This timer registration is to increment the bucket counts following after the windowSlide.
    // TODO(when needed): add randomness to the timer to avoid write contention on the window
    // boundaries.
    setSafeTimer(ctx, timestamp, 0);
    for (int b : buckets) {
      // These timer registrations are meant to decrement and remove counts after "inactivity".
      // This happens at a bucketUnit period after a bucket boundary.
      setSafeTimer(ctx, timestamp, b);
    }
  }

  // Ensures we set a bucket delayed event timer that hasn't already passed yet.
  private void setSafeTimer(Context ctx, long eventTime, int nBucketDelay) {
    long flinkTime = ctx.timestamp();
    long intended =
        eventTime - (eventTime % windowSlide) + (nBucketDelay * bucketUnitMillis + windowSlide);
    if (intended < flinkTime) {
      intended = flinkTime - (flinkTime % windowSlide) + windowSlide;
    }

    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "setSafeTimer");
      logRow.put("timestamp", flinkTime);
      logRow.put("watermark", ctx.timerService().currentWatermark());
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put("eventApiTimestamp", eventTime);
      logRow.put("message", "intended timer timestamp: " + intended);
      ctx.output(LOGGING_TAG, logRow);
    }

    ctx.timerService().registerEventTimeTimer(intended);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<WindowAggResult<KEY>> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);

    HashMap<Integer, Long> bucketSum = newBucketMap();
    ArrayList<Long> toRemove = new ArrayList<>();
    Instant timerTime = toDateTime(timestamp);
    for (Long window : windowCounts.keys()) {
      Instant windowEndTime = windowDateTime(window);
      // Both windowEndTime (exclusive) and timerTime are aligned on windowSlide.
      long timeDelta = bucketUnit.between(windowEndTime, timerTime);
      if (timeDelta >= maxBucket) {
        toRemove.add(window);
      } else if (timeDelta >= 0) {
        long windowCount = windowCounts.get(window);
        for (int b : buckets) {
          if (timeDelta < b) {
            Long count = bucketSum.get(b);
            count += windowCount;
            bucketSum.put(b, count);
          }
        }
      } else {
        break;
      }
    }

    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "onTimer");
      logRow.put("timestamp", timestamp);
      logRow.put("watermark", ctx.timerService().currentWatermark());
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put(
          "windowCounts",
          StreamSupport.stream(windowCounts.entries().spliterator(), false)
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
      logRow.put(
          "bucketSum",
          bucketSum.entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
      logRow.put("removed", toRemove);
      ctx.output(LOGGING_TAG, logRow);
    }

    MetricsValues.Builder metricsValuesBuilder = MetricsValues.newBuilder();
    for (int b : bucketSum.keySet()) {
      WindowAggResult<KEY> output =
          new WindowAggResult<>(ctx.getCurrentKey(), b, bucketUnit, bucketSum.get(b), expiry(b));
      metricsValuesBuilder.addLongValues(
          MetricsValues.LongEntry.newBuilder()
              .setKey(String.valueOf(b))
              .setValue(bucketSum.get(b)));
      if (sideOutputDebugLogging) {
        GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
        logRow.put("class", getClass().getSimpleName());
        logRow.put("method", "onTimerOutput");
        logRow.put("timestamp", timestamp);
        logRow.put("watermark", ctx.timerService().currentWatermark());
        logRow.put("key", String.valueOf(ctx.getCurrentKey()));
        logRow.put("message", output.toString());
        ctx.output(LOGGING_TAG, logRow);
      }

      if (!isQueryableStateEnabled()) {
        // Don't output result if queryable state is enabled.
        out.collect(output);
      }
    }
    updateOrClearQueryableState(metricsValuesBuilder.build());

    for (long key : toRemove) {
      windowCounts.remove(key);
    }
  }

  private HashMap<Integer, Long> newBucketMap() {
    HashMap<Integer, Long> bucketMap = new HashMap<>(buckets.size());
    for (int bucket : buckets) {
      bucketMap.put(bucket, 0L);
    }
    return bucketMap;
  }
}
