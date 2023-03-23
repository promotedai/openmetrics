package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.counter.LastTimeAggResult;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Keyed process function that remembers the last timestamp and counts T's using their event
 * watermark. The class has a TTL that roughly represents how long Flink and Redis will keep track
 * of the events for a user. Produces Tuple4 of <KEY, last timestamp millis, 90d count, expiry>
 *
 * @param <KEY> Flink partitioning key that we are tracking.
 * @param <T> Object type of the flink stream.
 */
public class LastTimeAndCount<KEY, T> extends KeyedProcessFunction<KEY, T, LastTimeAggResult<KEY>>
    implements ResultTypeQueryable<LastTimeAggResult<KEY>> {
  // TODO: generalize to a standalone logging sideoutput class
  @VisibleForTesting
  public static final int EXPIRE_TTL_SECONDS = Math.toIntExact(Duration.ofDays(3).toSeconds());
  /** Type info for logging. */
  private static final String LOG_SCHEMA_STRING =
      "{ \"type\": \"record\","
          + " \"name\": \"last_time_and_count\", \"namespace\": \"ai.promoted.metrics\","
          + " \"fields\": ["
          + " {\"name\": \"class\", \"type\": \"string\"},"
          + " {\"name\": \"method\", \"type\": \"string\"},"
          + " {\"name\": \"timestamp\", \"type\": \"long\"},"
          + " {\"name\": \"key\", \"type\": \"string\"},"
          + " {\"name\": \"eventApiTimestamp\", \"type\": [\"null\", \"long\"]},"
          + " {\"name\": \"message\", \"type\": [\"null\", \"string\"]},"
          + " {\"name\": \"dayCounts\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]},"
          + " {\"name\": \"removed\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}"
          + "] }";

  public static final Schema LOG_SCHEMA = new Schema.Parser().parse(LOG_SCHEMA_STRING);
  public static final TypeInformation<GenericRecord> LOG_ROW_TYPEINFO =
      new GenericRecordAvroTypeInfo(LOG_SCHEMA);
  /** OutputTag to get logging maps in a side channel. */
  public static final OutputTag<GenericRecord> LOGGING_TAG =
      new OutputTag<GenericRecord>("logging", LOG_ROW_TYPEINFO);

  @VisibleForTesting final int keepTtlSeconds;
  private final TypeInformation<KEY> keyTypeInfo;
  private final SerializableFunction<T, Long> eventTimeGetter;
  // Events data will be kept between ttlDays and ttlDays+2 (depending on date alignment and extra
  // buffers).
  private final long ttlDays;
  private final boolean sideOutputDebugLogging;
  // This function allows for shifting the clean-up so we don't have hotspots. This is a variable to
  // simplify tests.
  private final SerializableFunction<Integer, Long> hashToRandomTimerOffset;
  // date => count.
  private transient MapState<Instant, Long> dayCounts;
  // Keeps the time of the most recently processed event.  This might not be the latest timestamp.
  // eventTimeGetter semantics.
  private transient ValueState<Long> lastTimestamp;

  /**
   * Creates a LastTimeAndCount instance.
   *
   * @param eventTimeGetter Function to extract event timestamps from input types.
   * @param ttl This is how (roughly) how long we'll keep track of the events.
   */
  public LastTimeAndCount(
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      Duration ttl,
      boolean sideOutputDebugLogging) {
    this(
        keyTypeInfo,
        eventTimeGetter,
        ttl,
        sideOutputDebugLogging,
        LastTimeAndCount::pseudoRandomTimerOffset);
  }

  @VisibleForTesting
  public LastTimeAndCount(
      TypeInformation<KEY> keyTypeInfo,
      SerializableFunction<T, Long> eventTimeGetter,
      Duration ttl,
      boolean sideOutputDebugLogging,
      SerializableFunction<Integer, Long> hashToRandomTimerOffset) {
    this.keyTypeInfo = keyTypeInfo;
    this.eventTimeGetter = eventTimeGetter;
    this.ttlDays = ttl.toDays();
    // Q - Dan doesn't know why we have 3 days buffer.
    // TODO - switch to Duration.
    keepTtlSeconds = Math.toIntExact(ttl.plusDays(3).toSeconds());
    this.sideOutputDebugLogging = sideOutputDebugLogging;
    this.hashToRandomTimerOffset = hashToRandomTimerOffset;
  }

  /**
   * Returns a pseudo-random [0, 1) value using hash as a seed. This is cheaper than running Random.
   * This doesn't have to be perfect. It's just for spreading out timers.
   */
  @VisibleForTesting
  static long pseudoRandomTimerOffset(int hash) {
    // This is a hand-picked prime to smooth out the range.
    hash = 513431879 * hash;
    // Exclude 1.0.
    if (hash == Integer.MIN_VALUE) {
      hash = 0;
    }
    float rate = Math.abs(1f * hash / Integer.MIN_VALUE);
    return (long) (rate * 1000L * 60L * 60L * 24L);
  }

  @Override
  public void open(Configuration config) throws Exception {
    super.open(config);
    dayCounts =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("dayCounts", Types.INSTANT, Types.LONG));
    lastTimestamp =
        getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimestamp", Types.LONG));
  }

  @Override
  public TypeInformation<LastTimeAggResult<KEY>> getProducedType() {
    return LastTimeAggResult.getTypeInformation(keyTypeInfo);
  }

  @Override
  public void processElement(T in, Context ctx, Collector<LastTimeAggResult<KEY>> out)
      throws Exception {
    // Using the event timestamp instead of the flink processing timestamp (ctx.timestamp).
    // This is because the flink timestamp can be delayed by the compounded inference delay
    // due to inferred refs.
    long timestamp = eventTimeGetter.apply(in);
    lastTimestamp.update(timestamp);

    Instant day = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.DAYS);
    Long count = dayCounts.get(day);
    if (count == null) {
      count = 0L;
    }
    count += 1;
    dayCounts.put(day, count);

    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "processElement");
      logRow.put("timestamp", ctx.timestamp());
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put("eventApiTimestamp", timestamp);
      logRow.put("message", in.toString());
      logRow.put(
          "dayCounts",
          StreamSupport.stream(dayCounts.entries().spliterator(), false)
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
      ctx.output(LOGGING_TAG, logRow);
    }

    outputAndCleanup(timestamp, ctx, out);
    // These timer registrations are meant to decrement and remove counts after "inactivity".
    scheduleTimer(ctx, day.toEpochMilli(), timestamp, ttlDays);
  }

  // Ensures we set a bucket delayed event timer that hasn't already passed yet.
  private void scheduleTimer(Context ctx, long eventStartOfDay, long eventTime, long nDays) {
    long flinkTime = ctx.timestamp();
    long intended =
        getCleanupTime(ctx.getCurrentKey().hashCode(), eventStartOfDay, nDays, flinkTime);

    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "scheduleTimer");
      logRow.put("timestamp", flinkTime);
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put("eventApiTimestamp", eventTime);
      logRow.put("message", "scheduled timer: " + intended);
      ctx.output(LOGGING_TAG, logRow);
    }
    ctx.timerService().registerEventTimeTimer(intended);
  }

  /**
   * Gets the time for the cleanup timer.
   *
   * <p>Since counts are stored per day, it's easier to bucket the cleanup to per day too. We also
   * want to spread out cleanup timers. We'll use the hash of the key.
   *
   * <p>nDay = 1
   *
   * <p>v eventStartOfDay v event Day 1 |--------------------------------------------------|
   *
   * <p>v the earliest we want to clean up. Day 2
   * |--------------------------------------------------|
   *
   * <p><---- Cleanup based on hashcode of key. ---------> Day 3
   * |--------------------------------------------------|
   */
  private long getCleanupTime(int keyHash, long eventStartOfDay, long nDays, long flinkTime) {
    // Factor the eventStartOfDay into the hash so spammy keys don't become slow the same time per
    // day.
    int hash = keyHash + (int) (eventStartOfDay / 1000);
    // Q - Why is flinkTime considered?  Maybe if is a corner case for backfill?
    return Math.max(
        eventStartOfDay
            + Duration.ofDays(nDays + 2).toMillis()
            + hashToRandomTimerOffset.apply(hash),
        flinkTime);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<LastTimeAggResult<KEY>> out)
      throws Exception {
    super.onTimer(timestamp, ctx, out);
    outputAndCleanup(timestamp, ctx, out);
  }

  private void outputAndCleanup(long timestamp, Context ctx, Collector<LastTimeAggResult<KEY>> out)
      throws Exception {
    long count = 0;
    ArrayList<Instant> toRemove = new ArrayList<>();
    Instant timerTime = Instant.ofEpochMilli(timestamp);
    for (Instant day : dayCounts.keys()) {
      long timeDelta = ChronoUnit.DAYS.between(day, timerTime);
      // TODO - change this conditional to `>` after we've had the timer improvement deployed long
      // enough.
      // Either May or we do a full blue release.
      // In January 2023, this is not safe to change since the existing timers will fire early.
      // It's easier to wait 3 months instead of trying to code up conditional logic for this case.
      if (timeDelta >= ttlDays) {
        toRemove.add(day);
      } else if (timeDelta >= 0) {
        count += dayCounts.get(day);
      } else {
        break;
      }
    }

    LastTimeAggResult<KEY> output =
        new LastTimeAggResult<>(
            ctx.getCurrentKey(),
            lastTimestamp.value(),
            count,
            // Note, we set a more aggressive TTL when the count goes to 0.
            count == 0 ? EXPIRE_TTL_SECONDS : keepTtlSeconds);
    if (sideOutputDebugLogging) {
      GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
      logRow.put("class", getClass().getSimpleName());
      logRow.put("method", "onTimerOutput");
      logRow.put("timestamp", timestamp);
      logRow.put("key", String.valueOf(ctx.getCurrentKey()));
      logRow.put(
          "dayCounts",
          StreamSupport.stream(dayCounts.entries().spliterator(), false)
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
      logRow.put("message", output.toString());
      logRow.put("removed", toRemove);
      ctx.output(LOGGING_TAG, logRow);
    }

    if (output.f1 != null) {
      out.collect(output);
    } else if (count != 0) {
      Map<String, Long> strCounts =
          StreamSupport.stream(dayCounts.entries().spliterator(), false)
              .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
      throw new IllegalStateException(
          String.format(
              "no timestamp means we should be cleared state.  timestamp: %s\noutput: %s\ndayCounts: %s\ntoRemove: %s",
              timestamp, output, strCounts, toRemove));
    }

    for (Instant key : toRemove) {
      dayCounts.remove(key);
    }
    if (dayCounts.isEmpty()) {
      lastTimestamp.clear();
    }
  }
}
