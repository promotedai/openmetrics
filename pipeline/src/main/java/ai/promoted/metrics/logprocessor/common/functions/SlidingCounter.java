package ai.promoted.metrics.logprocessor.common.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/** Abstract keyed process function that counts T's on a (bucketed) basis using their event watermark.
 * @param <KEY> Flink partitioning key that we are counting.
 * @param <T> Object type of the flink stream.
 * @return Tuple4 of <KEY, time bucket, count within the bucket, expiry>
 */
abstract class SlidingCounter<KEY, T>
    extends KeyedProcessFunction<KEY, T, Tuple4<KEY, Integer, Long, Integer>>
    implements ResultTypeQueryable<Tuple4<KEY, Integer, Long, Integer>> {
    // TODO: generalize to a standalone logging sideoutput class
    /** Type info for logging. */
    private static final String LOG_SCHEMA_STRING = "{ \"type\": \"record\","
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
    public static final TypeInformation<GenericRecord> LOG_ROW_TYPEINFO = new GenericRecordAvroTypeInfo(LOG_SCHEMA);
    /** OutputTag to get logging maps in a side channel. */
    public static final OutputTag<GenericRecord> LOGGING_TAG = new OutputTag<GenericRecord>("logging", LOG_ROW_TYPEINFO);

    private final ChronoUnit bucketUnit;
    private final List<Integer> buckets;
    private final TypeInformation<KEY> keyTypeInfo;
    private final int maxBucket;
    @VisibleForTesting final long emitWindow;
    private final long bucketUnitMillis;
    private final SerializableFunction<T, Long> eventTimeGetter;
    private final SerializableFunction<T, Long> countGetter;
    private final boolean sideOutputDebugLogging;

    // Window key long (see toWindowKey) => count.
    private transient MapState<Long, Long> windowCounts;

    /**
     * Creates a SlidingCounter instance.
     * @param bucketUnit Granularity of buckets.
     * @param buckets Time buckets to aggregate over.  *MUST* have largest bucket first.
     * @param emitWindow Window to emit events.  For example, 4 hrs = 6 updates/day.
     * @param eventTimeGetter Function to extract event timestamps from input types.
     */
    @VisibleForTesting
    SlidingCounter(
            ChronoUnit bucketUnit,
            List<Integer> buckets,
            TypeInformation<KEY> keyTypeInfo,
            Duration emitWindow,
            SerializableFunction<T, Long> eventTimeGetter,
            SerializableFunction<T, Long> countGetter,
            boolean sideOutputDebugLogging) {
        this.bucketUnit = bucketUnit;
        this.bucketUnitMillis = bucketUnit.getDuration().toMillis();
        this.buckets = buckets;
        this.maxBucket = Collections.max(buckets);
        this.keyTypeInfo = keyTypeInfo;
        this.emitWindow = emitWindow.toMillis();
        this.eventTimeGetter = eventTimeGetter;
        this.countGetter = countGetter;
        this.sideOutputDebugLogging = sideOutputDebugLogging;
    }

    /** Returns a window key long for a datetime which needs to be aligned on the end time.
     * For example, YYYYMMDD (D8), YYYYMMDDHH, YYYYMMDDHHmm, etc.
     */
    abstract long toWindowKey(LocalDateTime datetime);

    /** Returns a datetime for a window key long. */
    abstract LocalDateTime windowDateTime(long windowKey);

    /**
     * Return the expiry TTL in seconds for the redis key.
     *
     * It is important to know that redis expiry happens on the redis key lavel, NOT the individual hash keys/values.
     * Positive values will set an expiry.  Negative values clears any expiry.  0 does no ttl modification.
     */
    int expiry(int bucket) {
        return 0;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        // PR(Jin) - what's a good backup TTL?
        windowCounts = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("windowCounts", Types.LONG, Types.LONG));
    }

    @Override
    public TypeInformation<Tuple4<KEY, Integer, Long, Integer>> getProducedType() {
        return Types.TUPLE(
                keyTypeInfo,
                Types.INT,
                Types.LONG,
                Types.INT);
    }

    @Override
    public void processElement(
            T in, Context ctx, Collector<Tuple4<KEY, Integer, Long, Integer>> out) throws Exception {
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
            logRow.put("windowCounts",
                    StreamSupport.stream(windowCounts.entries().spliterator(), false)
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            ctx.output(LOGGING_TAG, logRow);
        }

        // This timer registration is to increment the bucket counts following after the emitWindow.
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
        long intended = eventTime - (eventTime % emitWindow) + (nBucketDelay * bucketUnitMillis + emitWindow);
        if (intended < flinkTime) {
            intended = flinkTime - (flinkTime % emitWindow) + emitWindow;
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
    public void onTimer(
            long timestamp, OnTimerContext ctx, Collector<Tuple4<KEY, Integer, Long, Integer>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        HashMap<Integer, Long> bucketSum = newBucketMap();
        ArrayList<Long> toRemove = new ArrayList<>();
        LocalDateTime timerTime = toDateTime(timestamp);
        for (Long window : windowCounts.keys()) {
            LocalDateTime windowEndTime = windowDateTime(window);
            // Both windowEndTime (exclusive) and timerTime are aligned on emitWindow.
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
            }
        }

        if (sideOutputDebugLogging) {
            GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
            logRow.put("class", getClass().getSimpleName());
            logRow.put("method", "onTimer");
            logRow.put("timestamp", timestamp);
            logRow.put("watermark", ctx.timerService().currentWatermark());
            logRow.put("key", String.valueOf(ctx.getCurrentKey()));
            logRow.put("windowCounts",
                    StreamSupport.stream(windowCounts.entries().spliterator(), false)
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            logRow.put("bucketSum",
                    bucketSum.entrySet().stream()
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            logRow.put("removed", toRemove);
            ctx.output(LOGGING_TAG, logRow);
        }

        for (int b : bucketSum.keySet()) {
            Tuple4<KEY, Integer, Long, Integer> output = Tuple4.of(ctx.getCurrentKey(), b, bucketSum.get(b), expiry(b));

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

            out.collect(output);
        }

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

    @VisibleForTesting
    static LocalDateTime toDateTime(long millis) {
        // TODO: This could be costly.  If we need to optimize (and lose easier debugging), we can
        // stay in timestamp millis domain and truncate to the nearest bucketUnit in millis.
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
