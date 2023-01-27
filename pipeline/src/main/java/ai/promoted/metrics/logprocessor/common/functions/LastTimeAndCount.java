package ai.promoted.metrics.logprocessor.common.functions;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Keyed process function that remembers the last timestamp and counts T's using their event watermark.
 * @param <KEY> Flink partitioning key that we are tracking.
 * @param <T> Object type of the flink stream.
 * @return Tuple4 of <KEY, last timestamp millis, 90d count, expiry>
 */
public class LastTimeAndCount<KEY, T>
    extends KeyedProcessFunction<KEY, T, Tuple4<KEY, Long, Long, Integer>>
    implements ResultTypeQueryable<Tuple4<KEY, Long, Long, Integer>> {
    // TODO: generalize to a standalone logging sideoutput class
    /** Type info for logging. */
    private static final String LOG_SCHEMA_STRING = "{ \"type\": \"record\","
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
    public static final TypeInformation<GenericRecord> LOG_ROW_TYPEINFO = new GenericRecordAvroTypeInfo(LOG_SCHEMA);
    /** OutputTag to get logging maps in a side channel. */
    public static final OutputTag<GenericRecord> LOGGING_TAG = new OutputTag<GenericRecord>("logging", LOG_ROW_TYPEINFO);

    private static final long TTL_DAYS = 90;
    @VisibleForTesting
    public static final int KEEP_TTL_SECONDS = Math.toIntExact(Duration.ofDays(TTL_DAYS + 3).toSeconds());
    @VisibleForTesting
    public static final int EXPIRE_TTL_SECONDS = Math.toIntExact(Duration.ofDays(3).toSeconds());

    private final TypeInformation<KEY> keyTypeInfo;
    private final SerializableFunction<T, Long> eventTimeGetter;
    private final long emitWindow;
    private final boolean sideOutputDebugLogging;

    // date => count.
    private transient MapState<Instant, Long> dayCounts;
    // eventTimeGetter semantics.
    private transient ValueState<Long> lastTimestamp;

    /**
     * Creates a LastTimeAndCount instance.
     * @param eventTimeGetter Function to extract event timestamps from input types.
     */
    @VisibleForTesting
    public LastTimeAndCount(
            TypeInformation<KEY> keyTypeInfo,
            SerializableFunction<T, Long> eventTimeGetter,
            Duration emitWindow,
            boolean sideOutputDebugLogging) {
        this.keyTypeInfo = keyTypeInfo;
        this.eventTimeGetter = eventTimeGetter;
        this.emitWindow = emitWindow.toMillis();
        this.sideOutputDebugLogging = sideOutputDebugLogging;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        dayCounts = getRuntimeContext().getMapState(new MapStateDescriptor<>("dayCounts", Types.INSTANT, Types.LONG));
        lastTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimestamp", Types.LONG));
    }

    @Override
    public TypeInformation<Tuple4<KEY, Long, Long, Integer>> getProducedType() {
        return Types.TUPLE(
                keyTypeInfo,
                Types.LONG,
                Types.LONG,
                Types.INT);
    }

    @Override
    public void processElement(
            T in, Context ctx, Collector<Tuple4<KEY, Long, Long, Integer>> out) throws Exception {
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
            logRow.put("dayCounts",
                    StreamSupport.stream(dayCounts.entries().spliterator(), false)
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            ctx.output(LOGGING_TAG, logRow);
        }

        if (emitWindow == 0) {
            outputAndCleanup(timestamp, ctx, out);
        } else {
            // This timer registration is to increment the counts following after the emitWindow.
            // TODO(when needed): add randomness to the timer to avoid write contention on the window
            // boundaries.
            setSafeTimer(ctx, timestamp, 0);
        }
        // These timer registrations are meant to decrement and remove counts after "inactivity".
        setSafeTimer(ctx, day.toEpochMilli(), TTL_DAYS);
    }

    // Ensures we set a bucket delayed event timer that hasn't already passed yet.
    private void setSafeTimer(Context ctx, long eventTime, long nDays) {
        long flinkTime = ctx.timestamp();
        long windowedEventTime = eventTime - (emitWindow == 0 ? 0 : (eventTime % emitWindow));
        long intended = windowedEventTime + Duration.ofDays(nDays).toMillis() + emitWindow;
        if (intended < flinkTime) {
            long windowedFlinkTime = flinkTime - (emitWindow == 0 ? 0 : (flinkTime % emitWindow));
            intended = windowedFlinkTime + emitWindow;
        }

        if (sideOutputDebugLogging) {
            GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
            logRow.put("class", getClass().getSimpleName());
            logRow.put("method", "setSafeTimer");
            logRow.put("timestamp", flinkTime);
            logRow.put("key", String.valueOf(ctx.getCurrentKey()));
            logRow.put("eventApiTimestamp", eventTime);
            logRow.put("message", "intended timer timestamp: " + intended);
            ctx.output(LOGGING_TAG, logRow);
        }

        ctx.timerService().registerEventTimeTimer(intended);
    }

    @Override
    public void onTimer(
            long timestamp, OnTimerContext ctx, Collector<Tuple4<KEY, Long, Long, Integer>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        outputAndCleanup(timestamp, ctx, out);
    }

    private void outputAndCleanup(long timestamp, Context ctx, Collector<Tuple4<KEY, Long, Long, Integer>> out) throws Exception {
        long count = 0;
        ArrayList<Instant> toRemove = new ArrayList<>();
        Instant timerTime = Instant.ofEpochMilli(timestamp);
        for (Instant day : dayCounts.keys()) {
            long timeDelta = ChronoUnit.DAYS.between(day, timerTime);
            if (timeDelta >= TTL_DAYS) {
                toRemove.add(day);
            } else if (timeDelta >= 0) {
                count += dayCounts.get(day);
            }
        }

        Tuple4<KEY, Long, Long, Integer> output = Tuple4.of(
                ctx.getCurrentKey(),
                lastTimestamp.value(),
                count,
                // Note, we set a more aggressive TTL when the count goes to 0.
                count == 0 ? EXPIRE_TTL_SECONDS : KEEP_TTL_SECONDS);
        if (sideOutputDebugLogging) {
            GenericRecord logRow = new GenericData.Record(LOG_SCHEMA);
            logRow.put("class", getClass().getSimpleName());
            logRow.put("method", "onTimerOutput");
            logRow.put("timestamp", timestamp);
            logRow.put("key", String.valueOf(ctx.getCurrentKey()));
            logRow.put("dayCounts",
                    StreamSupport.stream(dayCounts.entries().spliterator(), false)
                            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
            logRow.put("message", output.toString());
            logRow.put("removed", toRemove);
            ctx.output(LOGGING_TAG, logRow);
        }

        if (output.f1 != null) {
            out.collect(output);
        } else {
            Map<String, Long> strCounts = StreamSupport
                .stream(dayCounts.entries().spliterator(), false)
                .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
            checkState(count == 0, "no timestamp means we should be cleared state.  timestamp: %s\noutput: %s\ndayCounts: %s\ntoRemove: %s", timestamp, output, strCounts, toRemove);
        }

        for (Instant key : toRemove) {
            dayCounts.remove(key);
        }
        if (dayCounts.isEmpty()) {
            lastTimestamp.clear();
        }
    }
}
