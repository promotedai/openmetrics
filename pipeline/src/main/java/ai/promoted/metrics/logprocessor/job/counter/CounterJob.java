package ai.promoted.metrics.logprocessor.job.counter;

import ai.promoted.metrics.logprocessor.common.functions.CounterUtil;
import ai.promoted.metrics.logprocessor.common.functions.LastTimeAndCount;
import ai.promoted.metrics.logprocessor.common.functions.RightSeenOutput;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SlidingDailyCounter;
import ai.promoted.metrics.logprocessor.common.functions.SlidingHourlyCounter;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisStandaloneSink;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafka;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.JoinedEvent;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static ai.promoted.metrics.logprocessor.job.counter.CounterKeys.*;
import static com.google.common.base.Preconditions.checkState;

/**
 * Flink job that counts along various dimensions.
 *
 * Currently, the output of this job is directed to a redis instance holding intermediary
 * aggregates accumulated here.
 */
@CommandLine.Command(name = "counter", mixinStandardHelpOptions = true, version = "counter 1.0.0",
        description = "Creates a Flink job that count JoinedEvents from Kafka and outputs to")
public class CounterJob extends BaseFlinkJob {
    private static final Logger LOGGER = LogManager.getLogger(CounterJob.class);

    public static final Joiner CSV = Joiner.on(",");

    @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment(this);
    @CommandLine.Mixin public final FlatOutputKafka flatOutputKafka = new FlatOutputKafka(kafkaSegment);
    @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this);

    @Option(names = {"--overrideInputLabel"}, defaultValue = "", description = "Overrides the Kafka input label (defaults to --jobLabel).  Can be used for cases like --jobLabel='blue.canary' and --overrideInputLabel='blue'.  Empty string means no override.  Cannot be used to override to empty string (not useful now).  Defaults to empty string (no override)")
    public String overrideInputLabel = "";

    @Option(names = {"--shardedEndpoints"}, defaultValue = "",
        description = "Endpoint URIs for sharded counter 'service' (e.g. redis://host1:6399,host2:6399,...).  Default=''")
    String shardedEndpoints = "";

    @Option(names = {"--unshardedEndpoint", "--counterService"}, defaultValue = "",
        description = "Endpoint URI for non-sharded counter 'service' (e.g. redis://localhost:6399/0).  Default=''")
    String unshardedEndpoint = "";

    @Option(names = {"--counterOutputStartTimestamp"}, required = true, description = "Required.  Timestamp to start outputting counts.  For non-backfill jobs, this should be set to the timestamp that the job was check/savepointed at.  For backfill, this value should be 0 to capture the evolving state.")
    public long counterOutputStartTimestamp;

    @Option(names = {"--counterOutputStopTimestamp"}, defaultValue = "-1", description = "Timestamp to stop outputting counts.  Negative values will be interpreted as Long.MAX_VALUE (290M+ years in the future).  Defaults to -1.")
    long counterOutputStopTimestamp = -1;

    @Option(names = {"--counterBackfillBuffer"}, defaultValue = "PT0S", description = "The time duration to buffer output writes during backfill.  Setting this to PT0S skips buffering.  WARNING: when in use, do NOT point production traffic to the counts as the data is watermark-inconsistent until the job is done backfilling (past the start time of the job).   Defaults to PT0S.")
    public Duration counterBackfillBuffer = Duration.ZERO;

    @Option(names = {"--globalEventDeviceHourlyEmitWindow"}, defaultValue = "PT15M", description = "The emit window for globalEventDevice hourly operator.  Default=PT15M.  Java8 Duration parse format.")
    public Duration globalEventDeviceHourlyEmitWindow = Duration.parse("PT15M");

    @Option(names = {"--contentEventDeviceHourlyEmitWindow"}, defaultValue = "PT15M", description = "The emit window for contentEventDevice hourly operator.  Default=PT15M.  Java8 Duration parse format.")
    public Duration contentEventDeviceHourlyEmitWindow = Duration.parse("PT15M");

    @Option(names = {"--queryEventHourlyEmitWindow"}, defaultValue = "PT15M", description = "The emit window for query-related hourly operators.  Default=PT15M.  Java8 Duration parse format.")
    public Duration queryEventHourlyEmitWindow = Duration.parse("PT15M");

    @Option(names = {"--logUserEventHourlyEmitWindow"}, defaultValue = "PT15M", description = "The emit window for logUserEvent hourly operator.  Default=PT15M.  Java8 Duration parse format.")
    public Duration logUserEventHourlyEmitWindow = Duration.parse("PT15M");

    @Option(names = {"--userEventHourlyEmitWindow"}, defaultValue = "PT15M", description = "The emit window for userEvent hourly operator.  Default=PT15M.  Java8 Duration parse format.")
    public Duration userEventHourlyEmitWindow = Duration.parse("PT15M");

    @Option(names = {"--lastTimeAndCountEmitWindow"}, defaultValue = "PT1M", description = "The emit window for lastTimeAndCount operators.  Default=PT1M.  Java8 Duration parse format.")
    public Duration lastTimeAndCountEmitWindow = Duration.parse("PT1M");

    @Option(names = {"--searchQueryLengthLimit"}, defaultValue = "100", description = "The maximum query length to allow as a top query.  Defaults to 100.")
    public int searchQueryLengthLimit = 100;

    @Option(names = {"--searchQueryWindowSize"}, defaultValue = "P14D", description = "The sliding window size for search query ranking.  Default=P14D.  Java8 Duration parse format")
    public Duration searchQueryWindowSize = Duration.ofDays(14);

    @Option(names = {"--searchQueryWindowSlide"}, defaultValue = "P3D", description = "The sliding window slide for search query ranking.  Default=P3D.  Java8 Duration parse format")
    public Duration searchQueryWindowSlide = Duration.ofDays(3);

    @Option(names = {"--searchQueryWindowMinThreshold"}, defaultValue = "1000", description = "The (inclusive) minimum query frequency to keep counts for.  Defaults to 1000.")
    public int searchQueryWindowMinThreshold = 1000;

    @Option(names = {"--no-queryCounts"}, negatable = true, description = "Disable query-related counters (query, query x content).  Default=true.")
    public boolean queryCounts = true;

    @Option(names = {"--no-lastUserQueryEvents"}, negatable = true, description = "Disable last user x query event streams.  Default=true.")
    public boolean lastUserQueryEvents = true;

    @Option(names = {"--no-hourlyQuery"}, negatable = true, description = "Disable hourly query-related counters.  Default=true.")
    public boolean hourlyQuery = true;

    @Option(names = {"--wipe"}, negatable = true, description = "Wipe redis endpoint before writing, should probably be used for backfills.  ONLY USE FOR BACKFILLS.  Defaults to false.")
    public boolean wipe = false;


    public static void main(String[] args) {
        int exitCode = new CommandLine(new CounterJob()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        validateArgs();
        startCounterJob();
        return 0;
    }

    @Override
    public void validateArgs() {
        kafkaSegment.validateArgs();
        flatOutputKafka.validateArgs();
        s3FileOutput.validateArgs();
        if (StringUtil.isBlank(shardedEndpoints) && StringUtil.isBlank(unshardedEndpoint)) {
            throw new IllegalArgumentException("--shardedEndpoints or --unshardedEndpoint must be given");
        }
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
                .addAll(kafkaSegment.getProtoClasses())
                .addAll(flatOutputKafka.getProtoClasses())
                .addAll(s3FileOutput.getProtoClasses())
                .build();
    }

    private void startCounterJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureExecutionEnvironment(env, parallelism, maxParallelism);

        // Create a local sink for sink preparation b/c flink's object lifecycle interaction is weird.
        prepareSink(newRedisSink());

        // TODO: make this pattern as part of BaseFlinkJob.
        defineJob(
                flatOutputKafka.getJoinedEventSource(env, toKafkaConsumerGroupId("countEvents"), flatOutputKafka.getJoinedEventTopic(getInputLabel())),
                flatOutputKafka.getJoinedEventSource(env, toKafkaConsumerGroupId("countEvents-user"), flatOutputKafka.getJoinedUserEventTopic(getInputLabel())),
                newRedisSink());

        env.execute(getJobName());
    }

    public String getInputLabel() {
        return StringUtil.firstNotEmpty(overrideInputLabel, getJobLabel());
    }

    @Override
    protected String getJobName() {
        return prefixJobLabel("counter");
    }

    private RedisSink newRedisSink() {
        return StringUtil.isBlank(shardedEndpoints)
            ? new RedisStandaloneSink(unshardedEndpoint)
            : new RedisSink(shardedEndpoints);
    }

    public void defineJob(
            SingleOutputStreamOperator<JoinedEvent> luidEvents,
            SingleOutputStreamOperator<JoinedEvent> uidEvents,
            SinkFunction<RedisSink.Command> redisSink) {
        final long maxQueryLength = searchQueryLengthLimit;
        final long minCountThreshold = searchQueryWindowMinThreshold;

        KeyedStream<Long, Long> topQueriesStream = null;
        if (queryCounts || lastUserQueryEvents) {
            topQueriesStream = luidEvents
                .map(FlatUtil::getQueryStringLowered)
                .uid("lower-query-string")
                .name("lower-query-string")
                .filter(s -> s.length() <= maxQueryLength)
                .uid("length-filter-query")
                .name("length-filter-query")
                .map(e -> FlatUtil.getQueryHash(e))
                .uid("hash-query")
                .name("hash-query")
                .keyBy(h -> h)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(searchQueryWindowSize.toMillis()), Time.milliseconds(searchQueryWindowSlide.toMillis())))
                .aggregate(
                        new AggregateFunction<Long, AtomicLong, AtomicLong>() {
                            @Override
                            public AtomicLong createAccumulator() {
                                return new AtomicLong(0);
                            }

                            @Override
                            public AtomicLong add(Long in, AtomicLong sum) {
                                sum.getAndIncrement();
                                return sum;
                            }

                            @Override
                            public AtomicLong getResult(AtomicLong sum) {
                                return sum;
                            }

                            @Override
                            public AtomicLong merge(AtomicLong a, AtomicLong b) {
                                a.getAndAdd(b.get());
                                return a;
                            }
                        }, new ProcessWindowFunction<AtomicLong, Long, Long, TimeWindow>() {
                            @Override
                            public void process(Long in, Context ctx, Iterable<AtomicLong> sums, Collector<Long> out) {
                                // TODO: remove when comfortable w/ behavior
                                checkState(Iterables.size(sums) == 1);
                                if (sums.iterator().next().get() >= minCountThreshold) {
                                    LOGGER.trace("{} emit: {}", ctx.window().maxTimestamp(), in);
                                    out.collect(in);
                                }
                            }
                        })
                .uid("emit-top-queries")
                .name("emit-top-queries")
                .keyBy(h -> h);
        }


        DataStream<RedisSink.Command> outputStream =
            countJoinedEvents(GLOBAL_EVENT_DEVICE_KEY, luidEvents, globalEventDeviceHourlyEmitWindow).union(
                    countJoinedEvents(CONTENT_EVENT_DEVICE_KEY, luidEvents, contentEventDeviceHourlyEmitWindow),
                    countJoinedEvents(LOG_USER_EVENT_KEY, luidEvents, logUserEventHourlyEmitWindow),
                    countJoinedEvents(USER_EVENT_KEY, uidEvents, userEventHourlyEmitWindow),
                    userEventLastTimeAndCount(LAST_LOG_USER_CONTENT_KEY, luidEvents),
                    userEventLastTimeAndCount(LAST_USER_CONTENT_KEY, uidEvents));

        if (topQueriesStream != null && queryCounts) {
            outputStream = outputStream.union(
                    countQueryJoinedEvents(QUERY_EVENT_KEY, luidEvents, topQueriesStream, queryEventHourlyEmitWindow),
                    countQueryJoinedEvents(CONTENT_QUERY_EVENT_KEY, luidEvents, topQueriesStream, queryEventHourlyEmitWindow));
        }

        if (topQueriesStream != null && lastUserQueryEvents) {
            outputStream = outputStream.union(
                    userQueryLastTimeAndCount(LAST_LOG_USER_QUERY_KEY, luidEvents, topQueriesStream),
                    userQueryLastTimeAndCount(LAST_USER_QUERY_KEY, uidEvents, topQueriesStream));
        }

        final long startTimeMillis = System.currentTimeMillis();
        final long backfillBufferMillis = counterBackfillBuffer.toMillis();

        if (backfillBufferMillis > 0) {
            outputStream = add(outputStream
                .keyBy(RedisSink.Command.REDIS_HASH_KEY)
                .process(new KeyedProcessFunction<Tuple2<String, String>, RedisSink.Command, RedisSink.Command>() {
                    private ValueState<RedisSink.Command> cachedCommand;

                    @Override
                    public void open(Configuration config) {
                        cachedCommand = getRuntimeContext().getState(new ValueStateDescriptor<>("cached-command",  RedisSink.Command.class));
                    }

                    @Override
                    public void processElement(RedisSink.Command in, Context ctx, Collector<RedisSink.Command> out) throws IOException {
                        long ts = ctx.timestamp();

                        // Backfill buffering
                        if (backfillBufferMillis > 0 && ts < startTimeMillis) {
                            cachedCommand.update(in);
                            ctx.timerService().registerEventTimeTimer(toBufferTime(ts));
                        } else {
                            // We're going to be outputting a more recent value, so just drop the cached one.
                            if (cachedCommand.value() != null) cachedCommand.clear();
                            out.collect(in);
                        }
                    }

                    @Override
                    public void onTimer(long ts, OnTimerContext ctx, Collector<RedisSink.Command> out) throws IOException {
                        if (cachedCommand.value() == null) return;
                        out.collect(cachedCommand.value());
                        cachedCommand.clear();
                    }

                    private long toBufferTime(long timestamp) {
                        long bufferTime = (timestamp - (timestamp % backfillBufferMillis)) + backfillBufferMillis;
                        // The counters themselves already bucketed.  We just need to spread the writes.
                        int partIndex = getRuntimeContext().getIndexOfThisSubtask();
                        if (partIndex > 1) {
                            bufferTime += (backfillBufferMillis / getRuntimeContext().getNumberOfParallelSubtasks()) * partIndex;
                        }
                        return bufferTime;
                    }
                }),
                "backfill-buffer-counters-output");
        }

        final long outputStartTimestamp = counterOutputStartTimestamp;
        final long outputStopTimestamp = (counterOutputStopTimestamp < 0)
            ? Long.MAX_VALUE
            : counterOutputStopTimestamp;
        outputStream = add(
                outputStream.process(new ProcessFunction<RedisSink.Command, RedisSink.Command>() {
                    @Override
                    public void processElement(RedisSink.Command in, Context ctx, Collector<RedisSink.Command> out) {
                        long ts = ctx.timestamp();
                        if (outputStartTimestamp <= ts && ts <= outputStopTimestamp) out.collect(in);
                    }
                }),
                "filter-counters-output");
        addSinkTransformation(add(outputStream.addSink(redisSink), "sink-redis-counters").getTransformation());
    }

    public void prepareSink(RedisSink sink) throws InterruptedException {
        sink.initConnection();
        sink.sendCommand(RedisSink.ping());
        if (wipe) {
            sink.sendCommand(RedisSink.flush());
        }

        FluentIterable<CountKey> keys = FluentIterable.of(
                GLOBAL_EVENT_DEVICE_KEY,
                CONTENT_EVENT_DEVICE_KEY,
                LOG_USER_EVENT_KEY,
                USER_EVENT_KEY,
                LAST_LOG_USER_CONTENT_KEY,
                LAST_USER_CONTENT_KEY);
        if (queryCounts) {
            keys = keys.append(
                    QUERY_EVENT_KEY,
                    CONTENT_QUERY_EVENT_KEY);
        }
        if (lastUserQueryEvents) {
            keys = keys.append(
                LAST_LOG_USER_QUERY_KEY,
                LAST_USER_QUERY_KEY);
        }
        for (RedisSink.Command cmd : createMetadataCommands(keys)) {
            sink.sendCommand(cmd);
        }

        sink.closeConnection(true);
    }

    private ImmutableList<RedisSink.Command> createMetadataCommands(Iterable<CountKey> keys) {
        ImmutableList.Builder<RedisSink.Command> builder = ImmutableList.builder();
        for (CountKey key : keys) {
            builder.add(RedisSink.hset(Tuple1.of(CounterKeys.ROW_FORMAT_KEY), Tuple1.of(key.getName()), key.getRowFormat(), -1));
            builder.add(RedisSink.hset(Tuple1.of(CounterKeys.FEATURE_IDS_KEY), Tuple1.of(key.getName()), CSV.join(key.getFeatureIds()), -1));
        }
        return builder.build();
    }

    // TODO: before doing prod-prod deployment, we need to resolve on what the final aggregation
    // strategy and subsequently the redis encoding we need.  This particular job graph should NEVER
    // be expected to incrementally restart as-is.
    <KEY> DataStream<RedisSink.Command> countJoinedEvents(
            JoinedEventCountKey<KEY> countKey,
            SingleOutputStreamOperator<JoinedEvent> luidEvents,
            Duration hourlyEmitWindow) {
        String name = countKey.getName();

        String toTinyUid = "to-tiny-event-" + name;
        // Tuple3<key, logTimestamp, actionCount>.
        DataStream<Tuple3<KEY, Long, Long>> tinyEvents = add(
                luidEvents.map(
                        event -> Tuple3.of(countKey.getKey(event), FlatUtil.getEventApiTimestamp(event), CounterUtil.getCount(event)),
                        Types.TUPLE(countKey.getTypeInfo(), Types.LONG, Types.LONG)),
                toTinyUid);
        KeyedStream<Tuple3<KEY, Long, Long>, KEY> keyedEvents =
            tinyEvents.keyBy(in -> in.f0, countKey.getTypeInfo());

        // Tuple4<key, bucket, count, expiry>
        String hourlyUid = "count-hourly-event-" + name;
        SingleOutputStreamOperator<Tuple4<KEY, Integer, Long, Integer>> hourlyEventCounts = add(
                keyedEvents.process(new SlidingHourlyCounter<>(
                        countKey.getTypeInfo(),
                        hourlyEmitWindow,
                        event -> event.f1,
                        event -> event.f2,
                        s3FileOutput.sideOutputDebugLogging)),
                hourlyUid);
        addSinkTransformation(s3FileOutput.outputDebugLogging(hourlyUid,
                hourlyEventCounts.getSideOutput(SlidingHourlyCounter.LOGGING_TAG),
                SlidingHourlyCounter.LOG_SCHEMA));

        // Tuple4<key, bucket, count, expiry>
        String dailyUid = "count-daily-event-" + name;
        SingleOutputStreamOperator<Tuple4<KEY, Integer, Long, Integer>> dailyEventCounts = add(
                keyedEvents.process(new SlidingDailyCounter<>(
                        countKey.getTypeInfo(),
                        event -> event.f1,
                        event -> event.f2,
                        s3FileOutput.sideOutputDebugLogging)),
                dailyUid);
        addSinkTransformation(s3FileOutput.outputDebugLogging(dailyUid,
                dailyEventCounts.getSideOutput(SlidingDailyCounter.LOGGING_TAG),
                SlidingDailyCounter.LOG_SCHEMA));

        return add(hourlyEventCounts.map(in -> countKey.map(in, "h")), "hset-hourly-event-" + name)
            .union(add(dailyEventCounts.map(in -> countKey.map(in, "d")), "hset-daily-event-" + name));
    }

    // TODO: refactor with countJoinedEvents
    <KEY> DataStream<RedisSink.Command> countQueryJoinedEvents(
            QueryEventCountKey<KEY> countKey,
            SingleOutputStreamOperator<JoinedEvent> luidEvents,
            KeyedStream<Long, Long> emitQueries,
            Duration hourlyEmitWindow) {
        String name = countKey.getName();

        String toTinyUid = "to-tiny-event-" + name;
        // Tuple3<key, logTimestamp, actionCount>.
        DataStream<Tuple3<KEY, Long, Long>> tinyEvents = add(
                luidEvents.map(
                        event -> Tuple3.of(countKey.getKey(event), FlatUtil.getEventApiTimestamp(event), CounterUtil.getCount(event)),
                        Types.TUPLE(countKey.getTypeInfo(), Types.LONG, Types.LONG)),
                toTinyUid);
        KeyedStream<Tuple3<KEY, Long, Long>, KEY> keyedEvents =
            tinyEvents.keyBy(in -> in.f0, countKey.getTypeInfo());

        // Tuple4<key, bucket, count, expiry>
        String dailyUid = "count-daily-event-" + name;
        SlidingDailyCounter<KEY, Tuple3<KEY, Long, Long>> dailyCounter = new SlidingDailyCounter<>(
                countKey.getTypeInfo(),
                event -> event.f1,
                event -> event.f2,
                s3FileOutput.sideOutputDebugLogging);
        SingleOutputStreamOperator<Tuple4<KEY, Integer, Long, Integer>> dailyEventCounts =
            add(keyedEvents.process(dailyCounter), dailyUid);
        addSinkTransformation(s3FileOutput.outputDebugLogging(dailyUid,
                dailyEventCounts.getSideOutput(SlidingDailyCounter.LOGGING_TAG),
                SlidingDailyCounter.LOG_SCHEMA));

        dailyEventCounts = add(
            dailyEventCounts
                .keyBy(in -> countKey.getQueryHash(in.f0))
                .connect(emitQueries)
                .process(new RightSeenOutput<>(dailyCounter.getProducedType(), Types.LONG, left -> countKey.getQueryHash(left.f0))),
            "top-filter-" + dailyUid);

        DataStream<RedisSink.Command> queryCountCommands =
            add(dailyEventCounts.map(in -> countKey.map(in, "d")), "hset-daily-event-" + name);

        // Tuple4<key, bucket, count, expiry>
        if (hourlyQuery) {
            String hourlyUid = "count-hourly-event-" + name;
            SlidingHourlyCounter<KEY, Tuple3<KEY, Long, Long>> hourlyCounter =
                new SlidingHourlyCounter<>(
                    countKey.getTypeInfo(),
                    hourlyEmitWindow,
                    event -> event.f1,
                    event -> event.f2,
                    s3FileOutput.sideOutputDebugLogging);
            SingleOutputStreamOperator<Tuple4<KEY, Integer, Long, Integer>> hourlyEventCounts =
                add(keyedEvents.process(hourlyCounter), hourlyUid);
            addSinkTransformation(s3FileOutput.outputDebugLogging(hourlyUid,
                    hourlyEventCounts.getSideOutput(SlidingHourlyCounter.LOGGING_TAG),
                    SlidingHourlyCounter.LOG_SCHEMA));

            hourlyEventCounts = add(
                hourlyEventCounts
                    .keyBy(in -> countKey.getQueryHash(in.f0))
                    .connect(emitQueries)
                    .process(new RightSeenOutput<>(hourlyCounter.getProducedType(), Types.LONG, left -> countKey.getQueryHash(left.f0))),
                "top-filter-" + hourlyUid);

            queryCountCommands = queryCountCommands.union(
                    add(hourlyEventCounts.map(in -> countKey.map(in, "h")), "hset-hourly-event-" + name));
        }

        return queryCountCommands;
    }

    // TODO: as discussed w/ jun, we probably want this to be unqualifed events, but keeping it qualified for now.
    DataStream<RedisSink.Command> userEventLastTimeAndCount(LastUserContentKey lastUserKey, SingleOutputStreamOperator<JoinedEvent> luidEvents) {
        String uid = lastUserKey.getName();

        String toTinyUid = "to-tiny-event-" + uid;
        // Tuple2<key, logTimestamp>.
        DataStream<Tuple2<Tuple4<Long, String, String, String>, Long>> tinyEvents = add(
                luidEvents.map(
                        event -> Tuple2.of(lastUserKey.getKey(event), FlatUtil.getEventApiTimestamp(event)),
                        Types.TUPLE(lastUserKey.getTypeInfo(), Types.LONG)),
                toTinyUid);

        // Tuple4<key, last timestamp, 90d count, expiry>
        SingleOutputStreamOperator<Tuple4<Tuple4<Long, String, String, String>, Long, Long, Integer>> lastTimeAndCount = add(tinyEvents
                .keyBy(in -> in.f0, lastUserKey.getTypeInfo())
                .process(
                    new LastTimeAndCount<>(
                        lastUserKey.getTypeInfo(),
                        event -> event.f1,
                        lastTimeAndCountEmitWindow,
                        s3FileOutput.sideOutputDebugLogging)),
                uid);
        addSinkTransformation(s3FileOutput.outputDebugLogging(
                uid,
                lastTimeAndCount.getSideOutput(LastTimeAndCount.LOGGING_TAG),
                LastTimeAndCount.LOG_SCHEMA));

        // platform_id, log_user_id, other_id (see mapTimestamp and mapCount90d) -> {fid -> value}
        return add(lastTimeAndCount.map(in -> lastUserKey.mapTimestamp(in)), "hset-" + uid + "-timestamp")
            .union(add(lastTimeAndCount.map(in -> lastUserKey.mapCount90d(in)), "hset-" + uid + "-count90d"));
    }

    // TODO: refactor with userEventLastTimeAndCount
    DataStream<RedisSink.Command> userQueryLastTimeAndCount(
            LastUserQueryKey lastUserKey,
            SingleOutputStreamOperator<JoinedEvent> luidEvents,
            KeyedStream<Long, Long> emitQueries) {
        String uid = lastUserKey.getName();

        String toTinyUid = "to-tiny-event-" + uid;
        // Tuple2<key, logTimestamp>.
        DataStream<Tuple2<Tuple4<Long, String, Long, String>, Long>> tinyEvents = add(
                luidEvents.map(
                        event -> Tuple2.of(lastUserKey.getKey(event), FlatUtil.getEventApiTimestamp(event)),
                        Types.TUPLE(lastUserKey.getTypeInfo(), Types.LONG)),
                toTinyUid);

        // Tuple4<key, last timestamp, 90d count, expiry>
        LastTimeAndCount<Tuple4<Long, String, Long, String>, Tuple2<Tuple4<Long, String, Long, String>, Long>> counter = new LastTimeAndCount<>(
                lastUserKey.getTypeInfo(),
                event -> event.f1,
                lastTimeAndCountEmitWindow,
                s3FileOutput.sideOutputDebugLogging);
        SingleOutputStreamOperator<Tuple4<Tuple4<Long, String, Long, String>, Long, Long, Integer>> lastTimeAndCount = add(tinyEvents
                .keyBy(in -> in.f0, lastUserKey.getTypeInfo())
                .process(counter),
                uid);
        addSinkTransformation(s3FileOutput.outputDebugLogging(
                uid,
                lastTimeAndCount.getSideOutput(LastTimeAndCount.LOGGING_TAG),
                LastTimeAndCount.LOG_SCHEMA));

        // Threshold filter
        lastTimeAndCount = add(
            lastTimeAndCount
                .keyBy(in -> LastUserQueryKey.getQueryHash(in.f0))
                .connect(emitQueries)
                .process(new RightSeenOutput<>(counter.getProducedType(), Types.LONG, left -> LastUserQueryKey.getQueryHash(left.f0))),
            "top-filter-" + uid);

        // platform_id, log_user_id, other_id (see mapTimestamp and mapCount90d) -> {fid -> value}
        return add(lastTimeAndCount.map(in -> lastUserKey.mapTimestamp(in)), "hset-" + uid + "-timestamp")
            .union(add(lastTimeAndCount.map(in -> lastUserKey.mapCount90d(in)), "hset-" + uid + "-count90d"));
    }
}
