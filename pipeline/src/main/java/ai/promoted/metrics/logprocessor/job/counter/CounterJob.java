package ai.promoted.metrics.logprocessor.job.counter;

import static ai.promoted.metrics.logprocessor.common.counter.CounterKeys.CountKey;
import static ai.promoted.metrics.logprocessor.common.counter.CounterKeys.JoinedEventCountKey;
import static ai.promoted.metrics.logprocessor.common.counter.CounterKeys.QueryEventCountKey;
import static com.google.common.base.Preconditions.checkState;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.logprocessor.common.counter.CounterKeys;
import ai.promoted.metrics.logprocessor.common.counter.CounterKeys.BaseLastUserQueryKey;
import ai.promoted.metrics.logprocessor.common.counter.CounterKeys.LastUserEventKey;
import ai.promoted.metrics.logprocessor.common.counter.LastTimeAggResult;
import ai.promoted.metrics.logprocessor.common.counter.UserEventRedisHashSupplier;
import ai.promoted.metrics.logprocessor.common.counter.WindowAggResult;
import ai.promoted.metrics.logprocessor.common.functions.CounterUtil;
import ai.promoted.metrics.logprocessor.common.functions.LastTimeAndCount;
import ai.promoted.metrics.logprocessor.common.functions.RightSeenOutput;
import ai.promoted.metrics.logprocessor.common.functions.SlidingCounter;
import ai.promoted.metrics.logprocessor.common.functions.SlidingDailyCounter;
import ai.promoted.metrics.logprocessor.common.functions.SlidingHourlyCounter;
import ai.promoted.metrics.logprocessor.common.functions.SlidingSixDayCounter;
import ai.promoted.metrics.logprocessor.common.functions.TemporalJoinFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisSink.RedisSinkCommand;
import ai.promoted.metrics.logprocessor.common.functions.sink.RedisStandaloneSink;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.DirectFlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.KeepFirstSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.queryablestate.StreamGraphRewriter;
import ai.promoted.metrics.logprocessor.common.util.CachingSupplier;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Flink job that counts along various dimensions.
 *
 * <p>Currently, the output of this job is directed to a redis instance holding intermediary
 * aggregates accumulated here.
 */
@CommandLine.Command(
    name = "counter",
    mixinStandardHelpOptions = true,
    version = "counter 1.0.0",
    description = "Creates a Flink job that count JoinedEvents from Kafka and outputs to")
public class CounterJob extends BaseFlinkJob {
  public static final Joiner CSV = Joiner.on(",");
  private static final Logger LOGGER = LogManager.getLogger(CounterJob.class);
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final FlatOutputKafkaSegment flatOutputKafkaSegment =
      new FlatOutputKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectFlatOutputKafkaSource directFlatOutputKafkaSource =
      new DirectFlatOutputKafkaSource(kafkaSourceSegment, flatOutputKafkaSegment);

  @CommandLine.Mixin public final KeepFirstSegment keepFirstSegment = new KeepFirstSegment(this);

  private final FlatOutputKafkaSource flatOutputKafkaSource =
      new FlatOutputKafkaSource(
          directFlatOutputKafkaSource, flatOutputKafkaSegment, keepFirstSegment);

  public final ValidatedEventKafkaSegment validatedEventKafkaSegment =
      new ValidatedEventKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectValidatedEventKafkaSource directValidatedEventKafkaSource =
      new DirectValidatedEventKafkaSource(kafkaSourceSegment, validatedEventKafkaSegment);

  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);
  @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this, s3);

  @Option(
      names = {"--countKey"},
      description = "The CountKeys to run in this job.  Defaults to empty (which runs all)")
  public Set<String> countKeys = new HashSet<>();

  @Option(
      names = {"--excludeCountKey"},
      description = "List of CountKeys to exclude.  Defaults to empty")
  public Set<String> excludeCountKeys = new HashSet<>();

  @Option(
      names = {"--no-hourlyQuery"},
      negatable = true,
      description = "Disable hourly query-related counters.  " + "Default=true.")
  public boolean hourlyQuery = true;

  @Option(
      names = {"--counterOutputStartTimestamp"},
      required = true,
      description =
          "Required.  Timestamp to start "
              + "outputting counts.  For non-backfill jobs, this should be set to the timestamp that the job was "
              + "check/savepointed at.  For backfill, this value should be 0 to capture the evolving state.")
  public long counterOutputStartTimestamp;

  @Option(
      names = {"--counterBackfillBuffer", "--counterResultsBuffer"},
      defaultValue = "PT6H",
      description =
          "The time duration to buffer output writes. "
              + "When a Redis command is delayed for more than 2 times of the buffer size, it will be buffered."
              + "Used for backfills or delayed jobs. Setting this to PT0S to disable buffering. Defaults to PT6H.")
  public Duration counterResultsBuffer = Duration.ofHours(6);

  @Option(
      names = {"--count90d"},
      defaultValue = "false",
      description = "Support 90d counters")
  public boolean count90d = false;

  @Option(
      names = {"--globalEventDeviceHourlyWindowSlide"},
      defaultValue = "PT15M",
      description =
          "The window slide "
              + "for globalEventDevice hourly operator.  Default=PT15M.  Java8 Duration parse format.")
  public Duration globalEventDeviceHourlyWindowSlide = Duration.parse("PT15M");

  @Option(
      names = {"--contentEventDeviceHourlyWindowSlide"},
      defaultValue = "PT15M",
      description =
          "The window slide "
              + "for contentEventDevice hourly operator.  Default=PT15M.  Java8 Duration parse format.")
  public Duration contentEventDeviceHourlyWindowSlide = Duration.parse("PT15M");

  @Option(
      names = {"--queryEventHourlyWindowSlide"},
      defaultValue = "PT15M",
      description =
          "The window slide for "
              + "query-related hourly operators.  Default=PT15M.  Java8 Duration parse format.")
  public Duration queryEventHourlyWindowSlide = Duration.parse("PT15M");

  @Option(
      names = {"--logUserEventHourlyWindowSlide"},
      defaultValue = "PT15M",
      description =
          "The window slide for "
              + "logUserEvent hourly operator.  Default=PT15M.  Java8 Duration parse format.")
  public Duration logUserEventHourlyWindowSlide = Duration.parse("PT15M");

  @Option(
      names = {"--userEventHourlyWindowSlide"},
      defaultValue = "PT15M",
      description =
          "The window slide for "
              + "userEvent hourly operator.  Default=PT15M.  Java8 Duration parse format.")
  public Duration userEventHourlyWindowSlide = Duration.parse("PT15M");

  @Option(
      names = {"--logUserLastTimeAndCountTtl"},
      defaultValue = "P90D",
      description =
          "The TTL for logUser lastTimeAndCount operators.  WARNING: This truncates the window for the counts in Flink and Redis.  Be careful modifying.  Default=P90D.  Java8 Duration parse format.")
  public Duration logUserLastTimeAndCountTtl = Duration.parse("P90D");

  @Option(
      names = {"--userLastTimeAndCountTtl"},
      defaultValue = "P90D",
      description =
          "The TTL for user lastTimeAndCount operators.  WARNING: This truncates the window for the counts in Flink and Redis.  Be careful modifying.  Default=P90D.  Java8 Duration parse format.")
  public Duration userLastTimeAndCountTtl = Duration.parse("P90D");

  @Option(
      names = {"--searchQueryLengthLimit"},
      defaultValue = "100",
      description = "The maximum query length to allow as a top query.  Defaults to 100.")
  public int searchQueryLengthLimit = 100;

  @Option(
      names = {"--searchQueryWindowSize"},
      defaultValue = "P14D",
      description =
          "The sliding window size for "
              + "search query ranking.  Default=P14D.  Java8 Duration parse format")
  public Duration searchQueryWindowSize = Duration.ofDays(14);

  @Option(
      names = {"--searchQueryWindowSlide"},
      defaultValue = "P3D",
      description =
          "The sliding window slide for "
              + "search query ranking.  Default=P3D.  Java8 Duration parse format")
  public Duration searchQueryWindowSlide = Duration.ofDays(3);

  @Option(
      names = {"--searchQueryWindowMinThreshold"},
      defaultValue = "1000",
      description =
          "The (inclusive) " + "minimum query frequency to keep counts for.  Defaults to 1000.")
  public int searchQueryWindowMinThreshold = 1000;

  @Option(
      names = {"--wipe"},
      negatable = true,
      description =
          "Wipe redis endpoint before writing, should probably "
              + "be used for backfills.  ONLY USE FOR BACKFILLS.  Defaults to false.")
  public boolean wipe = false;

  @Option(
      names = {"--shardedEndpoints"},
      defaultValue = "",
      description =
          "Endpoint URIs for sharded counter 'service' (e.g. redis://host1:6399,host2:6399,...).  "
              + "Default=''")
  String shardedEndpoints = "";

  @Option(
      names = {"--unshardedEndpoint", "--counterService"},
      defaultValue = "",
      description =
          "Endpoint URI for non-sharded counter 'service' (e.g. redis://localhost:6399/0).  Default=''")
  String unshardedEndpoint = "";

  @Option(
      names = {"--counterOutputStopTimestamp"},
      defaultValue = "-1",
      description =
          "Timestamp to stop "
              + "outputting counts.  Negative values will be interpreted as Long.MAX_VALUE (290M+ years in the future).  "
              + "Defaults to -1.")
  long counterOutputStopTimestamp = -1;

  @Option(
      names = {"--temporalJoinFirstDimensionDelay"},
      defaultValue = "PT15M",
      description =
          "The allowed delay for the first dimension row in temporal join. Fact events will wait more time before seeing the first dimension row.  "
              + "Defaults to PT15M.")
  Duration temporalJoinFirstDimensionDelay = Duration.parse("PT15M");

  @Option(
      names = {"--enableQueryableState"},
      negatable = true,
      description = "Whether to enable queryable state. Default = false")
  public boolean enableQueryableState = false;

  public static void main(String[] args) {
    executeMain(new CounterJob(), args);
  }

  // For now, just use JoinedImpressions.
  // Make a static function to avoid unnecessary serialization.
  private static KeyedStream<Long, Long> getTopQueriesStream(
      SingleOutputStreamOperator<JoinedImpression> logUserJoinImpressions,
      SingleOutputStreamOperator<AttributedAction> logUserAttributedActions,
      CounterJob job) {

    SingleOutputStreamOperator<Long> impressionQueryHashes =
        toQueryHash(
            logUserJoinImpressions,
            "impression",
            FlatUtil::getQueryStringLowered,
            job.searchQueryLengthLimit,
            job);
    SingleOutputStreamOperator<Long> actionQueryHashes =
        toQueryHash(
            logUserAttributedActions,
            "action",
            FlatUtil::getQueryStringLowered,
            job.searchQueryLengthLimit,
            job);

    int searchQueryWindowMinThreshold = job.searchQueryWindowMinThreshold;
    return impressionQueryHashes
        .union(actionQueryHashes)
        .keyBy(h -> h)
        .window(
            SlidingEventTimeWindows.of(
                Time.milliseconds(job.searchQueryWindowSize.toMillis()),
                Time.milliseconds(job.searchQueryWindowSlide.toMillis())))
        .aggregate(
            new AggregateFunction<Long, Long, Long>() {
              @Override
              public Long createAccumulator() {
                return 0L;
              }

              @Override
              public Long add(Long in, Long sum) {
                // Q - Is this because we're counting and we want to ignore the value?
                sum += 1;
                return sum;
              }

              @Override
              public Long getResult(Long sum) {
                return sum;
              }

              @Override
              public Long merge(Long a, Long b) {
                return a + b;
              }
            },
            new ProcessWindowFunction<Long, Long, Long, TimeWindow>() {
              @Override
              public void process(Long in, Context ctx, Iterable<Long> sums, Collector<Long> out) {
                // TODO: remove when comfortable w/ behavior
                checkState(Iterables.size(sums) == 1);
                if (sums.iterator().next() >= searchQueryWindowMinThreshold) {
                  LOGGER.trace("{} emit: {}", ctx.window().maxTimestamp(), in);
                  out.collect(in);
                }
              }
            })
        .uid("emit-top-queries")
        .name("emit-top-queries")
        .keyBy(h -> h);
  }

  private static <T> SingleOutputStreamOperator<Long> toQueryHash(
      SingleOutputStreamOperator<T> events,
      String eventName,
      MapFunction<T, String> toQuery,
      long searchQueryLengthLimit,
      CounterJob job) {
    SingleOutputStreamOperator<String> queries =
        job.add(events.map(toQuery), "lower-" + eventName + "-query-string");
    queries =
        job.add(
            queries.filter(s -> !StringUtil.isBlank(s) && s.length() <= searchQueryLengthLimit),
            "length-filter-" + eventName + "-query");
    return job.add(queries.map(FlatUtil::getQueryHash), "hash-" + eventName + "-query");
  }

  /** Enriches join event stream with logUser-user event stream by a temporal join. */
  private SingleOutputStreamOperator<JoinedImpression> enrichJoinedImpressionsWithLogUserUser(
      DataStream<JoinedImpression> joinedImpressionStream,
      KeyedStream<LogUserUser, Tuple2<Long, String>> logUserUserByLogUserId,
      CounterJob job) {
    return job.add(
        joinedImpressionStream
            .keyBy(
                new KeySelector<JoinedImpression, Tuple2<Long, String>>() {
                  @Override
                  public Tuple2<Long, String> getKey(JoinedImpression joinedImpression) {
                    return Tuple2.of(
                        joinedImpression.getIds().getPlatformId(),
                        joinedImpression.getIds().getLogUserId());
                  }
                })
            .connect(logUserUserByLogUserId)
            .process(
                new TemporalJoinFunction<>(
                    TypeInformation.of(JoinedImpression.class),
                    TypeInformation.of(LogUserUser.class),
                    (SerializableBiFunction<JoinedImpression, LogUserUser, JoinedImpression>)
                        (joinedEvent, logUserUser) -> {
                          JoinedImpression.Builder builder = joinedEvent.toBuilder();
                          builder.getIdsBuilder().setUserId(logUserUser.getUserId());
                          return builder.build();
                        },
                    true,
                    null,
                    temporalJoinFirstDimensionDelay.toMillis()))
            .returns(TypeInformation.of(JoinedImpression.class)),
        "enrich-joined-impression-with-log-user-user");
  }

  /** Enriches join event stream with logUser-user event stream by a temporal join. */
  private SingleOutputStreamOperator<AttributedAction> enrichAttributedActionsWithLogUserUser(
      DataStream<AttributedAction> attributedActionStream,
      KeyedStream<LogUserUser, Tuple2<Long, String>> logUserUserByLogUserId,
      CounterJob job) {
    return job.add(
        attributedActionStream
            .keyBy(
                new KeySelector<AttributedAction, Tuple2<Long, String>>() {
                  @Override
                  public Tuple2<Long, String> getKey(AttributedAction attributedAction) {
                    return new Tuple2<>(
                        attributedAction.getAction().getPlatformId(),
                        attributedAction.getAction().getUserInfo().getLogUserId());
                  }
                })
            .connect(logUserUserByLogUserId)
            .process(
                new TemporalJoinFunction<>(
                    TypeInformation.of(AttributedAction.class),
                    TypeInformation.of(LogUserUser.class),
                    (SerializableBiFunction<AttributedAction, LogUserUser, AttributedAction>)
                        (attributedAction, logUserUser) -> {
                          AttributedAction.Builder builder = attributedAction.toBuilder();
                          // CounterKeys looks at this field for auth userID counts.
                          builder
                              .getTouchpointBuilder()
                              .getJoinedImpressionBuilder()
                              .getIdsBuilder()
                              .setUserId(logUserUser.getUserId());
                          return builder.build();
                        },
                    true,
                    null,
                    temporalJoinFirstDimensionDelay.toMillis()))
            .returns(TypeInformation.of(AttributedAction.class)),
        "enrich-attributed-action-with-log-user-user");
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        flatOutputKafkaSegment,
        directFlatOutputKafkaSource,
        flatOutputKafkaSource,
        validatedEventKafkaSegment,
        directValidatedEventKafkaSource,
        s3,
        s3FileOutput);
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    if (StringUtil.isBlank(shardedEndpoints) && StringUtil.isBlank(unshardedEndpoint)) {
      throw new IllegalArgumentException("--shardedEndpoints or --unshardedEndpoint must be given");
    }
    Preconditions.checkArgument(
        countKeys.isEmpty() || excludeCountKeys.isEmpty(),
        "Both --countKey and --excludeCountKey cannot both be specified");
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);

    // TODO: make this pattern as part of BaseFlinkJob.
    StreamGraph graph =
        defineJob(
            flatOutputKafkaSource.getJoinedImpressionSource(
                env, toKafkaConsumerGroupId("countEvents")),
            flatOutputKafkaSource.getAttributedActionSource(
                env, toKafkaConsumerGroupId("countEvents")),
            directValidatedEventKafkaSource.getLogUserUserSource(
                env, toKafkaConsumerGroupId("countEvents-user")),
            newRedisSink());
    env.execute(graph);
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "counter";
  }

  private RedisSink newRedisSink() {
    return StringUtil.isBlank(shardedEndpoints)
        ? new RedisStandaloneSink(unshardedEndpoint, createMetadataCommands())
        : new RedisSink(shardedEndpoints, createMetadataCommands());
  }

  public StreamGraph defineJob(
      SingleOutputStreamOperator<JoinedImpression> inputLogUserJoinImpressions,
      SingleOutputStreamOperator<AttributedAction> inputLogUserAllAttributedActions,
      SingleOutputStreamOperator<LogUserUser> logUserUserEvents,
      SinkFunction<RedisSinkCommand> redisSink) {

    // Suppliers are used so avoid initializing sources that are not used (depending on flags).

    // For now, limit AttributedActions down to the "last touchpoint" model.
    SingleOutputStreamOperator<AttributedAction> logUserAttributedAction =
        add(
            inputLogUserAllAttributedActions.filter(
                attributedAction -> attributedAction.getAttribution().getModelId() == 1L),
            "filter-to-last-touchpoint-attribution");

    Supplier<KeyedStream<LogUserUser, Tuple2<Long, String>>> logUserUserByLogUserIdSupplier =
        new CachingSupplier<>() {
          @Override
          public KeyedStream<LogUserUser, Tuple2<Long, String>> supplyValue() {
            return logUserUserEvents.keyBy(
                new KeySelector<LogUserUser, Tuple2<Long, String>>() {
                  @Override
                  public Tuple2<Long, String> getKey(LogUserUser logUserUser) {
                    return new Tuple2<>(logUserUser.getPlatformId(), logUserUser.getLogUserId());
                  }
                });
          }
        };

    Supplier<SingleOutputStreamOperator<JoinedImpression>> joinImpressionsWithUser =
        new CachingSupplier<>() {
          @Override
          public SingleOutputStreamOperator<JoinedImpression> supplyValue() {
            return enrichJoinedImpressionsWithLogUserUser(
                inputLogUserJoinImpressions, logUserUserByLogUserIdSupplier.get(), CounterJob.this);
          }
        };

    Supplier<SingleOutputStreamOperator<AttributedAction>> joinActionsWithUser =
        new CachingSupplier<>() {
          @Override
          public SingleOutputStreamOperator<AttributedAction> supplyValue() {
            return enrichAttributedActionsWithLogUserUser(
                logUserAttributedAction, logUserUserByLogUserIdSupplier.get(), CounterJob.this);
          }
        };

    Supplier<KeyedStream<Long, Long>> topQueriesStream =
        new CachingSupplier<>() {
          @Override
          public KeyedStream<Long, Long> supplyValue() {
            return getTopQueriesStream(
                inputLogUserJoinImpressions, logUserAttributedAction, CounterJob.this);
          }
        };

    List<DataStream<RedisSinkCommand>> outputStreamList = new ArrayList<>();

    CounterKeys keys = getAllCountKeys();

    if (isEnabled(keys.getGlobalEventDeviceKey())) {
      outputStreamList.add(
          countJoinedImpressions(
              keys.getGlobalEventDeviceKey(),
              inputLogUserJoinImpressions,
              globalEventDeviceHourlyWindowSlide));
      outputStreamList.add(
          countAttributedActions(
              keys.getGlobalEventDeviceKey(),
              logUserAttributedAction,
              globalEventDeviceHourlyWindowSlide));
    }

    if (isEnabled(keys.getContentEventDeviceKey())) {
      outputStreamList.add(
          countJoinedImpressions(
              keys.getContentEventDeviceKey(),
              inputLogUserJoinImpressions,
              contentEventDeviceHourlyWindowSlide));
      outputStreamList.add(
          countAttributedActions(
              keys.getContentEventDeviceKey(),
              logUserAttributedAction,
              contentEventDeviceHourlyWindowSlide));
    }

    if (isEnabled(keys.getLogUserEventKey())) {
      outputStreamList.add(
          countJoinedImpressions(
              keys.getLogUserEventKey(),
              inputLogUserJoinImpressions,
              logUserEventHourlyWindowSlide));
      outputStreamList.add(
          countAttributedActions(
              keys.getLogUserEventKey(), logUserAttributedAction, logUserEventHourlyWindowSlide));
    }

    if (isEnabled(keys.getUserEventKey())) {
      outputStreamList.add(
          countJoinedImpressions(
              keys.getUserEventKey(), joinImpressionsWithUser.get(), userEventHourlyWindowSlide));
      outputStreamList.add(
          countAttributedActions(
              keys.getUserEventKey(), joinActionsWithUser.get(), userEventHourlyWindowSlide));
    }

    if (isEnabled(keys.getLastLogUserContentKey())) {
      outputStreamList.add(
          userImpressionLastTimeAndCount(
              keys.getLastLogUserContentKey(),
              inputLogUserJoinImpressions,
              logUserLastTimeAndCountTtl));
      outputStreamList.add(
          userActionLastTimeAndCount(
              keys.getLastLogUserContentKey(),
              logUserAttributedAction,
              logUserLastTimeAndCountTtl));
    }

    if (isEnabled(keys.getLastUserContentKey())) {
      outputStreamList.add(
          userImpressionLastTimeAndCount(
              keys.getLastUserContentKey(),
              joinImpressionsWithUser.get(),
              userLastTimeAndCountTtl));
      outputStreamList.add(
          userActionLastTimeAndCount(
              keys.getLastUserContentKey(), joinActionsWithUser.get(), userLastTimeAndCountTtl));
    }

    if (isEnabled(keys.getQueryEventKey())) {
      outputStreamList.add(
          countQueryJoinedImpressions(
              keys.getQueryEventKey(),
              inputLogUserJoinImpressions,
              topQueriesStream.get(),
              queryEventHourlyWindowSlide));
      outputStreamList.add(
          countQueryAttributedActions(
              keys.getQueryEventKey(),
              logUserAttributedAction,
              topQueriesStream.get(),
              queryEventHourlyWindowSlide));
    }

    if (isEnabled(keys.getContentQueryEventKey())) {
      outputStreamList.add(
          countQueryJoinedImpressions(
              keys.getContentQueryEventKey(),
              inputLogUserJoinImpressions,
              topQueriesStream.get(),
              queryEventHourlyWindowSlide));
      outputStreamList.add(
          countQueryAttributedActions(
              keys.getContentQueryEventKey(),
              logUserAttributedAction,
              topQueriesStream.get(),
              queryEventHourlyWindowSlide));
    }

    if (isEnabled(keys.getLastLogUserQueryKey())) {
      outputStreamList.add(
          userQueryImpressionLastTimeAndCount(
              keys.getLastLogUserQueryKey(),
              inputLogUserJoinImpressions,
              topQueriesStream.get(),
              logUserLastTimeAndCountTtl));
      outputStreamList.add(
          userQueryActionLastTimeAndCount(
              keys.getLastLogUserQueryKey(),
              logUserAttributedAction,
              topQueriesStream.get(),
              logUserLastTimeAndCountTtl));
    }

    if (isEnabled(keys.getLastUserQueryKey())) {
      outputStreamList.add(
          userQueryImpressionLastTimeAndCount(
              keys.getLastUserQueryKey(),
              joinImpressionsWithUser.get(),
              topQueriesStream.get(),
              userLastTimeAndCountTtl));
      outputStreamList.add(
          userQueryActionLastTimeAndCount(
              keys.getLastUserQueryKey(),
              joinActionsWithUser.get(),
              topQueriesStream.get(),
              userLastTimeAndCountTtl));
    }

    Preconditions.checkArgument(
        !outputStreamList.isEmpty(), "At least one --table needs to be specified.");

    @SuppressWarnings("unchecked")
    DataStream<RedisSinkCommand> outputStream =
        outputStreamList
            .get(0)
            .union(outputStreamList.subList(1, outputStreamList.size()).toArray(new DataStream[0]));

    // Use a dummy sink to replace redisSink if QueryableState is enabled since we don't write Redis
    // in that mode.
    if (enableQueryableState) {
      addSinkTransformation(
          add(outputStream.addSink(new SinkFunction<>() {}), "dummy-counters-sink")
              .getTransformation());
    } else {
      outputStream = filterAndBufferRedisCommands(outputStream);
      addSinkTransformation(
          add(outputStream.addSink(redisSink), "sink-redis-counters").getTransformation());
    }

    StreamGraph graph = outputStream.getExecutionEnvironment().getStreamGraph(false);
    graph.setJobName(getJobName());
    // Rewrite the graph by appending queryable operators to the target queryable operators
    if (enableQueryableState) {
      StreamGraphRewriter.rewriteToAddQueryOperators(
          graph,
          List.of(
              "COUNT-impression-BY-plat_dvc-$evt$hourly",
              "COUNT-impression-BY-plat_dvc-$evt$daily",
              "COUNT-impression-BY-plat-cont_dvc-$evt$hourly",
              "COUNT-impression-BY-plat-cont_dvc-$evt$daily",
              "COUNT-impression-BY-plat-logUsr_$evt$hourly",
              "COUNT-impression-BY-plat-logUsr_$evt$daily",
              "COUNT-impression-BY-plat-usr_$evt$hourly",
              "COUNT-impression-BY-plat-usr_$evt$daily",
              "COUNT-impression-BY-plat-qry_$evt$daily",
              "COUNT-impression-BY-plat-qry_$evt$hourly",
              "COUNT-impression-BY-plat-cont-qry_$evt$daily",
              "COUNT-impression-BY-plat-cont-qry_$evt$hourly",
              "COUNT-action-BY-plat_dvc-$evt$hourly",
              "COUNT-action-BY-plat_dvc-$evt$daily",
              "COUNT-action-BY-plat-cont_dvc-$evt$hourly",
              "COUNT-action-BY-plat-cont_dvc-$evt$daily",
              "COUNT-action-BY-plat-logUsr_$evt$hourly",
              "COUNT-action-BY-plat-logUsr_$evt$daily",
              "COUNT-action-BY-plat-usr_$evt$hourly",
              "COUNT-action-BY-plat-usr_$evt$daily",
              "COUNT-action-BY-plat-qry_$evt$daily",
              "COUNT-action-BY-plat-qry_$evt$hourly",
              "COUNT-action-BY-plat-cont-qry_$evt$daily",
              "COUNT-action-BY-plat-cont-qry_$evt$hourly",
              "LASTTIME-impression-BY-plat-logUsr-cont_$evt",
              "LASTTIME-impression-BY-plat-usr-cont_$evt",
              "LASTTIME-impression-BY-plat-logUsr-qry_$evt",
              "LASTTIME-impression-BY-plat-usr-qry_$evt",
              "LASTTIME-action-BY-plat-logUsr-cont_$evt",
              "LASTTIME-action-BY-plat-usr-cont_$evt",
              "LASTTIME-action-BY-plat-logUsr-qry_$evt",
              "LASTTIME-action-BY-plat-usr-qry_$evt"));
    }
    return graph;
  }

  /**
   * Starts to buffer redis commands if they are delayed for more than (2 * resultBufferMillis).
   *
   * <ol>
   *   Prerequisites:
   *   <li>Commands of the same REDIS_HASH_KEY must be strictly ordered by timestamps (watermark is
   *       perfect).
   *   <li>The commands are idempotent so that we can just output the latest value.
   * </ol>
   */
  @VisibleForTesting
  DataStream<RedisSinkCommand> filterAndBufferRedisCommands(
      DataStream<RedisSinkCommand> outputStream) {
    final long resultsBufferMillis = counterResultsBuffer.toMillis();
    final long outputStartTimestamp = counterOutputStartTimestamp;
    final long outputStopTimestamp =
        (counterOutputStopTimestamp < 0) ? Long.MAX_VALUE : counterOutputStopTimestamp;
    return add(
        outputStream
            .keyBy(RedisSinkCommand.REDIS_HASH_KEY)
            .process(
                new KeyedProcessFunction<>() {
                  private ValueState<RedisSinkCommand> cachedCommand;

                  // No need to store in state. It's fine to reset this to false for each restart.
                  private transient boolean cacheCleared = false;

                  @Override
                  public void open(Configuration config) {
                    cachedCommand =
                        getRuntimeContext()
                            .getState(
                                new ValueStateDescriptor<>(
                                    "cached-redis-command", RedisSinkCommand.class));
                  }

                  /** The incoming commands must be strictly ordered by the timestamps! */
                  @Override
                  public void processElement(
                      RedisSinkCommand in, Context ctx, Collector<RedisSinkCommand> out)
                      throws IOException {
                    long eventTime = ctx.timestamp();
                    // Skip the command if its timestamp is not in the time range.
                    if (outputStartTimestamp <= eventTime && eventTime <= outputStopTimestamp) {
                      // Start to buffer when the command is delayed for more than
                      // 2 times of the buffer size
                      if (resultsBufferMillis > 0
                          && eventTime
                              < ctx.timerService().currentProcessingTime()
                                  - resultsBufferMillis * 2) {
                        cachedCommand.update(in);
                        cacheCleared = false;
                        ctx.timerService().registerEventTimeTimer(toBufferTime(eventTime));
                      } else {
                        // We're going to output a more recent value, so just drop the
                        // cached old value.
                        if (!cacheCleared) {
                          if (cachedCommand.value() != null) {
                            cachedCommand.clear();
                          }
                          cacheCleared = true;
                        }
                        out.collect(in);
                      }
                    }
                  }

                  @Override
                  public void onTimer(long ts, OnTimerContext ctx, Collector<RedisSinkCommand> out)
                      throws IOException {
                    // The cachedCommand may have been cleared by a more recent value.
                    // We won't output an older value in that case.
                    if (cachedCommand.value() != null) {
                      out.collect(cachedCommand.value());
                      cachedCommand.clear();
                      cacheCleared = true;
                    }
                  }

                  private long toBufferTime(long timestamp) {
                    long bufferTime =
                        (timestamp - (timestamp % resultsBufferMillis)) + resultsBufferMillis;
                    // Stagger timers for different tasks to avoid spikes.
                    int partIndex = getRuntimeContext().getIndexOfThisSubtask();
                    if (partIndex > 1) {
                      bufferTime +=
                          (resultsBufferMillis / getRuntimeContext().getNumberOfParallelSubtasks())
                              * partIndex;
                    }
                    return bufferTime;
                  }
                }),
        "buffer-filter-counters-output");
  }

  public void prepareSink(RedisSink sink) throws InterruptedException {
    sink.initConnection();
    sink.sendCommand(RedisSink.ping());
    if (wipe) {
      sink.sendCommand(RedisSink.flush());
    }

    sink.closeConnection(true);
  }

  @VisibleForTesting
  public ImmutableList<RedisSinkCommand> createMetadataCommands() {
    Iterable<CountKey<?>> keys = getEnabledCountKeys();
    ImmutableList.Builder<RedisSinkCommand> builder = ImmutableList.builder();
    for (CountKey<?> key : keys) {
      builder.add(
          RedisSink.hset(
              Tuple1.of(CounterKeys.ROW_FORMAT_KEY),
              Tuple1.of(key.getDeprecatedRedisName()),
              key.getRowFormat(),
              -1));
      builder.add(
          RedisSink.hset(
              Tuple1.of(CounterKeys.FEATURE_IDS_KEY),
              Tuple1.of(key.getDeprecatedRedisName()),
              CSV.join(key.getFeatureIds()),
              -1));
    }
    return builder.build();
  }

  <KEY> DataStream<RedisSinkCommand> countJoinedImpressions(
      JoinedEventCountKey<KEY> countKey,
      SingleOutputStreamOperator<JoinedImpression> events,
      Duration hourlyWindowSlide) {
    return countJoinedEvents(
        "impression",
        countKey,
        countKey::extractKey,
        events,
        hourlyWindowSlide,
        impression -> 1L,
        FlatUtil::getEventApiTimestamp);
  }

  <KEY> DataStream<RedisSinkCommand> countAttributedActions(
      JoinedEventCountKey<KEY> countKey,
      SingleOutputStreamOperator<AttributedAction> events,
      Duration hourlyWindowSlide) {
    return countJoinedEvents(
        "action",
        countKey,
        countKey::extractKey,
        events,
        hourlyWindowSlide,
        CounterUtil::getCount,
        action -> action.getAction().getTiming().getEventApiTimestamp());
  }

  // TODO: before doing prod-prod deployment, we need to resolve on what the final aggregation
  // strategy and subsequently the redis encoding we need.  This particular job graph should NEVER
  // be expected to incrementally restart as-is.
  <EVENT, KEY> DataStream<RedisSinkCommand> countJoinedEvents(
      String eventName,
      JoinedEventCountKey<KEY> countKey,
      SerializableFunction<EVENT, KEY> getKey,
      SingleOutputStreamOperator<EVENT> luidEvents,
      Duration hourlyWindowSlide,
      SerializableToLongFunction<EVENT> getCount,
      SerializableToLongFunction<EVENT> getEventApiTimestamp) {
    String name = "COUNT-" + eventName + "-BY-" + countKey.getName();

    String toTinyUid = "to-tiny-" + name;
    // Tuple3<key, eventApiTimestamp, actionCount>.
    DataStream<Tuple3<KEY, Long, Long>> tinyEvents =
        add(
            luidEvents.map(
                event ->
                    Tuple3.of(
                        getKey.apply(event),
                        getEventApiTimestamp.applyAsLong(event),
                        getCount.applyAsLong(event)),
                Types.TUPLE(countKey.getTypeInfo(), Types.LONG, Types.LONG)),
            toTinyUid);
    KeyedStream<Tuple3<KEY, Long, Long>, KEY> keyedEvents =
        tinyEvents.keyBy(in -> in.f0, countKey.getTypeInfo());
    //    String hourlyUid = "count-hourly-" + hourlyWindowSlide + "-" + name;
    String hourlyUid = name + "$hourly";
    // Tuple4<key, bucket, count, expiry>
    SingleOutputStreamOperator<WindowAggResult<KEY>> hourlyEventCounts =
        add(
            keyedEvents.process(
                new SlidingHourlyCounter<>(
                    enableQueryableState,
                    countKey.getTypeInfo(),
                    hourlyWindowSlide,
                    event -> event.f1,
                    event -> event.f2,
                    s3FileOutput.sideOutputDebugLogging)),
            hourlyUid);
    addSinkTransformation(
        s3FileOutput.outputDebugLogging(
            hourlyUid,
            hourlyEventCounts.getSideOutput(SlidingCounter.LOGGING_TAG),
            SlidingCounter.LOG_SCHEMA));

    // Tuple4<key, bucket, count, expiry>
    //    String dailyUid = "count-daily-" + name;
    String dailyUid = name + "$daily";
    SingleOutputStreamOperator<WindowAggResult<KEY>> dailyEventCounts =
        add(
            keyedEvents.process(
                new SlidingDailyCounter<>(
                    enableQueryableState,
                    countKey.getTypeInfo(),
                    event -> event.f1,
                    event -> event.f2,
                    s3FileOutput.sideOutputDebugLogging)),
            dailyUid);
    addSinkTransformation(
        s3FileOutput.outputDebugLogging(
            dailyUid,
            dailyEventCounts.getSideOutput(SlidingCounter.LOGGING_TAG),
            SlidingCounter.LOG_SCHEMA));

    DataStream<RedisSinkCommand> commands =
        add(hourlyEventCounts.map(countKey), "hourly-" + name + "-to-redis")
            .union(add(dailyEventCounts.map(countKey), "daily-" + name + "-to-redis"));

    if (count90d) {
      // Tuple4<key, bucket, count, expiry>
      //      String sixDayUid = "count-sixday-" + name;
      String sixDayUid = name + "$sixday";
      SingleOutputStreamOperator<WindowAggResult<KEY>> sixDayEventCounts =
          add(
              keyedEvents.process(
                  new SlidingSixDayCounter<>(
                      enableQueryableState,
                      countKey.getTypeInfo(),
                      event -> event.f1,
                      event -> event.f2,
                      s3FileOutput.sideOutputDebugLogging)),
              sixDayUid);
      addSinkTransformation(
          s3FileOutput.outputDebugLogging(
              sixDayUid,
              sixDayEventCounts.getSideOutput(SlidingCounter.LOGGING_TAG),
              SlidingCounter.LOG_SCHEMA));

      commands =
          commands.union(add(sixDayEventCounts.map(countKey), "sixday-" + name + "-to-redis"));
    }

    return commands;
  }

  <KEY> DataStream<RedisSinkCommand> countQueryJoinedImpressions(
      QueryEventCountKey<KEY> countKey,
      SingleOutputStreamOperator<JoinedImpression> impressions,
      KeyedStream<Long, Long> emitQueries,
      Duration hourlyWindowSlide) {
    return countQueryJoinedEvents(
        "impression",
        countKey,
        countKey::extractKey,
        impressions,
        emitQueries,
        hourlyWindowSlide,
        (ignored) -> 1L,
        FlatUtil::getEventApiTimestamp);
  }

  <KEY> DataStream<RedisSinkCommand> countQueryAttributedActions(
      QueryEventCountKey<KEY> countKey,
      SingleOutputStreamOperator<AttributedAction> actions,
      KeyedStream<Long, Long> emitQueries,
      Duration hourlyWindowSlide) {
    return countQueryJoinedEvents(
        "action",
        countKey,
        countKey::extractKey,
        actions,
        emitQueries,
        hourlyWindowSlide,
        CounterUtil::getCount,
        action -> action.getAction().getTiming().getEventApiTimestamp());
  }

  // TODO: refactor with countJoinedEvents
  <EVENT, KEY> DataStream<RedisSinkCommand> countQueryJoinedEvents(
      String eventName,
      QueryEventCountKey<KEY> countKey,
      SerializableFunction<EVENT, KEY> getKey,
      SingleOutputStreamOperator<EVENT> luidEvents,
      KeyedStream<Long, Long> emitQueries,
      Duration hourlyWindowSlide,
      SerializableToLongFunction<EVENT> getCount,
      SerializableToLongFunction<EVENT> getEventApiTimestamp) {
    String name = "COUNT-" + eventName + "-BY-" + countKey.getName();

    String toTinyUid = "to-tiny-" + name;
    // Tuple3<key, eventApiTimestamp, actionCount>.
    DataStream<Tuple3<KEY, Long, Long>> tinyEvents =
        add(
            luidEvents.map(
                event ->
                    Tuple3.of(
                        getKey.apply(event),
                        getEventApiTimestamp.applyAsLong(event),
                        getCount.applyAsLong(event)),
                Types.TUPLE(countKey.getTypeInfo(), Types.LONG, Types.LONG)),
            toTinyUid);
    KeyedStream<Tuple3<KEY, Long, Long>, KEY> keyedEvents =
        tinyEvents.keyBy(in -> in.f0, countKey.getTypeInfo());

    // Tuple4<key, bucket, count, expiry>
    //    String dailyUid = "count-daily-" + name;
    String dailyUid = name + "$daily";
    SlidingCounter<KEY, Tuple3<KEY, Long, Long>> dailyCounter =
        new SlidingDailyCounter<>(
            enableQueryableState,
            countKey.getTypeInfo(),
            event -> event.f1,
            event -> event.f2,
            s3FileOutput.sideOutputDebugLogging);
    SingleOutputStreamOperator<WindowAggResult<KEY>> dailyEventCounts =
        add(keyedEvents.process(dailyCounter), dailyUid);
    addSinkTransformation(
        s3FileOutput.outputDebugLogging(
            dailyUid,
            dailyEventCounts.getSideOutput(SlidingCounter.LOGGING_TAG),
            SlidingCounter.LOG_SCHEMA));

    dailyEventCounts =
        add(
            dailyEventCounts
                .keyBy(in -> countKey.getQueryHash(in.getKey()))
                .connect(emitQueries)
                .process(
                    new RightSeenOutput<>(
                        dailyCounter.getProducedType(),
                        Types.LONG,
                        left -> countKey.getQueryHash(left.getKey()),
                        left ->
                            countKey.getQueryHash(left.getKey()) == FlatUtil.EMPTY_STRING_HASH)),
            "top-filter-" + dailyUid);

    DataStream<RedisSinkCommand> commands =
        add(dailyEventCounts.map(countKey), "daily-" + name + "-to-redis");

    // TODO - add 90d query counts.

    // Tuple4<key, bucket, count, expiry>
    if (hourlyQuery) {
      // hourlyWindowSlide is included in the uid because changing it causes a breaking change.
      //      String hourlyUid = "count-hourly-" + hourlyWindowSlide + "-" + name;
      String hourlyUid = name + "$hourly";
      SlidingCounter<KEY, Tuple3<KEY, Long, Long>> hourlyCounter =
          new SlidingHourlyCounter<>(
              enableQueryableState,
              countKey.getTypeInfo(),
              hourlyWindowSlide,
              event -> event.f1,
              event -> event.f2,
              s3FileOutput.sideOutputDebugLogging);

      SingleOutputStreamOperator<WindowAggResult<KEY>> hourlyEventCounts =
          add(keyedEvents.process(hourlyCounter), hourlyUid);
      addSinkTransformation(
          s3FileOutput.outputDebugLogging(
              hourlyUid,
              hourlyEventCounts.getSideOutput(SlidingCounter.LOGGING_TAG),
              SlidingCounter.LOG_SCHEMA));

      hourlyEventCounts =
          add(
              hourlyEventCounts
                  .keyBy(in -> countKey.getQueryHash(in.getKey()))
                  .connect(emitQueries)
                  .process(
                      new RightSeenOutput<>(
                          hourlyCounter.getProducedType(),
                          Types.LONG,
                          left -> countKey.getQueryHash(left.getKey()),
                          left ->
                              countKey.getQueryHash(left.getKey()) == FlatUtil.EMPTY_STRING_HASH)),
              "top-filter-" + hourlyUid);

      commands =
          commands.union(add(hourlyEventCounts.map(countKey), "hourly-" + name + "-to-redis"));
    }

    return commands;
  }

  <KEY> DataStream<RedisSinkCommand> userImpressionLastTimeAndCount(
      LastUserEventKey<KEY> lastUserKey,
      SingleOutputStreamOperator<JoinedImpression> logUserJoinedImpressions,
      Duration ttl) {
    return userEventLastTimeAndCount(
        "impression",
        lastUserKey,
        event -> lastUserKey.extractKey(event),
        // TODO - for an unknown reason, the following line causes a serialization error.
        // lastUserKey::getJoinedImpressionKey,
        logUserJoinedImpressions,
        ttl,
        FlatUtil::getEventApiTimestamp);
  }

  <KEY> DataStream<RedisSinkCommand> userActionLastTimeAndCount(
      LastUserEventKey<KEY> lastUserKey,
      SingleOutputStreamOperator<AttributedAction> logUserAttributedActions,
      Duration ttl) {
    return userEventLastTimeAndCount(
        "action",
        lastUserKey,
        event -> lastUserKey.extractKey(event),
        // TODO - for an unknown reason, the following line causes a serialization error.
        // lastUserKey::getAttributedActionKey,
        logUserAttributedActions,
        ttl,
        action -> action.getAction().getTiming().getEventApiTimestamp());
  }
  // TODO: as discussed w/ jun, we probably want this to be unqualifed events, but keeping it
  // qualified for now.
  <EVENT, KEY> DataStream<RedisSinkCommand> userEventLastTimeAndCount(
      String eventName,
      LastUserEventKey<KEY> lastUserKey,
      SerializableFunction<EVENT, KEY> getKey,
      SingleOutputStreamOperator<EVENT> logUserJoinEvents,
      Duration ttl,
      SerializableToLongFunction<EVENT> getEventApiTimestamp) {
    String uid = "LASTTIME-" + eventName + "-BY-" + lastUserKey.getName();

    String toTinyUid = "to-tiny-" + uid;
    // Tuple2<key, eventApiTimestamp>.
    DataStream<Tuple2<KEY, Long>> tinyEvents =
        add(
            logUserJoinEvents.map(
                event -> Tuple2.of(getKey.apply(event), getEventApiTimestamp.applyAsLong(event)),
                Types.TUPLE(lastUserKey.getTypeInfo(), Types.LONG)),
            toTinyUid);

    // Tuple4<key, last timestamp, 90d count, expiry>
    SingleOutputStreamOperator<LastTimeAggResult<KEY>> lastTimeAndCount =
        add(
            tinyEvents
                .keyBy(in -> in.f0, lastUserKey.getTypeInfo())
                .process(
                    new LastTimeAndCount<>(
                        enableQueryableState,
                        lastUserKey.getTypeInfo(),
                        event -> event.f1,
                        ttl,
                        s3FileOutput.sideOutputDebugLogging)),
            uid);
    addSinkTransformation(
        s3FileOutput.outputDebugLogging(
            uid,
            lastTimeAndCount.getSideOutput(LastTimeAndCount.LOGGING_TAG),
            LastTimeAndCount.LOG_SCHEMA));

    // platform_id, log_user_id, other_id (see mapTimestamp and mapCount90d) -> {fid -> value}
    return add(lastTimeAndCount.map(lastUserKey::mapTimestamp), uid + "-timestamp-to-redis")
        .union(add(lastTimeAndCount.map(lastUserKey::mapCount90d), uid + "-count90d-to-redis"));
  }

  <KEY extends UserEventRedisHashSupplier>
      DataStream<RedisSinkCommand> userQueryImpressionLastTimeAndCount(
          BaseLastUserQueryKey<KEY> lastUserKey,
          SingleOutputStreamOperator<JoinedImpression> impressions,
          KeyedStream<Long, Long> emitQueries,
          Duration ttl) {
    return userQueryLastTimeAndCount(
        "impression",
        lastUserKey,
        event -> lastUserKey.extractKey(event),
        // TODO - for an unknown reason, the following line causes a serialization error.
        // lastUserKey::getJoinedImpressionKey,
        impressions,
        emitQueries,
        ttl,
        FlatUtil::getEventApiTimestamp);
  }

  <KEY extends UserEventRedisHashSupplier>
      DataStream<RedisSinkCommand> userQueryActionLastTimeAndCount(
          BaseLastUserQueryKey<KEY> lastUserKey,
          SingleOutputStreamOperator<AttributedAction> actions,
          KeyedStream<Long, Long> emitQueries,
          Duration ttl) {
    return userQueryLastTimeAndCount(
        "action",
        lastUserKey,
        event -> lastUserKey.extractKey(event),
        // TODO - for an unknown reason, the following line causes a serialization error.
        // lastUserKey::getAttributedActionKey,
        actions,
        emitQueries,
        ttl,
        action -> action.getAction().getTiming().getEventApiTimestamp());
  }

  // TODO: refactor with userEventLastTimeAndCount
  <EVENT, KEY extends UserEventRedisHashSupplier>
      DataStream<RedisSinkCommand> userQueryLastTimeAndCount(
          String eventName,
          BaseLastUserQueryKey<KEY> lastUserKey,
          SerializableFunction<EVENT, KEY> getKey,
          SingleOutputStreamOperator<EVENT> luidEvents,
          KeyedStream<Long, Long> emitQueries,
          Duration ttl,
          SerializableToLongFunction<EVENT> getEventApiTimestamp) {
    String uid = "LASTTIME-" + eventName + "-BY-" + lastUserKey.getName();

    String toTinyUid = "to-tiny-" + uid;
    // Tuple2<key, eventApiTimestamp>.
    DataStream<Tuple2<KEY, Long>> tinyEvents =
        add(
            luidEvents.map(
                event -> Tuple2.of(getKey.apply(event), getEventApiTimestamp.applyAsLong(event)),
                Types.TUPLE(lastUserKey.getTypeInfo(), Types.LONG)),
            toTinyUid);

    // Tuple4<key, last timestamp, 90d count, expiry>
    LastTimeAndCount<KEY, Tuple2<KEY, Long>> counter =
        new LastTimeAndCount<>(
            enableQueryableState,
            lastUserKey.getTypeInfo(),
            event -> event.f1,
            ttl,
            s3FileOutput.sideOutputDebugLogging);
    SingleOutputStreamOperator<LastTimeAggResult<KEY>> lastTimeAndCount =
        add(tinyEvents.keyBy(in -> in.f0, lastUserKey.getTypeInfo()).process(counter), uid);
    addSinkTransformation(
        s3FileOutput.outputDebugLogging(
            uid,
            lastTimeAndCount.getSideOutput(LastTimeAndCount.LOGGING_TAG),
            LastTimeAndCount.LOG_SCHEMA));

    // Threshold filter
    lastTimeAndCount =
        add(
            lastTimeAndCount
                .keyBy(in -> lastUserKey.getQueryHash(in.getKey()))
                .connect(emitQueries)
                .process(
                    new RightSeenOutput<>(
                        counter.getProducedType(),
                        Types.LONG,
                        left -> lastUserKey.getQueryHash(left.getKey()),
                        left ->
                            lastUserKey.getQueryHash(left.getKey()) == FlatUtil.EMPTY_STRING_HASH)),
            "top-filter-" + uid);

    // platform_id, log_user_id, other_id (see mapTimestamp and mapCount90d) -> {fid -> value}
    return add(lastTimeAndCount.map(lastUserKey::mapTimestamp), uid + "-timestamp-to-redis")
        .union(add(lastTimeAndCount.map(lastUserKey::mapCount90d), uid + "-count90d-to-redis"));
  }

  private CounterKeys getAllCountKeys() {
    return new CounterKeys(count90d);
  }

  /**
   * @return the effective count keys for the job.
   */
  @VisibleForTesting
  public Collection<CountKey<?>> getEnabledCountKeys() {
    CounterKeys counterKeys = getAllCountKeys();
    Map<String, CountKey<?>> allNameToCountKey = counterKeys.getAllKeys();
    Set<CountKey<?>> keys =
        allNameToCountKey.entrySet().stream()
            .filter(entry -> isCountKeyEnabled(entry.getKey()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toSet());
    return keys;
  }

  private boolean isEnabled(CountKey<?> key) {
    return isCountKeyEnabled(key.getName());
  }

  private boolean isCountKeyEnabled(String keyName) {
    return (countKeys.isEmpty() || countKeys.contains(keyName))
        && !excludeCountKeys.contains(keyName);
  }
}
