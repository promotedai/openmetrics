package ai.promoted.metrics.logprocessor.job.join;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.error.MismatchErrorTag;
import ai.promoted.metrics.logprocessor.common.flink.operator.InferenceOperator;
import ai.promoted.metrics.logprocessor.common.flink.operator.KeyedCoProcessOperatorWithWatermarkDelay;
import ai.promoted.metrics.logprocessor.common.flink.operator.KeyedProcessOperatorWithWatermarkDelay;
import ai.promoted.metrics.logprocessor.common.functions.AddLatestImpressions;
import ai.promoted.metrics.logprocessor.common.functions.CombineDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.Delay;
import ai.promoted.metrics.logprocessor.common.functions.FilterOperator;
import ai.promoted.metrics.logprocessor.common.functions.FixDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.RestructureDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicates;
import ai.promoted.metrics.logprocessor.common.functions.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.SetLogTimestamp;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.functions.filter.BuyerPredicate;
import ai.promoted.metrics.logprocessor.common.functions.inferred.ImpressionActionProcessFunction;
import ai.promoted.metrics.logprocessor.common.functions.inferred.InsertionImpressionProcessFunction;
import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeActionDetails;
import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeImpressionDetails;
import ai.promoted.metrics.logprocessor.common.functions.inferred.Options;
import ai.promoted.metrics.logprocessor.common.functions.inferred.ViewResponseInsertionProcessFunction;
import ai.promoted.metrics.logprocessor.common.functions.redundantimpression.ReduceRedundantTinyImpressions;
import ai.promoted.metrics.logprocessor.common.functions.redundantimpression.RedundantImpressionKey;
import ai.promoted.metrics.logprocessor.common.functions.userjoin.UserJoin;
import ai.promoted.metrics.logprocessor.common.functions.validate.BaseValidate;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateAction;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateImpression;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateView;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateUser;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.ContentApiSegment;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafka;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.MergeDetailsOutputs;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.RawActionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawImpressionSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.s3.S3Path;
import ai.promoted.metrics.logprocessor.common.util.BotUtil;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.DroppedMergeDetailsEvent;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LatestImpression;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
    name = "flatoutput",
    mixinStandardHelpOptions = true,
    version = "flatoutput 1.0.0",
    description = "Creates a Flink job that reads LogRequests from Kafka, fills in defaults,"
                + " and produces flat event messages to Kafka.")
public class FlatOutputJob extends BaseFlinkJob {
    private static final Logger LOGGER = LogManager.getLogger(FlatOutputJob.class);
    private static final TypeInformation<TinyEvent> flatTypeInfo = TypeInformation.of(TinyEvent.class);

    @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment(this);
    @CommandLine.Mixin public final MetricsApiKafkaSource metricsApiKafkaSource = new MetricsApiKafkaSource(this, kafkaSegment);
    @CommandLine.Mixin public final RawImpressionSegment rawImpressionSegment = new RawImpressionSegment(this);
    @CommandLine.Mixin public final RawActionSegment rawActionSegment = new RawActionSegment(this);
    @CommandLine.Mixin public final FlatOutputKafka flatOutputKafka = new FlatOutputKafka(kafkaSegment);
    @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this);
    @CommandLine.Mixin public final ContentApiSegment contentApiSegment = new ContentApiSegment(this);

    @Option(names = {"--devMode"}, negatable = true, description = "Whether this is in a local development mode.  Default=false")
    public boolean devMode = false;

    @FeatureFlag
    @Option(names = {"--no-writeJoinedEventsToKafka"}, negatable = true, description = "Whether to write flat events to Kafka.  Default=true")
    public boolean writeJoinedEventsToKafka = true;

    @FeatureFlag
    @Option(names = {"--writeFlatResponseInsertionsToKafka"}, negatable = true, description = "Whether to write flat response insertions to Kafka.  Default=false")
    public boolean writeFlatResponseInsertionsToKafka = false;

    @FeatureFlag
    @Option(names = {"--no-writeFlatActionedResponseInsertion"}, negatable = true, description = "Whether to write a flat_response_insertions that have actions to S3.  Default=true")
    public boolean writeFlatActionedResponseInsertion = true;

    @Option(names = {"--no-checkLateness"}, negatable = true, description = "Whether to check events for lateness and ordering.")
    public boolean checkLateness = true;

    @Option(names = {"--idJoinDurationMultiplier"}, defaultValue = "1", description = "Multiplier to apply to join durations for id joins.")
    public int idJoinDurationMultiplier = 1;

    // Join intervals.  The format uses Duration.parse.

    @Option(names = {"--userJoinMaxTime"}, defaultValue = "-P90D", description = "How far to look back for the user join?  Default=-P90D. Java8 Duration parse format.")
    public Duration userJoinMaxTime = Duration.parse("-P90D");

    @Option(names = {"--userJoinFastMaxOutOfOrderTime"}, defaultValue = "PT0S", description = "This is a softer version of maxOutOfOrderTime that is faster.  If a User record already exist, we'll output using it.  Default=PT0S.  0s is the default so the pipeline waits for the onTimer to trigger after the watermark proceeds. Java8 Duration parse format.")
    public Duration userJoinFastMaxOutOfOrderTime = Duration.parse("PT0S");

    @Option(names = {"--userJoinMaxOutOfOrderTime"}, defaultValue = "PT5M", description = "Max range in User interval join.  TBD on the ideal value for this.  If Flink state wasn't important, we'd bump this up to session length.  Default=PT5M.  Java8 Duration parse format.")
    public Duration userJoinMaxOutOfOrderTime = Duration.parse("PT5M");

    // TODO - shift this from a flag to automatic configuration based on View traffic.
    // Dan: I don't know how to do this in a cheap way.  I'd prefer not to check Flink state when it's not needed.
    // We might need an configuration structure that can be updated asynchronously.
    // It's fine if we miss some View joins.
    @Option(names = {"--skipViewJoin"}, negatable = true, description = "Whether to skip the View joins.")
    public boolean skipViewJoin = false;

    @Option(names = {"--viewInsertionJoinMin"}, defaultValue = "-PT10M", description = "Min range in View Insertion interval join. This uses the DeliveryLog kafka timestamp. Default=-PT10M. Java8 Duration parse format.")
    public Duration viewInsertionJoinMin = Duration.parse("-PT10M");

    @Option(names = {"--viewInsertionJoinMax"}, defaultValue = "PT0S", description = "Max range in View Insertion interval join. This uses the DeliveryLog kafka timestamp. Default=PT0S. Java8 Duration parse format.")
    public Duration viewInsertionJoinMax = Duration.parse("PT0S");

    @Option(names = {"--combineDeliveryLogWindow"}, defaultValue = "PT2S", description = "Window for merging DeliveryLogs with the same clientRequestId. The longer the window, the slower the output.  This impacts the downstream joinMaxes. Default=PT2S. Java8 Duration parse format.")
    public Duration combineDeliveryLogWindow = Duration.parse("PT2S");

    @Option(names = {"--insertionImpressionJoinMin"}, defaultValue = "-PT30M", description = "Min range in Insertion Impression interval join. Default=-PT30M. Java8 Duration parse format.")
    public Duration insertionImpressionJoinMin = Duration.parse("-PT30M");

    @Option(names = {"--insertionImpressionJoinMax"}, defaultValue = "PT0S", description = "Max range in Insertion Impression interval join. Default=PT0S. Java8 Duration parse format.")
    public Duration insertionImpressionJoinMax = Duration.parse("PT0S");

    @Option(names = {"--impressionActionJoinMin"}, defaultValue = "-P1D", description = "Min range in Impression Action interval join. Default=-P1D. Java8 Duration parse format.")
    public Duration impressionActionJoinMin = Duration.parse("-P1D");

    @Option(names = {"--impressionActionJoinMax"}, defaultValue = "PT0S", description = "Max range in Impression Action interval join. Default=PT0S. Java8 Duration parse format.")
    public Duration impressionActionJoinMax = Duration.parse("PT0S");

    @FeatureFlag
    @Option(names = {"--no-addLatestImpressions"}, negatable = true, description = "Whether to AddLatestImpressions on actions.")
    public boolean addLatestImpressions = true;

    @Option(names = {"--extraAddLatestImpressionOutOfOrderness"}, defaultValue = "PT0.5S", description = "An extra maxOutOfOrderness for AddLatestImpressions. Default=PT0.5S")
    public Duration extraAddLatestImpressionOutOfOrderness = Duration.parse("PT0.5S");

    @FeatureFlag
    @Option(names = {"--no-syntheticUser"}, negatable = true, description = "Whether to create fake User records from DeliveryLog.")
    public boolean syntheticUser = true;

    // CR: i'm inclined to just use the max of the impressionActionJoinM* flag values.
    @Option(names = {"--flatResponseInsertionGapDuration"}, defaultValue = "P1D", description = "Gap duration to indicate a completed FlatResponseInsertion. This should be equal to the max impression action join duration.  Default=P1D")
    public Duration flatResponseInsertionGapDuration = Duration.parse("P1D");

    // Keep first durations

    @Option(names = {"--syntheticUserDelayDuration"}, defaultValue = "PT30S", description = "Duration for which we'll delay synthetically created User records.  We need this delay since we prefer real User records and we only look at the first record right now.  We'll improve this later in the year so we don't need a delay. Default=PT30S.")
    public Duration syntheticUserDelayDuration = Duration.parse("PT30S");

    @Option(names = {"--mergeDetailsCleanupPeriod"}, defaultValue = "PT30S", description = "Duration for which to delay TinyEvents where we don't have full entities.  This delay probably isn't needed.  It might catch small issues if we stream realtime through inferred refs. Default=PT1S.")
    public Duration mergeDetailsCleanupPeriod = Duration.parse("PT30S");

    @Option(names = {"--mergeDetailsMissingOutputDelay"}, defaultValue = "PT0S", description = "Duration for which to delay TinyEvents where we don't have full entities.  This delay probably isn't needed.  It might catch small issues if we stream realtime through inferred refs. Default=PT0S.")
    public Duration mergeDetailsMissingOutputDelay = Duration.parse("PT0S");

    @Option(names = {"--mergeDetailsCleanupBuffer"}, defaultValue = "PT30S", description = "Additional Duration added to the clean-ups of events.  This is extra padding. Increasing this means full state lives longer in our system. Default=PT30S.")
    public Duration mergeDetailsCleanupBuffer = Duration.parse("PT30S");

    @Option(names = {"--batchCleanupAllMergeDetailsTimersBeforeTimestamp"}, defaultValue = "0", description = "Used to force cleanup on all timers before timestamp.  Changing cleanup intervals can cause state to be leaked.  This flag can be used when changing intervals to avoid leaks.  Defaults to 0")
    public long batchCleanupAllMergeDetailsTimersBeforeTimestamp = 0;

    @FeatureFlag
    @Option(names = {"--no-writeMismatchError"}, negatable = true, description = "Whether to write MismatchError s3 Parquet files.  This is a flag in case this writing causes performance issues.  Defaults to true.")
    public boolean writeMismatchError = true;

    @FeatureFlag
    @Option(names = {"--nonBuyerUserSparseHash" }, description = "Filters out flat events that contain these Sparse IDs with value=1.  Defaults to empty.")
    public List<Long> nonBuyerUserSparseHashes = new ArrayList<Long>();

    @Option(names = {"--textLogWatermarks"}, negatable = true, description = "Whether to text log watermarks in certain operators.  Defaults to false.")
    public boolean textLogWatermarks = false;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new FlatOutputJob()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        validateArgs();
        startJoinLogJob();
        return 0;
    }

    @Override
    public void validateArgs() {
        Preconditions.checkArgument(maxParallelism > 0, "--maxParalllelism must be set");
        kafkaSegment.validateArgs();
        metricsApiKafkaSource.validateArgs();
        flatOutputKafka.validateArgs();
        s3FileOutput.validateArgs();
        contentApiSegment.validateArgs();
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
                .addAll(kafkaSegment.getProtoClasses())
                .addAll(metricsApiKafkaSource.getProtoClasses())
                .addAll(flatOutputKafka.getProtoClasses())
                .addAll(s3FileOutput.getProtoClasses())
                .add(LatestImpression.class)
                .add(Diagnostics.class)
                .add(TinyEvent.class)
                .add(TinyDeliveryLog.class)
                .add(UnionEvent.class)
                .build();
    }

    private void startJoinLogJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureExecutionEnvironment(env, parallelism, maxParallelism);

        // I think this is safe to change but I didn't want to in case this breaks anything.
        MetricsApiKafkaSource.SplitSources splitLogRequest = metricsApiKafkaSource.splitSources(env, toKafkaConsumerGroupId("joinuser"));


        // TODO: deal with missing log user ids for pre-luid events
        executeJoinEvents(
                splitLogRequest.getRawUserSource(),
                splitLogRequest.getRawViewSource(),
                splitLogRequest.getRawDeliveryLogSource(),
                rawImpressionSegment.getDeduplicatedImpression(splitLogRequest.getRawImpressionSource()),
                rawActionSegment.getDeduplicatedAction(splitLogRequest.getRawActionSource()));

        LOGGER.info("{}.executionPlan\n{}", getJobName(), env.getExecutionPlan());
        env.execute(getJobName());
    }

    /** Returns the name of the Flink join job. */
    @Override
    protected String getJobName() {
        return prefixJobLabel("join-event");
    }

    private static Time toFlinkTime(Duration duration) {
      return Time.milliseconds(duration.toMillis());
    }

    /**
     * Known edge cases:
     * - late correct LHS with the same surrogate key of an existing LHS
     */
    @VisibleForTesting
    void executeJoinEvents(
            DataStream<User> rawUserInput,
            DataStream<View> rawViewInput,
            DataStream<DeliveryLog> rawDeliveryLogInput,
            DataStream<Impression> rawImpressionInput,
            DataStream<Action> rawActionInput) throws Exception {

        // Set the Kafka timestamps as the logTimestamps.
        // TODO - make sure we are not setting logTimestamp in other spots (I assume inferred refs sets it too).

        // TODO - do for rawUserInput too.
        rawUserInput = add(rawUserInput.process(SetLogTimestamp.forUser), "set-user-log-timestamp");

        // TODO - have a better output file format that is easier to analyze.
        // - error_type
        // - the raw record.

        SingleOutputStreamOperator<View> validatedView = add(rawViewInput.process(SetLogTimestamp.forView), "set-view-log-timestamp");
        validatedView = add(validatedView.process(new ValidateView()), "validate-view");

        SingleOutputStreamOperator<DeliveryLog> validatedDeliveryLog = add(rawDeliveryLogInput.process(SetLogTimestamp.forDeliveryLog), "set-delivery-log-log-timestamp");
        validatedDeliveryLog = add(validatedDeliveryLog.process(new ValidateDeliveryLog()), "validate-delivery-log");

        SingleOutputStreamOperator<Impression> validatedImpression = add(rawImpressionInput.process(SetLogTimestamp.forImpression), "set-impression-log-timestamp");
        validatedImpression = add(validatedImpression.process(new ValidateImpression()), "validate-impression");

        SingleOutputStreamOperator<Action> validatedAction = add(rawActionInput.process(SetLogTimestamp.forAction), "set-action-log-timestamp");
        validatedAction = add(validatedAction.process(new ValidateAction()), "validate-action");

        // Remove DeliveryLogs that we do not want to consider in inferred refs.
        // Do this before writing to the side.all_delivery_log... so we can improve throughput.
        validatedDeliveryLog = add(
                validatedDeliveryLog.map(new RestructureDeliveryLog()),
                "restructure-delivery-log");
        SingleOutputStreamOperator<DeliveryLog> unkeyedFilteredDeliveryLogs = filterShouldJoin(validatedDeliveryLog);
        unkeyedFilteredDeliveryLogs = filterNonBotDeliveryLogs(unkeyedFilteredDeliveryLogs);
        unkeyedFilteredDeliveryLogs = fixDeliveryLogStream(unkeyedFilteredDeliveryLogs);
        outputAllDeliveryLogVariants(unkeyedFilteredDeliveryLogs);

        DataStream<User> allUsersStream = createAllUserStream(rawUserInput, unkeyedFilteredDeliveryLogs);
        SingleOutputStreamOperator<User> userUpdates = add(allUsersStream.process(new ValidateUser()), "validate-alluser");

        outputValidationError(
                userUpdates.getSideOutput(BaseValidate.INVALID_TAG)
                        .union(validatedView.getSideOutput(BaseValidate.INVALID_TAG))
                        .union(validatedDeliveryLog.getSideOutput(BaseValidate.INVALID_TAG))
                        .union(validatedImpression.getSideOutput(BaseValidate.INVALID_TAG))
                        .union(validatedAction.getSideOutput(BaseValidate.INVALID_TAG)));

        SingleOutputStreamOperator<View> filteredViews = filterNonBotViews(validatedView);

        SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLogs = add(
                unkeyedFilteredDeliveryLogs
                        .keyBy(KeyUtil.deliveryLogLogUserIdKey)
                        // Support a simple PassThroughCombineDeliveryLog for tests.
                        // We need to delay the Watermark since CombineDeliveryLog can delay outputs up to the window.
                        // If we do not delay the watermark, the inferred reference code will mark the delayed output
                        // as late.
                        // Warning - this is a weird spot to delay for synthetic DeliveryLogs.  Trying to delay
                        // this closer to window/aggregate code fails with a message saying the timer service has
                        // not been initialized.
                        .transform("combine-delivery-log", TypeInformation.of(CombinedDeliveryLog.class),
                                new KeyedProcessOperatorWithWatermarkDelay<>(
                                        new CombineDeliveryLog(combineDeliveryLogWindow, getDebugIds()),
                                        combineDeliveryLogWindow.toMillis(),
                                        textLogWatermarks)),
                "combine-delivery-log");

        KeyedStream<TinyEvent, Tuple2<Long, String>> keyedSmallViewInput =
                toTinyView(filteredViews, "map-tiny-view").keyBy(KeyUtil.TinyEventLogUserIdKey);
        KeyedStream<TinyDeliveryLog, Tuple2<Long, String>> smallDeliveryLogInput =
                toTinyDeliveryLog(combinedDeliveryLogs).keyBy(KeyUtil.TinyDeliveryLogLogUserIdKey);
        KeyedStream<TinyEvent, Tuple2<Long, String>> smallImpressionInput =
                toTinyImpression(validatedImpression).keyBy(KeyUtil.TinyEventLogUserIdKey);
        SingleOutputStreamOperator<TinyEvent> tinyActions = toTinyAction(validatedAction);
        if (contentApiSegment.shouldJoinOtherContentIds()) {
            // Dan: I'm not sure if keying by contentId decreases the number of times the same content appears in a cache.
            KeyedStream<TinyEvent, Tuple2<Long, String>> keyedContentIdTinyAction = tinyActions.keyBy(KeyUtil.TinyEventContentIdKey);
            tinyActions = add(contentApiSegment.joinOtherContentIdsFromContentService(keyedContentIdTinyAction), "add-action-other-content-ids");
        }
        outputDebugRecords("rhs-tiny-action", TinyEvent::getLogTimestamp, tinyActions);

        KeyedStream<TinyEvent, Tuple2<Long, String>> keyedTinyActionInput = tinyActions.keyBy(KeyUtil.TinyEventLogUserIdKey);

        ViewResponseInsertionProcessFunction viewResponseInsertionProcessFunction = new ViewResponseInsertionProcessFunction(Options.builder()
                .setSkipJoin(skipViewJoin)
                .setRightOuterJoin(true)
                .setMaxTime(skipViewJoin ? Duration.ZERO : viewInsertionJoinMin)
                .setMaxOutOfOrder(skipViewJoin ? Duration.ZERO : viewInsertionJoinMax)
                .setCheckLateness(checkLateness)
                .setIdJoinDurationMultiplier(skipViewJoin ? 1 : idJoinDurationMultiplier)
                .setDebugIds(getDebugIds())
                .build());

        SingleOutputStreamOperator<TinyEvent> unkeyedViewToInsertions = add(
                keyedSmallViewInput.connect(smallDeliveryLogInput)
                        .transform("join-view-insertions", flatTypeInfo, InferenceOperator.of(viewResponseInsertionProcessFunction, textLogWatermarks)),
                "join-view-responseinsertions");
        outputLateEvents("view-responseinsertions", unkeyedViewToInsertions.getSideOutput(ViewResponseInsertionProcessFunction.LATE_EVENTS_TAG));
        outputDroppedEvents(
                "view-responseinsertions", viewResponseInsertionProcessFunction.getRightLogTimeGetter(),
                unkeyedViewToInsertions.getSideOutput(viewResponseInsertionProcessFunction.getDroppedEventsTag()));

        // Join content IDs after the View-DeliveryLog join.  This keeps the View-DeliveryLog join more efficient.

        // TODO - how is the watermark impacted?
        if (contentApiSegment.shouldJoinOtherContentIds()) {
            // Dan: I'm not sure if keying by contentId decreases the number of times the same content appears in a cache.
            KeyedStream<TinyEvent, Tuple2<Long, String>> keyedContentIdViewToInsertions = unkeyedViewToInsertions.keyBy(KeyUtil.TinyEventContentIdKey);
            unkeyedViewToInsertions = add(contentApiSegment.joinOtherContentIdsFromContentService(keyedContentIdViewToInsertions), "add-insertion-other-content-ids");
        }
        KeyedStream<TinyEvent, Tuple2<Long, String>> viewToInsertions =
                unkeyedViewToInsertions.keyBy(KeyUtil.TinyEventLogUserIdKey);
        outputPartialResponseInsertion(unkeyedViewToInsertions);

        InsertionImpressionProcessFunction insertionImpressionProcessFunction = new InsertionImpressionProcessFunction(Options.builder()
                .setMaxTime(insertionImpressionJoinMin)
                .setMaxOutOfOrder(insertionImpressionJoinMax)
                .setCheckLateness(checkLateness)
                .setIdJoinDurationMultiplier(idJoinDurationMultiplier)
                .setDebugIds(getDebugIds())
                .build());
        SingleOutputStreamOperator<TinyEvent> unkeyedViewToImpressions = add(
                viewToInsertions.connect(smallImpressionInput)
                        .transform("join-insertion-impressions", flatTypeInfo, InferenceOperator.of(insertionImpressionProcessFunction, textLogWatermarks)),
                "join-insertion-impressions");
        KeyedStream<TinyEvent, Tuple2<Long, String>> viewToImpressions =
            unkeyedViewToImpressions.keyBy(KeyUtil.TinyEventLogUserIdKey);
        outputLateEvents("insertion-impressions", unkeyedViewToImpressions.getSideOutput(InsertionImpressionProcessFunction.LATE_EVENTS_TAG));
        outputDroppedEvents(
                "insertion-impressions", insertionImpressionProcessFunction.getRightLogTimeGetter(),
                unkeyedViewToImpressions.getSideOutput(insertionImpressionProcessFunction.getDroppedEventsTag()));

        ImpressionActionProcessFunction impressionActionProcessFunction = new ImpressionActionProcessFunction(
                Options.builder()
                        .setMaxTime(impressionActionJoinMin)
                        .setMaxOutOfOrder(impressionActionJoinMax)
                        .setCheckLateness(checkLateness)
                        .setIdJoinDurationMultiplier(idJoinDurationMultiplier)
                        .setDebugIds(getDebugIds())
                        .build(),
                contentApiSegment.contentIdFieldKeys);
        SingleOutputStreamOperator<TinyEvent> unkeyedViewToActions = add(
                viewToImpressions.connect(keyedTinyActionInput)
                        .transform("join-impression-actions", flatTypeInfo, InferenceOperator.of(impressionActionProcessFunction, textLogWatermarks)),
                "join-impression-actions");
        KeyedStream<TinyEvent, Tuple2<Long, String>> viewToActions = unkeyedViewToActions.keyBy(KeyUtil.TinyEventLogUserIdKey);
        outputLateEvents("impression-actions", unkeyedViewToActions.getSideOutput(ImpressionActionProcessFunction.LATE_EVENTS_TAG));
        outputDroppedEvents(
                "impression-actions", impressionActionProcessFunction.getRightLogTimeGetter(),
                unkeyedViewToActions.getSideOutput(impressionActionProcessFunction.getDroppedEventsTag()));

        // Union the streams so we don't need to maintain as much duplicate state.
        DataStream<TinyEvent> unionTinyEvent = viewToImpressions.union(viewToActions);

        SingleOutputStreamOperator<TinyEvent> reducedEvents = add(
                unionTinyEvent.keyBy(RedundantImpressionKey::of)
                        .process(new ReduceRedundantTinyImpressions<>(impressionActionJoinMin.abs())),
                "reduce-redundant-events");
        outputDroppedEvents("redundant-impression", TinyEvent::getLogTimestamp,
                reducedEvents.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION));
        outputDebugRecords("tiny-event", TinyEvent::getLogTimestamp, reducedEvents);

        DataStream<MismatchError> mismatchErrors = unkeyedViewToInsertions.getSideOutput(MismatchErrorTag.TAG)
                    .union(unkeyedViewToImpressions.getSideOutput(MismatchErrorTag.TAG))
                    .union(unkeyedViewToActions.getSideOutput(MismatchErrorTag.TAG));
        enrichFilterAndFlatten(reducedEvents, filteredViews, combinedDeliveryLogs, validatedImpression, validatedAction, userUpdates, mismatchErrors);
    }

    // TODO - refactor this to a different class.  It'll make the separate more obvious.

    /**
     * The second phase of the join.  The first phase validates and joins the stream of TinyEvents together.
     *
     * The second phase:
     * 1. Converts the TinyEvents back to fully joined details.
     * 2. Filters out events.
     * 3. Produced joined and flat event records.
     *
     * This section is structured as a separate method with a lot of parameters so we can eventually refactor it into a
     * different Flink job.  This is difficult right now because the validation logic is coupled to the first part of the job.
     *
     * @param tinyEvent Unioned tiny Impression and Action stream.  This is a combined stream to support redundant impressions
     * @param view Post validation and filtering
     * @param combinedDeliveryLog Post validation and filtering
     * @param impression Post validation and filtering
     * @param action Post validation and filtering
     * @param userUpdates Stream of User record updates
     * @param mismatchErrors This is passed through to have one side output.  When we split the job, this does not
     *                       need to be passed through
     */
    private void enrichFilterAndFlatten(
            SingleOutputStreamOperator<TinyEvent> tinyEvent,
            SingleOutputStreamOperator<View> view,
            SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLog,
            SingleOutputStreamOperator<Impression> impression,
            SingleOutputStreamOperator<Action> action,
            SingleOutputStreamOperator<User> userUpdates,
            DataStream<MismatchError> mismatchErrors
    ) {
        SingleOutputStreamOperator<TinyEvent> tinyImpression =
                add(tinyEvent.filter(event -> event.getActionId().isEmpty()), "filter-tiny-event-to-impression");
        SingleOutputStreamOperator<TinyEvent> tinyAction =
                add(tinyEvent.filter(event -> !event.getActionId().isEmpty()), "filter-tiny-event-to-action");

        MergeDetailsOutputs mergeImpressionDetailsOutput =
                mergeImpressionDetails(view, combinedDeliveryLog, impression, tinyImpression);

        // TODO - If we don't do the data lake, we can shift the MergeActionDetails to be a normal interval join.
        // MergeImpressionDetails is still useful because we can consolidate DeliveryLog state into fewer denormalized
        // copies by doing a special join.
        MergeDetailsOutputs mergeActionDetailsOutput =
                mergeActionDetails(mergeImpressionDetailsOutput.joinedEvent(), action, tinyAction);

        mismatchErrors = mismatchErrors
                .union(mergeImpressionDetailsOutput.mismatchErrors())
                .union(mergeActionDetailsOutput.mismatchErrors());

        // Currently filters out non-buyer traffic.  We might add more over time.
        // It's important to do this after the full join so we don't get a lot of dropped records.
        // This is fine.  The volume is currently low.
        //
        // TODO - filter out these events earlier.  We can include the filter conditions inside TinyEvent and avoid the MergeDetails.
        SingleOutputStreamOperator<JoinedEvent> filteredJoinedImpression = filterJoinedEvent(mergeImpressionDetailsOutput.joinedEvent(), "impression");
        SingleOutputStreamOperator<JoinedEvent> filteredJoinedAction = filterJoinedEvent(mergeActionDetailsOutput.joinedEvent(), "action");

        joinUserAndOutput(userUpdates, filteredJoinedImpression, filteredJoinedAction, mismatchErrors);
    }

    /** Merges details onto tiny impressions. */
    private MergeDetailsOutputs mergeImpressionDetails(
            SingleOutputStreamOperator<View> view,
            SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLog,
            SingleOutputStreamOperator<Impression> impression,
            SingleOutputStreamOperator<TinyEvent> tinyImpression) {
        DataStream<UnionEvent> unkeyedFullUnionEvent = toUnionEvent(view, combinedDeliveryLog, impression);
        KeyedStream<UnionEvent, Tuple2<Long, String>> fullUnionEvent = unkeyedFullUnionEvent.keyBy(KeyUtil.unionEntityKeySelector);
        KeyedStream<TinyEvent, Tuple2<Long, String>> keyedTinyImpression = tinyImpression.keyBy(KeyUtil.TinyEventLogUserIdKey);
        SingleOutputStreamOperator<JoinedEvent> joinedImpression = add(
                fullUnionEvent.connect(keyedTinyImpression).process(createMergeImpressionDetails()),
                "merge-impression-details");

        outputDroppedMergeDetailsEvents("merge-impression-details", joinedImpression.getSideOutput(MergeImpressionDetails.DROPPED_TAG));
        return MergeDetailsOutputs.create(joinedImpression, joinedImpression.getSideOutput(MismatchErrorTag.TAG));
    }

    /** Merges details onto tiny impressions. */
    private MergeDetailsOutputs mergeActionDetails(
            SingleOutputStreamOperator<JoinedEvent> joinedImpression,
            SingleOutputStreamOperator<Action> action,
            SingleOutputStreamOperator<TinyEvent> tinyImpression) {
        DataStream<UnionEvent> unkeyedFullUnionEvent = toUnionEvent(joinedImpression, action);
        KeyedStream<UnionEvent, Tuple2<Long, String>> fullUnionEvent = unkeyedFullUnionEvent.keyBy(KeyUtil.unionEntityKeySelector);
        KeyedStream<TinyEvent, Tuple2<Long, String>> keyedTinyImpression = tinyImpression.keyBy(KeyUtil.TinyEventLogUserIdKey);
        SingleOutputStreamOperator<JoinedEvent> joinedAction = add(
                fullUnionEvent.connect(keyedTinyImpression).process(createMergeActionDetails()),
                "merge-action-details");

        outputDroppedMergeDetailsEvents("merge-action-details", joinedAction.getSideOutput(MergeActionDetails.DROPPED_TAG));
        return MergeDetailsOutputs.create(joinedAction, joinedAction.getSideOutput(MismatchErrorTag.TAG));
    }

    private SingleOutputStreamOperator<TinyEvent> toTinyView(DataStream<View> views, String uid) {
        return add(
                views.map(view -> TinyEvent.newBuilder()
                        .setPlatformId(view.getPlatformId())
                        .setLogUserId(view.getUserInfo().getLogUserId())
                        .setLogTimestamp(view.getTiming().getLogTimestamp())
                        .setViewId(view.getViewId())
                        .build()),
                uid);
    }

    private SingleOutputStreamOperator<TinyDeliveryLog> toTinyDeliveryLog(SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLogs) {
        return add(combinedDeliveryLogs.map(new ToTinyDeliveryLog(contentApiSegment.contentIdFieldKeys)), "map-tiny-delivery-log");
    }

    /** Used to have small dropped side outputs for DeliveryLogs. */
    private SingleOutputStreamOperator<TinyEvent> toTinyDeliveryLogRequest(DataStream<DeliveryLog> deliveryLogs, String uid) {
        return add(
                deliveryLogs
                        .map(deliveryLog -> {
                            Request request = deliveryLog.getRequest();
                            return TinyEvent.newBuilder()
                                    .setPlatformId(DeliveryLogUtil.getPlatformId(deliveryLog))
                                    .setLogUserId(request.getUserInfo().getLogUserId())
                                    .setLogTimestamp(request.getTiming().getLogTimestamp())
                                    .setViewId(request.getViewId())
                                    .setRequestId(request.getRequestId())
                                    .build();
                        }),
                uid);
    }

    private SingleOutputStreamOperator<TinyEvent> toTinyImpression(SingleOutputStreamOperator<Impression> impressions) {
        return add(impressions.map(new ToTinyImpression(contentApiSegment.contentIdFieldKeys)), "map-tiny-impression");
    }

    // TODO - see if this is a breaking change.
    private SingleOutputStreamOperator<TinyEvent> toTinyAction(SingleOutputStreamOperator<Action> actions) {
        return add(actions.flatMap(new ToTinyAction(contentApiSegment.contentIdFieldKeys)), "map-tiny-action");
    }

    private MergeImpressionDetails createMergeImpressionDetails() {
        // It's better to over allocate the clean-up delay than under allocate it.

        // We need to delay cleaning up all records for any delays in the main join.  We'll add this to impressionCleanupDelay.
        // - inferred ref delays.
        // - combineDeliveryLogWindow
        // - mergeDetailsMissingOutputDelay
        // - mergeDetailsCleanupBuffer
        // - redundant impressions.
        //
        // The maxes are added to the leaf clean-up to support out of order events.
        Duration impressionCleanupDelay = add(viewInsertionJoinMax, insertionImpressionJoinMax, combineDeliveryLogWindow, mergeDetailsMissingOutputDelay, mergeDetailsCleanupBuffer);
        Duration deliveryLogCleanupDelay = add(impressionCleanupDelay, insertionImpressionJoinMin);
        Duration viewCleanupDelay = add(deliveryLogCleanupDelay, viewInsertionJoinMin);
        LOGGER.info("setting up MergeImpressionDetails.  impressionCleanupDelay={}, deliveryLogCleanupDelay={}, viewCleanupDelay={}",
                impressionCleanupDelay, deliveryLogCleanupDelay, viewCleanupDelay);
        return new MergeImpressionDetails(viewCleanupDelay, deliveryLogCleanupDelay, impressionCleanupDelay, mergeDetailsCleanupPeriod, mergeDetailsMissingOutputDelay, skipViewJoin, batchCleanupAllMergeDetailsTimersBeforeTimestamp, getDebugIds());
    }

    // Keeping MergeActionDetails separate from MergeImpressionDetails allows MergeImpressionDetails to have shorter TTLs
    // on it's state.  Since many customers will have a lot more DeliveryLog info than impressions, this allows us to
    // remove a lot of state earlier.
    private MergeActionDetails createMergeActionDetails() {
        // It's better to over allocate the clean-up delay than under allocate it.

        // We need to delay cleaning up all records for any delays in the main join.  We'll add this to actionCleanupDelay.
        // - inferred ref delays.
        // - combineDeliveryLogWindow
        // - mergeDetailsMissingOutputDelay
        // - mergeDetailsCleanupBuffer
        // - redundant impressions.
        //
        // The maxes are added to the leaf clean-up to support out of order events.
        Duration actionCleanupDelay = add(viewInsertionJoinMax, insertionImpressionJoinMax, combineDeliveryLogWindow, impressionActionJoinMax, mergeDetailsMissingOutputDelay, mergeDetailsCleanupBuffer);
        Duration joinedImpressionCleanupDelay = add(actionCleanupDelay, impressionActionJoinMin);
        LOGGER.info("setting up MergeActionDetails.  actionCleanupDelay={}, joinedImpressionCleanupDelay={}",
                actionCleanupDelay, joinedImpressionCleanupDelay);
        return new MergeActionDetails(joinedImpressionCleanupDelay, actionCleanupDelay, mergeDetailsCleanupPeriod, mergeDetailsMissingOutputDelay, batchCleanupAllMergeDetailsTimersBeforeTimestamp, getDebugIds());
    }

    private static Duration add(Duration... durations) {
        long millis = 0;
        for (int i = 0; i < durations.length; i++) {
            millis += Math.abs(durations[i].toMillis());
        }
        return Duration.ofMillis(millis);
    }

    private DataStream<UnionEvent> toUnionEvent(
            SingleOutputStreamOperator<View> views,
            SingleOutputStreamOperator<CombinedDeliveryLog> deliveryLogs,
            SingleOutputStreamOperator<Impression> impressions) {
        SingleOutputStreamOperator<UnionEvent> unionViews = add(
                views.map(view -> UnionEvent.newBuilder()
                        .setView(FlatUtil.clearRedundantFlatViewFields(view.toBuilder()))
                        .build()),
                "map-view-to-union-event");
        SingleOutputStreamOperator<UnionEvent> unionDeliveryLogs = add(
                deliveryLogs.map(deliveryLog -> UnionEvent.newBuilder()
                        // TODO - clean fields.
                        .setCombinedDeliveryLog(deliveryLog)
                        .build()),
                "map-delivery-log-to-union-event");
        SingleOutputStreamOperator<UnionEvent> unionImpressions = add(
                impressions.map(impression -> UnionEvent.newBuilder()
                        .setImpression(FlatUtil.clearRedundantFlatImpressionFields(impression.toBuilder()))
                        .build()),
                "map-impression-to-union-event");
        return unionViews.union(unionDeliveryLogs, unionImpressions);
    }

    private DataStream<UnionEvent> toUnionEvent(
            SingleOutputStreamOperator<JoinedEvent> joinedImpressions,
            SingleOutputStreamOperator<Action> actions) {
        SingleOutputStreamOperator<UnionEvent> unionImpressions = add(
                joinedImpressions.map(joinedImpression -> UnionEvent.newBuilder()
                        .setJoinedImpression(joinedImpression)
                        .build()),
                "map-joined-impression-to-union-event");
        SingleOutputStreamOperator<UnionEvent> unionActions = add(
                actions.map(action -> UnionEvent.newBuilder()
                        .setAction(FlatUtil.clearRedundantFlatActionFields(action.toBuilder()))
                        .build()),
                "map-action-to-union-event");
        return unionImpressions.union(unionActions);
    }

    /**
     * Creates a stream of all User records.  Can include synthetic User records created from non-User records.
     * This is temporary until we have a User DB, support retractions and change the joins to support
     * updating the LHS.
     *
     * @param allDeliveryLogs if synthetic users are enabled, synthetic users are made from allDeliveryLogs 
     */
    private DataStream<User> createAllUserStream(
            DataStream<User> users,
            DataStream<DeliveryLog> allDeliveryLogs) {

        if (syntheticUser) {
            DataStream<User> syntheticUsers = createSyntheticUserStream(allDeliveryLogs);

            // Delays the synthetic first user output so we can get any real user records first.
            DataStream<User> delayFirstSyntheticUserInput = add(
                    syntheticUsers.keyBy(KeyUtil.logUserIdKeySelector)
                            .transform("delay-synthetic-user", TypeInformation.of(User.class),
                                    new KeyedProcessOperatorWithWatermarkDelay<>(
                                            new Delay<User>("synthetic-user", syntheticUserDelayDuration, User.class),
                                            syntheticUserDelayDuration.toMillis(),
                                            textLogWatermarks)),
                    "delay-synthetic-user");

            // Union the users stream with the delayed synthetic user stream.
            return users.union(delayFirstSyntheticUserInput);
        } else {
            return users;
        }
    }

    /** Creates a stream of synthetic Users from allDeliveryLogs. */
    private DataStream<User> createSyntheticUserStream(DataStream<DeliveryLog> allDeliveryLogs) {
        return add(allDeliveryLogs.flatMap(new FlatMapFunction<DeliveryLog, User>() {
                    @Override
                    public void flatMap(DeliveryLog deliveryLog, Collector<User> collector) throws Exception {
                        Request request = deliveryLog.getRequest();
                        if (!request.getUserInfo().getUserId().isEmpty()) {
                            collector.collect(User.newBuilder()
                                    .setPlatformId(deliveryLog.getPlatformId())
                                    .setUserInfo(request.getUserInfo())
                                    .setTiming(request.getTiming())
                                    .build());
                        }
                    }
                }),
                "create-synthetic-user");
    }

    // This code is temporary.  We can remove it later.
    @VisibleForTesting
    SingleOutputStreamOperator<DeliveryLog> fixDeliveryLogStream(SingleOutputStreamOperator<DeliveryLog> deliveryLogs) {
        return withDebugIdLogger(
                add(deliveryLogs.map(new FixDeliveryLog()), "prepare-delivery-log"),
                "rawDeliveryLogInput");
    }

    private SingleOutputStreamOperator<DeliveryLog> withDebugIdLogger(SingleOutputStreamOperator<DeliveryLog> deliveryLogs, String label) {
        DebugIds debugIds = getDebugIds();
        if (!debugIds.hasAnyIds()) {
            return deliveryLogs;
        } else {
            return add(deliveryLogs
                .map(deliveryLog -> {
                    if (debugIds.matches(deliveryLog)) {
                        LOGGER.info("Found debugId in {}, deliveryLog={}", label, deliveryLog);
                    }
                    return deliveryLog;
                }),
                "match-debug-id-" + label + "-log");
        }
    }

    void joinUserAndOutput(
            SingleOutputStreamOperator<User> userUpdates,
            SingleOutputStreamOperator<JoinedEvent> joinedImpressions,
            SingleOutputStreamOperator<JoinedEvent> joinedActions,
            DataStream<MismatchError> mismatchErrors) {
        DebugIds debugIds = getDebugIds();

        KeyedStream<User, Tuple2<Long, String>> keyedUsers =
            userUpdates.keyBy(KeyUtil.logUserIdKeySelector);
        KeyedStream<JoinedEvent, Tuple2<Long, String>> keyedFlatImpressions =
            joinedImpressions.keyBy(KeyUtil.joinedEventLogUserIdKey);
        KeyedStream<JoinedEvent, Tuple2<Long, String>> keyedFlatActions =
            joinedActions.keyBy(KeyUtil.joinedEventLogUserIdKey);

        // TODO - group together attribution models.
        SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions = cogroupFlatResponseInsertions(
                keyedFlatImpressions, keyedFlatActions);
        // Currently this should only happen during edge cases.
        // This might become more common after we support outer joins with DeliveryLog.
        flatResponseInsertions = filterFlatResponseInsertion(flatResponseInsertions);

        KeyedCoProcessOperator<Tuple2<Long, String>, User, JoinedEvent, JoinedEvent> userJoinedEventJoin =
                new KeyedCoProcessOperatorWithWatermarkDelay(
                        new UserJoin<>(
                                userJoinMaxTime,
                                userJoinFastMaxOutOfOrderTime,
                                userJoinMaxOutOfOrderTime,
                                JoinedEvent.class,
                                FlatUtil::setFlatUserAndBuild,
                                (event) -> event.getTiming().getLogTimestamp(),
                                debugIds,
                                debugIds::matches,
                                devMode),
                        userJoinMaxOutOfOrderTime.toMillis(),
                        textLogWatermarks);

        SingleOutputStreamOperator<JoinedEvent> flatUserImpressions = add(
                keyedUsers.connect(keyedFlatImpressions)
                        .transform("join-user-impression", TypeInformation.of(JoinedEvent.class), userJoinedEventJoin),
                "join-user-impression");

        if (addLatestImpressions) {
            // TODO - delay watermark?
            joinedActions = add(
                    keyedFlatActions.connect(keyedFlatImpressions)
                            .process(new AddLatestImpressions(extraAddLatestImpressionOutOfOrderness, devMode)),
                    "join-latest-impressions");
        }

        SingleOutputStreamOperator<JoinedEvent> flatUserActions = add(
                keyedUsers.connect(keyedFlatActions)
                        .transform("join-user-impression", TypeInformation.of(JoinedEvent.class), userJoinedEventJoin),
                "join-user-action");

        KeyedCoProcessOperator<Tuple2<Long, String>, User, FlatResponseInsertion, FlatResponseInsertion> userFlatResponseInsertionJoin =
                new KeyedCoProcessOperatorWithWatermarkDelay(
                        new UserJoin<>(
                                userJoinMaxTime,
                                userJoinFastMaxOutOfOrderTime,
                                userJoinMaxOutOfOrderTime,
                                FlatResponseInsertion.class,
                                FlatUtil::setFlatUserAndBuild,
                                (event) -> event.getTiming().getLogTimestamp(),
                                debugIds,
                                debugIds::matches,
                                devMode),
                        userJoinMaxOutOfOrderTime.toMillis(),
                        textLogWatermarks);

        SingleOutputStreamOperator<FlatResponseInsertion> flatUserResponseInsertions = add(
                keyedUsers.connect(flatResponseInsertions.keyBy(KeyUtil.flatResponseInsertionLogUserIdKey))
                        .transform("join-user-impression", TypeInformation.of(FlatResponseInsertion.class), userFlatResponseInsertionJoin),
                "join-flat-user-response-insertion");

        if (writeMismatchError) {
            outputMismatchError(mismatchErrors
                    .union(flatUserImpressions.getSideOutput(MismatchErrorTag.TAG))
                    .union(flatUserActions.getSideOutput(MismatchErrorTag.TAG))
                    .union(flatUserResponseInsertions.getSideOutput(MismatchErrorTag.TAG)));
        }

        outputJoinedEvents(
                "etl",
                joinedImpressions, flatUserImpressions,
                joinedActions, flatUserActions,
                flatResponseInsertions, flatUserResponseInsertions);
    }

    private SingleOutputStreamOperator<FlatResponseInsertion> cogroupFlatResponseInsertions(
            KeyedStream<JoinedEvent, Tuple2<Long, String>> keyedFlatImpressions,
            KeyedStream<JoinedEvent, Tuple2<Long, String>> keyedFlatActions) {

        // TODO - evaluate switching to key by our usual Tuple2 and do our own window join.
        // CR: this is going to be very memory intensive.
        // are we sure an offline (non-realtime) enrichment process is not the right way to go
        // instead of using flink streaming?
        SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions = add(
                keyedFlatImpressions
                        .coGroup(keyedFlatActions)
                        .where(KeyUtil.joinedEventLogUserInsertionKey)
                        .equalTo(KeyUtil.joinedEventLogUserInsertionKey)
                        .window(EventTimeSessionWindows.withGap(toFlinkTime(flatResponseInsertionGapDuration)))
                        // TODO: switch to apply with flink 2.0+.
                        .with(new CoGroupFunction<JoinedEvent, JoinedEvent, FlatResponseInsertion>() {
                            @Override
                            public void coGroup(
                                    Iterable<JoinedEvent> impressions,
                                    Iterable<JoinedEvent> actions,
                                    Collector<FlatResponseInsertion> out) {
                                out.collect(FlatUtil.createFlatResponseInsertion(impressions, actions).build());
                            }
                        }),
                "join-flat-response-insertion");

        // Flink core has a bug with cogroups.  It has internal operators and does not support setting direct uids on them.
        // For now, set uids and names using the Transformation interface.
        // https://issues.apache.org/jira/browse/FLINK-25285
        Transformation<?> partition = Iterables.getOnlyElement(flatResponseInsertions.getTransformation().getInputs());
        setUidAndName(partition, "partition-join-flat-response-insertion");
        Transformation<?> union = Iterables.getOnlyElement(partition.getInputs());
        setUidAndName(union, "union-join-flat-response-insertion");
        List<Transformation<?>> mapTransformations = union.getInputs();
        Preconditions.checkState(mapTransformations.size() == 2, "There should be 2 Cogroup internal Map transformations");
        setUidAndName(mapTransformations.get(0), "map-left-join-flat-response-insertion");
        setUidAndName(mapTransformations.get(1), "map-right-join-flat-response-insertion");

        return flatResponseInsertions;
    }

    private SingleOutputStreamOperator<JoinedEvent> filterJoinedEvent(SingleOutputStreamOperator<JoinedEvent> joinedEvents, String recordType) {
        // Concat so recordType can be kept as "impression" and "action".
        recordType = "joined-" + recordType;
        joinedEvents = filterOutIgnoreUsage(joinedEvents, recordType, JoinedEvent.class, FlatUtil::hasIgnoreUsage,
                joined -> joined.getTiming().getLogTimestamp());
        return filterBuyer(joinedEvents, recordType, JoinedEvent.class, JoinedEvent::getApiExecutionInsertion,
                joined -> joined.getTiming().getLogTimestamp());
    }

    private SingleOutputStreamOperator<FlatResponseInsertion> filterFlatResponseInsertion(SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions) {
        flatResponseInsertions = filterOutIgnoreUsage(flatResponseInsertions, "flat-response-insertion", FlatResponseInsertion.class,
                FlatUtil::hasIgnoreUsage, flat -> flat.getTiming().getLogTimestamp());
        return filterBuyer(flatResponseInsertions, "flat-response-insertion", FlatResponseInsertion.class,
                FlatResponseInsertion::getApiExecutionInsertion, flat -> flat.getTiming().getLogTimestamp());
    }

    private SingleOutputStreamOperator<DeliveryLog> filterShouldJoin(SingleOutputStreamOperator<DeliveryLog> events) {
        // Use TinyEvent to keep the records small.
        OutputTag<DeliveryLog> tag = new OutputTag("should-not-join", TypeInformation.of(DeliveryLog.class));
        SingleOutputStreamOperator<DeliveryLog> result = add(
                events.process(new FilterOperator<>(DeliveryLogUtil::shouldJoin, tag)),
                "filter-delivery-log-should-join");
        SingleOutputStreamOperator<TinyEvent> tinyDroppedDeliveryLogs =
                toTinyDeliveryLogRequest(result.getSideOutput(tag), "map-should-join-tiny-delivery-log");
        outputDroppedEvents("delivery-log-should-not-join", TinyEvent::getLogTimestamp, tinyDroppedDeliveryLogs);
        return result;
    }

    private SingleOutputStreamOperator<DeliveryLog> filterNonBotDeliveryLogs(SingleOutputStreamOperator<DeliveryLog> events) {
        // Use TinyEvent to keep the records small.
        OutputTag<DeliveryLog> tag = new OutputTag("has-bot", TypeInformation.of(DeliveryLog.class));
        SingleOutputStreamOperator<DeliveryLog> result = add(
                events.process(new FilterOperator<>(BotUtil::isNotBot, tag)),
                "filter-delivery-log-is-bot");
        SingleOutputStreamOperator<TinyEvent> tinyDroppedDeliveryLogs =
                toTinyDeliveryLogRequest(result.getSideOutput(tag), "map-is-bot-tiny-delivery-log");
        outputDroppedEvents("delivery-log-is-bot", TinyEvent::getLogTimestamp, tinyDroppedDeliveryLogs);
        return result;
    }

    private SingleOutputStreamOperator<View> filterNonBotViews(SingleOutputStreamOperator<View> events) {
        // Use TinyEvent to keep the records small.
        OutputTag<View> tag = new OutputTag("has-bot", TypeInformation.of(View.class));
        SingleOutputStreamOperator<View> result = add(
                events.process(new FilterOperator<>(BotUtil::isNotBot, tag)),
                "filter-view-is-bot");
        SingleOutputStreamOperator<TinyEvent> tinyDroppedDeliveryLogs = toTinyView(result.getSideOutput(tag), "map-is-bot-tiny-view");
        outputDroppedEvents("view-is-bot", TinyEvent::getLogTimestamp, tinyDroppedDeliveryLogs);
        return result;
    }

    private <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> filterOutIgnoreUsage(
            SingleOutputStreamOperator<T> events,
            String recordType,
            Class<T> clazz,
            SerializablePredicate<T> isNotIgnoreUsage,
            SerializableToLongFunction<T> getLogTimestamp) {
        OutputTag<T> tag = new OutputTag("filtered-out", TypeInformation.of(clazz));
        SingleOutputStreamOperator<T> result = add(
                events.process(new FilterOperator<>(SerializablePredicates.not(isNotIgnoreUsage), tag)),
                "filter-out-ignore-usage-" + recordType);
        outputDroppedEvents("ignore-usage-" + recordType,
                getLogTimestamp,
                result.getSideOutput(tag));
        return result;
    }

    private <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> filterBuyer(
            SingleOutputStreamOperator<T> events,
            String recordType,
            Class<T> clazz,
            SerializableFunction<T, Insertion> getApiExecutionInsertion,
            SerializableToLongFunction<T> getLogTimestamp) {
        if (nonBuyerUserSparseHashes.isEmpty()) {
            return events;
        }
        OutputTag<T> tag = new OutputTag("filtered-out", TypeInformation.of(clazz));
        SingleOutputStreamOperator<T> result = add(
                events.process(new FilterOperator<>(new BuyerPredicate<>(nonBuyerUserSparseHashes, getApiExecutionInsertion), tag)),
                "filter-buyer-" + recordType);
        outputDroppedEvents("non-buyer-" + recordType,
                getLogTimestamp,
                result.getSideOutput(tag));
        return result;
    }

    private static void setUidAndName(Transformation<?> transformation, String uid) {
        transformation.setUid(uid);
        transformation.setName(uid);
    }

    void outputJoinedEvents(
            String pathPrefix,
            DataStream<JoinedEvent> joinedImpressions,
            DataStream<JoinedEvent> flatUserImpressions,
            DataStream<JoinedEvent> joinedActions,
            DataStream<JoinedEvent> flatUserActions,
            DataStream<FlatResponseInsertion> flatResponseInsertions,
            DataStream<FlatResponseInsertion> flatUserResponseInsertions) {
        // TODO: fix latest impressions stuff to only attach to output needed for aws personalize
        // and our own modelling (if needed).  right now all flat action ouputs have them.

        // Strip our userId.
        joinedImpressions = add(joinedImpressions.map(UserInfoUtil::clearUserId), "clear-user-id-joined-impressions");
        joinedActions = add(joinedActions.map(UserInfoUtil::clearUserId), "clear-user-id-joined-actions");
        flatResponseInsertions = add(flatResponseInsertions.map(UserInfoUtil::clearUserId), "clear-user-id-flat-response-insertions");

        if (writeJoinedEventsToKafka) {
            // Note that we're sending both flat impression and action streams to the same topic
            flatOutputKafka.addJoinedEventSink(joinedImpressions.union(joinedActions), flatOutputKafka.getJoinedEventTopic(getJobLabel()));
            flatOutputKafka.addJoinedEventSink(flatUserImpressions.union(flatUserActions), flatOutputKafka.getJoinedUserEventTopic(getJobLabel()));
        }

        if (writeFlatResponseInsertionsToKafka) {
            flatOutputKafka.addFlatResponseInsertionSink(flatResponseInsertions, flatOutputKafka.getFlatResponseInsertionTopic(getJobLabel()));
            flatOutputKafka.addFlatResponseInsertionSink(flatUserResponseInsertions, flatOutputKafka.getFlatUserResponseInsertionTopic(getJobLabel()));
        }

        // TBD: we could break out s3 writing to its own job now.
        S3Path outputDir = s3FileOutput.getOutputS3Dir(pathPrefix).build();

        addSinkTransformations(s3FileOutput.sink(
                    joinedImpressions,
                    joinedEvent -> joinedEvent.getImpression().getTiming(),
                    outputDir.toBuilder().addSubDir("joined-impression")));
        addSinkTransformations(s3FileOutput.sink(
                    flatUserImpressions,
                    joinedEvent -> joinedEvent.getImpression().getTiming(),
                    outputDir.toBuilder().addSubDir("joined-user-impression")));
        addSinkTransformations(s3FileOutput.sink(
                    joinedActions,
                    joinedEvent -> joinedEvent.getAction().getTiming(),
                    outputDir.toBuilder().addSubDir("joined-action")));
        addSinkTransformations(s3FileOutput.sink(
                    flatUserActions,
                    joinedEvent -> joinedEvent.getAction().getTiming(),
                    outputDir.toBuilder().addSubDir("joined-user-action")));
        addSinkTransformations(s3FileOutput.sink(
                    flatResponseInsertions,
                    joinedEvent -> joinedEvent.getRequest().getTiming(),
                    outputDir.toBuilder().addSubDir("flat-response-insertion")));
        addSinkTransformations(s3FileOutput.sink(
                    flatUserResponseInsertions,
                    joinedEvent -> joinedEvent.getRequest().getTiming(),
                    outputDir.toBuilder().addSubDir("flat-user-response-insertion")));

        if (writeFlatActionedResponseInsertion) {
            DataStream<FlatResponseInsertion> flatActionedResponseInsertion =
                    add(flatResponseInsertions.filter(insertion -> insertion.getActionCount() > 0),
                            "filter-flat-actioned-response-insertion");
            addSinkTransformations(s3FileOutput.sink(
                    flatActionedResponseInsertion,
                    joinedEvent -> joinedEvent.getRequest().getTiming(),
                    outputDir.toBuilder().addSubDir("flat-actioned-response-insertion")));
        }
    }

    private static boolean hasActions(FlatResponseInsertion insertion) {
        return insertion.getActionCount() > 0;
    }

    private void outputDroppedMergeDetailsEvents(String name, DataStream<DroppedMergeDetailsEvent> droppedStream) {
        SingleOutputStreamOperator<DroppedMergeDetailsEvent> droppedEvents =
                add(droppedStream.map(UserInfoUtil::clearUserId), "clear-user-id-dropped-" + name);
        outputDroppedEvents(name, event -> event.getTinyEvent().getLogTimestamp(), droppedEvents);
    }

    private void outputLateEvents(String name, DataStream<TinyEvent> lateStream) {
        outputSideRecords(ImmutableList.of("late", name), TinyEvent::getLogTimestamp, lateStream);
    }

    private <T extends GeneratedMessageV3> void outputDroppedEvents(
            String name, SerializableToLongFunction<T> timestampGetter, DataStream<T> droppedStream) {
        outputSideRecords(ImmutableList.of("dropped", name), timestampGetter, droppedStream);
    }

    private <T extends GeneratedMessageV3> void outputDebugRecords(
            String name, SerializableToLongFunction<T> timestampGetter, DataStream<T> stream) {
        outputSideRecords(ImmutableList.of("debug", name), timestampGetter, stream);
    }

    private <T extends GeneratedMessageV3> void outputSideRecords(
            Iterable<String> subdirs, SerializableToLongFunction<T> timestampGetter, DataStream<T> stream) {
        addSinkTransformations(s3FileOutput.sink(
                timestampGetter,
                stream,
                s3FileOutput.getOutputS3Dir(ImmutableList.<String>builder().add("etl-side").addAll(subdirs).build())));
    }

    @VisibleForTesting
    void outputAllDeliveryLogVariants(DataStream<DeliveryLog> allDeliveryLogs) {
        DataStream<DeliveryLog> metadataStream = add(
                allDeliveryLogs.map(deliveryLog -> {
                    DeliveryLog.Builder builder = deliveryLog.toBuilder();
                    builder.getRequestBuilder().clearInsertion();
                    builder.getRequestBuilder().clearInsertionMatrix();
                    builder.getResponseBuilder().clearInsertion();
                    builder.getExecutionBuilder().clearExecutionInsertion();
                    UserInfoUtil.clearUserId(builder);
                    return builder.build();
                }),
                "strip-all-delivery-log");
        outputAllDeliveryLog(metadataStream, "all-delivery-log-metadata");

        DataStream<Request> requestStream = add(
                allDeliveryLogs.map((deliveryLog) -> UserInfoUtil.clearUserId(deliveryLog.getRequest().toBuilder()).build()),
                "map-to-request-and-clear-user-id-all-delivery-log-request");
        outputAllDeliveryLogRequest(requestStream, "all-delivery-log-request");
        DataStream<Request> requestMetadataStream = add(
                requestStream.map(request -> request.toBuilder().clearInsertion().clearInsertionMatrix().build()),
                "clear-insertion-all-delivery-log-request");
        outputAllDeliveryLogRequest(requestMetadataStream, "all-delivery-log-request-metadata");
    }

    // This writes out the combined v1 (synthetic DeliveryLog formed from Request and response Insertion) and v2 DeliveryLog.
    // This function is unit tested.
    @VisibleForTesting
    void outputAllDeliveryLog(DataStream<DeliveryLog> allDeliveryLogs, String name) {
        addSinkTransformations(s3FileOutput.sink(
                allDeliveryLogs,
                deliveryLog -> deliveryLog.getRequest().getTiming(),
                s3FileOutput.getOutputS3Dir("etl-side", "debug", name)));
    }

    // TODO - refactor these methods after PR/929 is merged.
    // Writes debug Request files out to S3 Avro files.
    @VisibleForTesting
    void outputAllDeliveryLogRequest(DataStream<Request> requests, String name) {
        addSinkTransformations(s3FileOutput.sink(
                requests,
                request -> request.getTiming(),
                s3FileOutput.getOutputS3Dir("etl-side", "debug", name)));
    }

    @VisibleForTesting
    void outputPartialResponseInsertion(DataStream<TinyEvent> partialResponseInsertion) {
        addSinkTransformations(s3FileOutput.sink(
                flat -> flat.getLogTimestamp(),
                partialResponseInsertion,
                s3FileOutput.getOutputS3Dir("etl-side", "debug", "partial-response-insertion")));
    }

    private void outputValidationError(DataStream<ValidationError> errorStream) {
        addSinkTransformation(s3FileOutput.outputSpecificAvroRecordParquet(
                errorStream,
                ValidationError.class,
                error -> error.getTiming().getEventApiTimestamp(),
                // TODO - we'll probably want this exposed externally.  Might make sense to have in etl.
                s3FileOutput.getOutputS3Dir("etl-side", "validation-error").build()));
    }

    private void outputMismatchError(DataStream<MismatchError> errors) {
        addSinkTransformation(s3FileOutput.outputSpecificAvroRecordParquet(
                errors,
                MismatchError.class,
                error -> error.getLogTimestamp(),
                s3FileOutput.getOutputS3Dir("etl-side", "mismatch-error").build()));
    }
}
