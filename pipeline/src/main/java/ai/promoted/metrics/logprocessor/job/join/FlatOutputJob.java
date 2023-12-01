package ai.promoted.metrics.logprocessor.job.join;

import static ai.promoted.metrics.logprocessor.common.util.TableUtil.getLabeledDatabaseName;

import ai.promoted.metrics.common.RedundantAction;
import ai.promoted.metrics.common.RedundantImpression;
import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.error.MismatchErrorTag;
import ai.promoted.metrics.logprocessor.common.flink.operator.KeyedProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.flink.operator.ProcessOperatorWithWatermarkAddition;
import ai.promoted.metrics.logprocessor.common.functions.CombineDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.DeliveryExecutionUtil;
import ai.promoted.metrics.logprocessor.common.functions.FilterOperator;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.PopulatePagingId;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicates;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.filter.BuyerPredicate;
import ai.promoted.metrics.logprocessor.common.functions.inferred.IsImpressionAction;
import ai.promoted.metrics.logprocessor.common.functions.inferred.IsImpressionActionPath;
import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeActionDetails;
import ai.promoted.metrics.logprocessor.common.functions.inferred.MergeImpressionDetails;
import ai.promoted.metrics.logprocessor.common.functions.inferred.ToTinyAttributedAction;
import ai.promoted.metrics.logprocessor.common.functions.redundantevent.ReduceRedundantTinyActions;
import ai.promoted.metrics.logprocessor.common.functions.redundantevent.ReduceRedundantTinyImpressions;
import ai.promoted.metrics.logprocessor.common.functions.redundantevent.RedundantEventKeys;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkTableJob;
import ai.promoted.metrics.logprocessor.common.job.ContentApiSegment;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.FlatOutputKafkaSink;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.KeepFirstSegment;
import ai.promoted.metrics.logprocessor.common.job.MergeDetailsOutputs;
import ai.promoted.metrics.logprocessor.common.job.RegionSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.paimon.PaimonSegment;
import ai.promoted.metrics.logprocessor.common.s3.S3Path;
import ai.promoted.metrics.logprocessor.common.util.BotUtil;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.metrics.logprocessor.common.util.TinyFlatUtil;
import ai.promoted.metrics.logprocessor.job.join.action.ActionJoinSegment;
import ai.promoted.metrics.logprocessor.job.join.action.ActionPathAndDropped;
import ai.promoted.metrics.logprocessor.job.join.action.UnnestTinyAction;
import ai.promoted.metrics.logprocessor.job.join.impression.ImpressionJoinOutputs;
import ai.promoted.metrics.logprocessor.job.join.impression.ImpressionJoinSegment;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.Response;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.DroppedMergeActionDetails;
import ai.promoted.proto.event.DroppedMergeImpressionDetails;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.UnnestedTinyAction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "flatoutput",
    mixinStandardHelpOptions = true,
    version = "flatoutput 1.0.0",
    description =
        "Creates a Flink job that reads LogRequests from Kafka, fills in defaults,"
            + " and produces flat event messages to Kafka.")
public class FlatOutputJob extends BaseFlinkTableJob {
  private static final Logger LOGGER = LogManager.getLogger(FlatOutputJob.class);
  private static final TypeInformation<TinyInsertion> tinyInsertionTypeInfo =
      TypeInformation.of(TinyInsertion.class);
  private static final TypeInformation<TinyActionPath> tinyActionPathTypeInfo =
      TypeInformation.of(TinyActionPath.class);
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final KafkaSinkSegment kafkaSinkSegment = new KafkaSinkSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final FlatOutputKafkaSegment flatOutputKafkaSegment =
      new FlatOutputKafkaSegment(this, kafkaSegment);

  public final FlatOutputKafkaSink flatOutputKafkaSink =
      new FlatOutputKafkaSink(this, flatOutputKafkaSegment, kafkaSinkSegment);
  public final ValidatedEventKafkaSegment validatedEventKafkaSegment =
      new ValidatedEventKafkaSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final DirectValidatedEventKafkaSource directValidatedEventKafkaSource =
      new DirectValidatedEventKafkaSource(kafkaSourceSegment, validatedEventKafkaSegment);

  @CommandLine.Mixin public final KeepFirstSegment keepFirstSegment = new KeepFirstSegment(this);
  public final ValidatedEventKafkaSource validatedEventKafkaSource =
      new ValidatedEventKafkaSource(
          directValidatedEventKafkaSource, validatedEventKafkaSegment, keepFirstSegment);
  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);
  @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this, s3);
  @CommandLine.Mixin public final RegionSegment regionSegment = new RegionSegment();

  @CommandLine.Mixin
  public final ContentApiSegment contentApiSegment = new ContentApiSegment(this, regionSegment);

  @CommandLine.Mixin public final PaimonSegment paimonSegment = new PaimonSegment(this);

  @CommandLine.Mixin
  public final ImpressionJoinSegment impressionJoinSegment = new ImpressionJoinSegment();

  @CommandLine.Mixin public final ActionJoinSegment actionJoinSegment = new ActionJoinSegment();

  @FeatureFlag
  @Option(
      names = {"--no-writeJoinedEventsToKafka"},
      negatable = true,
      description = "Whether to write flat events to Kafka.  Default=true")
  public boolean writeJoinedEventsToKafka = true;

  @Option(
      names = {"--no-checkLateness"},
      negatable = true,
      description = "Whether to check events for lateness and ordering.")
  public boolean checkLateness = true;

  @Option(
      names = {"--combineDeliveryLogWindow"},
      defaultValue = "PT2S",
      description =
          "Window for merging DeliveryLogs with the same clientRequestId. The longer the window, the slower the output.  This impacts the downstream joinMaxes. Default=PT2S. Java8 Duration parse format.")
  public Duration combineDeliveryLogWindow = Duration.parse("PT2S");

  @FeatureFlag
  @CommandLine.Option(
      names = {"--writePaimonTables"},
      negatable = true,
      description = "Whether to write data to Paimon tables.  Default=false")
  public boolean writePaimonTables = false;

  // CR: i'm inclined to just use the max of the impressionActionJoinM* flag values.
  @Option(
      names = {"--flatResponseInsertionGapDuration"},
      defaultValue = "P1D",
      description =
          "Gap duration to indicate a completed FlatResponseInsertion. This should be equal to the max impression action join duration.  Default=P1D")
  public Duration flatResponseInsertionGapDuration = Duration.parse("P1D");

  // Keep first durations
  @Option(
      names = {"--mergeDetailsCleanupPeriod"},
      defaultValue = "PT30S",
      description =
          "Duration for which to delay TinyEvents where we don't have full entities.  This delay probably isn't needed.  It might catch small issues if we stream realtime through inferred refs. Default=PT1S.")
  public Duration mergeDetailsCleanupPeriod = Duration.parse("PT30S");

  @Option(
      names = {"--mergeImpressionDetailsMissingOutputDelay"},
      defaultValue = "PT0S",
      description =
          "Duration for which to delay Tiny Impressions where we don't have full entities.  This delay probably isn't needed.  It might catch small issues if we stream realtime through inferred refs. Default=PT0S.")
  public Duration mergeImpressionDetailsMissingOutputDelay = Duration.parse("PT0S");

  @Option(
      names = {"--mergeActionDetailsMissingOutputDelay"},
      defaultValue = "PT0S",
      description =
          "Duration for which to delay Tiny Actions where we don't have full entities.  This delay probably isn't needed.  It might catch small issues if we stream realtime through inferred refs. Default=PT0S.")
  public Duration mergeActionDetailsMissingOutputDelay = Duration.parse("PT0S");

  @Option(
      names = {"--mergeDetailsCleanupBuffer"},
      defaultValue = "PT30S",
      description =
          "Additional Duration added to the clean-ups of events.  This is extra padding. Increasing this means full state lives longer in our system. Default=PT30S.")
  public Duration mergeDetailsCleanupBuffer = Duration.parse("PT30S");

  @Option(
      names = {"--batchCleanupAllMergeDetailsTimersBeforeTimestamp"},
      defaultValue = "0",
      description =
          "Used to force cleanup on all timers before timestamp.  Changing cleanup intervals can cause state to be leaked.  This flag can be used when changing intervals to avoid leaks.  Defaults to 0")
  public long batchCleanupAllMergeDetailsTimersBeforeTimestamp = 0;

  @FeatureFlag
  @Option(
      names = {"--no-writeMismatchError"},
      negatable = true,
      description =
          "Whether to write MismatchError s3 Parquet files.  This is a flag in case this writing causes performance issues.  Defaults to true.")
  public boolean writeMismatchError = true;

  @FeatureFlag
  @Option(
      names = {"--nonBuyerUserSparseHash"},
      description =
          "Filters out flat events that contain these Sparse IDs with value=1.  Defaults to empty.")
  public List<Long> nonBuyerUserSparseHashes = new ArrayList<Long>();

  @Option(
      names = {"--textLogWatermarks"},
      negatable = true,
      description = "Whether to text log watermarks in certain operators.  Defaults to false.")
  public boolean textLogWatermarks = false;

  @Option(
      names = {"--debugOutputBeforeReductionTinyEvents"},
      negatable = true,
      description =
          "Whether to log the before-redundant-reduction tiny events.  Defaults to false to reduce data lake write volumes.")
  public boolean debugOutputBeforeReductionTinyEvents = false;

  public static void main(String[] args) {
    executeMain(new FlatOutputJob(), args);
  }

  private static Time toFlinkTime(Duration duration) {
    return Time.milliseconds(duration.toMillis());
  }

  private static Duration add(Duration... durations) {
    long millis = 0;
    for (Duration duration : durations) {
      millis += Math.abs(duration.toMillis());
    }
    return Duration.ofMillis(millis);
  }

  private static void setUidAndName(Transformation<?> transformation, String uid) {
    transformation.setUid(uid);
    transformation.setName(uid);
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        kafkaSinkSegment,
        flatOutputKafkaSegment,
        flatOutputKafkaSink,
        validatedEventKafkaSegment,
        directValidatedEventKafkaSource,
        validatedEventKafkaSource,
        s3,
        s3FileOutput,
        contentApiSegment);
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    Set<Class<? extends GeneratedMessageV3>> protoClasses =
        ImmutableSet.<Class<? extends GeneratedMessageV3>>builder()
            .addAll(super.getProtoClasses())
            // The join job serializes these types.  The segments do not serialize them.
            .add(CombinedDeliveryLog.class)
            .add(Request.class)
            .add(Response.class)
            .add(Insertion.class)
            .add(TinyAction.class)
            .add(TinyActionPath.class)
            .add(TinyJoinedImpression.class)
            .add(TinyDeliveryLog.class)
            .add(TinyInsertion.class)
            .add(TinyImpression.class)
            .add(UnionEvent.class)
            .build();
    return protoClasses;
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    setTableEnv(tEnv);
    String kafkaGroupId = toKafkaConsumerGroupId("join");

    // TODO: deal with missing log user ids for pre-luid events
    executeJoinEvents(
        validatedEventKafkaSource.getDeliveryLogSource(env, kafkaGroupId),
        validatedEventKafkaSource.getImpressionSource(env, kafkaGroupId),
        validatedEventKafkaSource.getActionSource(env, kafkaGroupId));
    prepareToExecute();
    LOGGER.info("{}.executionPlan\n{}", getJobName(), env.getExecutionPlan());
    env.execute(getJobName());
  }

  @Override
  public void tableOperationsToDataStream() {
    paimonSegment.getStatementSet().attachAsDataStream();
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    Preconditions.checkArgument(
        !mergeImpressionDetailsMissingOutputDelay.isNegative(),
        "--mergeImpressionDetailsMissingOutputDelay should not be negative.");
    Preconditions.checkArgument(
        !mergeActionDetailsMissingOutputDelay.isNegative(),
        "--mergeActionDetailsMissingOutputDelay should not be negative.");
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "join-event";
  }

  /** Known edge cases: - late correct LHS with the same surrogate key of an existing LHS */
  @VisibleForTesting
  void executeJoinEvents(
      DataStream<DeliveryLog> deliveryLog,
      DataStream<Impression> impression,
      DataStream<Action> action)
      throws Exception {

    // TODO - have a better output file format that is easier to analyze.
    // - error_type
    // - the raw record.

    // TODO - detect and output late events here.
    /*
    outputLateEvents(
        "insertion_impression_insertion",
        LateTinyInsertion::getFlinkEventTimestamp,
        outputs.lateInsertions());
    outputLateEvents(
        "insertion_impression_impression",
        LateTinyImpression::getFlinkEventTimestamp,
        outputs.lateImpressions());
     */

    // TODO - remove TinyView from protos.

    // TODO - remove View from the join protos.  We're not actively using it.  It simplifies the
    // schemas.
    SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLogs =
        toCombinedDeliveryLogs(deliveryLog);

    SingleOutputStreamOperator<TinyInsertion> tinyInsertions =
        toTinyInsertion(combinedDeliveryLogs);
    SingleOutputStreamOperator<TinyImpression> tinyImpressions = toTinyImpression(impression);

    SingleOutputStreamOperator<TinyJoinedImpression> tinyJoinedImpressions =
        joinImpressions(tinyInsertions, tinyImpressions);

    // TODO - it's weird to unnest here and later with other_content_ids.
    SingleOutputStreamOperator<TinyAction> tinyActions = toTinyAction(action);
    outputDebugRecords(
        "rhs_tiny_action",
        tinyAction -> tinyAction.getCommon().getEventApiTimestamp(),
        "common.event_api_timestamp",
        tinyActions);

    // Unnest to simplify the next joins.
    // TODO - have a better materialized proto.
    // TODO - evaluate doing other_content_id lookups, unnesting and cart unnesting in Flink SQL.

    // TODO - Xingcan: this is not used any more. We can safely remove the logic after some tests.
    // SingleOutputStreamOperator<UnnestedTinyJoinedImpression> unnestedJoinedImpressions =
    //    add(
    //        tinyJoinedImpressions.process(new MaterializeAndUnnestJoinedImpression()),
    //        "materialize-and-unnest-joined-impression");
    SingleOutputStreamOperator<UnnestedTinyAction> unnestedTinyActions =
        add(tinyActions.process(new UnnestTinyAction()), "unnest-tiny-action");

    DataStream<TinyActionPath> actionPaths =
        joinActions(tinyJoinedImpressions, unnestedTinyActions);

    /*
    outputLateEvents(
        "impression_action_path_impressions",
        LateTinyJoinedImpression::getFlinkEventTimestamp,
        unkeyedInsertionToActionPaths.getSideOutput(
            ImpressionActionProcessFunction.LATE_TINY_JOINED_IMPRESSION_TAG));
    outputLateEvents(
        "impression_action_actions",
        LateTinyAction::getFlinkEventTimestamp,
        unkeyedInsertionToActionPaths.getSideOutput(
            ImpressionActionProcessFunction.LATE_TINY_ACTION_TAG));
     */
    // Union the streams so we don't need to maintain as much duplicate state.
    SingleOutputStreamOperator<TinyJoinedImpression> tinyJoinedImpression =
        add(
            tinyJoinedImpressions
                .keyBy(RedundantEventKeys::forRedundantImpressionInImpressionStream)
                .connect(
                    actionPaths.keyBy(RedundantEventKeys::forRedundantImpressionInActionPathStream))
                .process(
                    new ReduceRedundantTinyImpressions<>(
                        actionJoinSegment.impressionActionJoinMin.abs())),
            "reduce-redundant-impressions");

    SingleOutputStreamOperator<TinyAttributedAction> tinyAttributedAction =
        toTinyAttributedAction(tinyJoinedImpression);

    enrichFilterAndFlatten(
        tinyJoinedImpression, tinyAttributedAction, combinedDeliveryLogs, impression, action);
  }

  // TODO - We could reduce load in extreme edge cases by removing some redundant events earlier
  // in the join job. E.g. a user seeing the same item multiple times on repeat.
  // This gets tricky based on how we want to deduplicate.

  // TODO - check that event time works correctly.  Might be broken.
  // TODO - Make sure CombineDeliveryLog doesn't delay event times.

  private SingleOutputStreamOperator<TinyJoinedImpression> joinImpressions(
      SingleOutputStreamOperator<TinyInsertion> unkeyedInsertions,
      SingleOutputStreamOperator<TinyImpression> impressions) {
    outputPartialResponseInsertion(unkeyedInsertions);

    ImpressionJoinOutputs outputs =
        impressionJoinSegment.joinImpressions(
            this, this.tEnv, unkeyedInsertions, impressions, textLogWatermarks);

    outputDroppedEvents(
        "insertion_impressions",
        tinyImpression -> tinyImpression.getCommon().getEventApiTimestamp(),
        "common.event_api_timestamp",
        outputs.droppedImpressions());

    if (debugOutputBeforeReductionTinyEvents) {
      outputDebugRecords(
          "beforereduce_tiny_joined_impression",
          tinyImpression -> tinyImpression.getImpression().getCommon().getEventApiTimestamp(),
          "impression.common.event_api_timestamp",
          outputs.joinedImpressions());
    }

    return outputs.joinedImpressions();
  }

  private DataStream<TinyActionPath> joinActions(
      SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions,
      SingleOutputStreamOperator<UnnestedTinyAction> actions) {
    ActionPathAndDropped outputs =
        actionJoinSegment.joinActions(
            this, this.tEnv, joinedImpressions, actions, textLogWatermarks);

    outputDroppedEvents(
        "impression_actions",
        action -> action.getCommon().getEventApiTimestamp(),
        "common.event_api_timestamp",
        outputs.droppedActions());

    if (debugOutputBeforeReductionTinyEvents) {
      outputDebugRecords(
          "beforereduce_tiny_action_path",
          actionPath -> actionPath.getAction().getCommon().getEventApiTimestamp(),
          "action.common.event_api_timestamp",
          outputs.actionPaths());
    }

    return outputs.actionPaths();
  }

  /**
   * The second phase of the join. The first phase validates and joins the stream of TinyEvents
   * together.
   *
   * <p>The second phase: 1. Converts the TinyEvents back to fully joined details. 2. Filters out
   * events. 3. Produced joined and flat event records.
   *
   * <p>This section is structured as a separate method with a lot of parameters so we can
   * eventually refactor it into a different Flink job. This is difficult right now because the
   * validation logic is coupled to the first part of the job.
   *
   * @param tinyJoinedImpression tiny JoinedImpression stream
   * @param tinyAttributedAction tiny AttributedAction stream
   * @param combinedDeliveryLog Post validation and filtering
   * @param impression Post validation and filtering
   * @param action Post validation and filtering
   */
  private void enrichFilterAndFlatten(
      SingleOutputStreamOperator<TinyJoinedImpression> tinyJoinedImpression,
      SingleOutputStreamOperator<TinyAttributedAction> tinyAttributedAction,
      SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLog,
      DataStream<Impression> impression,
      DataStream<Action> action) {
    MergeDetailsOutputs<JoinedImpression> mergeImpressionDetailsOutput =
        mergeImpressionDetails(combinedDeliveryLog, impression, tinyJoinedImpression);

    // TODO - If we don't do the data lake, we can shift the MergeActionDetails to be a normal
    // interval join.
    // MergeImpressionDetails is still useful because we can consolidate DeliveryLog state into
    // fewer denormalized
    // copies by doing a special join.
    MergeDetailsOutputs<AttributedAction> mergeActionDetailsOutput =
        mergeActionDetails(
            mergeImpressionDetailsOutput.joinedEvents(), action, tinyAttributedAction);

    DataStream<MismatchError> mismatchErrors =
        mergeImpressionDetailsOutput
            .mismatchErrors()
            .union(mergeActionDetailsOutput.mismatchErrors());

    // Currently filters out non-buyer traffic.  We might add more over time.
    // It's important to do this after the full join so we don't get a lot of dropped records.
    // This is fine.  The volume is currently low.
    //
    // TODO - filter out these events earlier.  We can include the filter conditions inside
    // TinyEvent and avoid the MergeDetails.
    SingleOutputStreamOperator<JoinedImpression> filteredJoinedImpression =
        filterJoinedImpression(mergeImpressionDetailsOutput.joinedEvents());

    SingleOutputStreamOperator<AttributedAction> filteredAttributedAction =
        filterAttributedAction(mergeActionDetailsOutput.joinedEvents());

    joinUserAndOutput(filteredJoinedImpression, filteredAttributedAction, mismatchErrors);
  }

  /** Merges details onto tiny impressions. */
  private MergeDetailsOutputs<JoinedImpression> mergeImpressionDetails(
      SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLog,
      DataStream<Impression> impression,
      DataStream<TinyJoinedImpression> tinyImpression) {
    DataStream<UnionEvent> unkeyedFullUnionEvent =
        toUnionEventForImpressions(combinedDeliveryLog, impression);
    KeyedStream<UnionEvent, Tuple2<Long, String>> fullUnionEvent =
        unkeyedFullUnionEvent.keyBy(KeyUtil.unionEntityKeySelector);
    KeyedStream<TinyJoinedImpression, Tuple2<Long, String>> keyedTinyImpression =
        tinyImpression.keyBy(KeyUtil.tinyJoinedImpressionAnonUserIdKey);
    SingleOutputStreamOperator<JoinedImpression> joinedImpression =
        add(
            fullUnionEvent.connect(keyedTinyImpression).process(createMergeImpressionDetails()),
            "merge-impression-details");
    SideOutputDataStream<DroppedMergeImpressionDetails> dropped =
        joinedImpression.getSideOutput(MergeImpressionDetails.DROPPED_TAG);

    // Delay the watermark by the delay for missingOutputDelay.
    joinedImpression =
        add(
            ProcessOperatorWithWatermarkAddition.addWatermarkDelay(
                joinedImpression,
                TypeInformation.of(JoinedImpression.class),
                mergeImpressionDetailsMissingOutputDelay.negated(),
                "delay-merge-impression-details-watermark",
                textLogWatermarks),
            "delay-merge-impression-details-watermark");

    outputDroppedMergeDetailsEvents(
        "merge_impression_details",
        details -> details.getImpression().getImpression().getCommon().getEventApiTimestamp(),
        "impression.impression.common.event_api_timestamp",
        dropped);
    return MergeDetailsOutputs.create(
        joinedImpression, joinedImpression.getSideOutput(MismatchErrorTag.TAG));
  }

  /** Merges details onto tiny impressions. */
  private MergeDetailsOutputs<AttributedAction> mergeActionDetails(
      SingleOutputStreamOperator<JoinedImpression> joinedImpression,
      DataStream<Action> action,
      SingleOutputStreamOperator<TinyAttributedAction> tinyAttributedActions) {
    DataStream<UnionEvent> unkeyedFullUnionEvent = toUnionEventForActions(joinedImpression, action);
    KeyedStream<UnionEvent, Tuple2<Long, String>> fullUnionEvent =
        unkeyedFullUnionEvent.keyBy(KeyUtil.unionEntityKeySelector);
    KeyedStream<TinyAttributedAction, Tuple2<Long, String>> keyedTinyAttributedAction =
        tinyAttributedActions.keyBy(KeyUtil.tinyAttributedActionAnonUserIdKey);
    SingleOutputStreamOperator<AttributedAction> attributedAction =
        add(
            fullUnionEvent.connect(keyedTinyAttributedAction).process(createMergeActionDetails()),
            "merge-action-details");
    SideOutputDataStream<DroppedMergeActionDetails> dropped =
        attributedAction.getSideOutput(MergeActionDetails.DROPPED_TAG);

    // Delay the watermark by the delay for missingOutputDelay.
    attributedAction =
        add(
            ProcessOperatorWithWatermarkAddition.addWatermarkDelay(
                attributedAction,
                TypeInformation.of(AttributedAction.class),
                mergeActionDetailsMissingOutputDelay.negated(),
                "delay-merge-action-details-watermark",
                textLogWatermarks),
            "delay-merge-action-details-watermark");

    outputDroppedMergeDetailsEvents(
        "merge_action_details",
        details -> details.getAction().getAction().getCommon().getEventApiTimestamp(),
        "action.action.common.event_api_timestamp",
        dropped);
    return MergeDetailsOutputs.create(
        attributedAction, attributedAction.getSideOutput(MismatchErrorTag.TAG));
  }

  private SingleOutputStreamOperator<CombinedDeliveryLog> toCombinedDeliveryLogs(
      DataStream<DeliveryLog> deliveryLog) {
    // Remove DeliveryLogs that we do not want to consider in inferred refs.
    // Do this before writing to the side.all_delivery_log... so we can improve throughput.
    SingleOutputStreamOperator<DeliveryLog> filteredDeliveryLogs = filterShouldJoin(deliveryLog);
    filteredDeliveryLogs = filterNonBotDeliveryLogs(filteredDeliveryLogs);
    // In Paimon, we'll use the main DeliveryLog table and remove these side tables.
    if (!writePaimonTables) {
      outputAllDeliveryLogVariants(filteredDeliveryLogs);
    }

    SingleOutputStreamOperator<CombinedDeliveryLog> combinedDeliveryLogs =
        add(
            filteredDeliveryLogs
                .keyBy(KeyUtil.deliveryLogAnonUserIdKey)
                // Support a simple PassThroughCombineDeliveryLog for tests.
                // We need to delay the Watermark since CombineDeliveryLog can delay outputs up to
                // the window.
                // If we do not delay the watermark, the inferred reference code will mark the
                // delayed output as late.
                // Warning - this is a weird spot to delay for synthetic DeliveryLogs.
                // Trying to delay this closer to window/aggregate code fails with a message saying
                // the timer service has not been initialized.
                .transform(
                    "combine-delivery-log",
                    TypeInformation.of(CombinedDeliveryLog.class),
                    KeyedProcessOperatorWithWatermarkAddition.withDelay(
                        new CombineDeliveryLog(getDebugIds()),
                        combineDeliveryLogWindow,
                        textLogWatermarks)),
            "combine-delivery-log");

    // PopulatingPagingId is done after CombineDeliveryLog so the paging_id can be copied from
    // the API DeliveryLog to the SDK DeliveryLog.
    //
    // TODO - this would be slightly cleaner if done inside the ValidateEnrichJob.  That'd require
    // running the same create paging_id function inside ValidateEnrichJob as in Delivery C++.
    // That's a lot of work for very little value.
    combinedDeliveryLogs =
        add(combinedDeliveryLogs.map(new PopulatePagingId()), "populating-paging-id");
    return combinedDeliveryLogs;
  }

  /** Used to have small dropped side outputs for DeliveryLogs. */
  private SingleOutputStreamOperator<TinyDeliveryLog> toTinyDeliveryLogRequest(
      DataStream<DeliveryLog> deliveryLogs, String uid) {
    return add(
        deliveryLogs.map(
            deliveryLog -> {
              Request request = deliveryLog.getRequest();
              return TinyDeliveryLog.newBuilder()
                  .setCommon(
                      TinyCommonInfo.newBuilder()
                          .setPlatformId(DeliveryLogUtil.getPlatformId(deliveryLog))
                          .setAnonUserId(request.getUserInfo().getAnonUserId())
                          .setEventApiTimestamp(request.getTiming().getEventApiTimestamp()))
                  .setViewId(request.getViewId())
                  .setRequestId(request.getRequestId())
                  .build();
            }),
        uid);
  }

  private SingleOutputStreamOperator<TinyInsertion> toTinyInsertion(
      DataStream<CombinedDeliveryLog> combinedDeliveryLogs) {
    SingleOutputStreamOperator<TinyDeliveryLog> tinyDeliveryLog =
        add(
            combinedDeliveryLogs.map(new ToTinyDeliveryLog(contentApiSegment.contentIdFieldKeys)),
            "map-tiny-delivery-log");
    KeyedStream<TinyDeliveryLog, Tuple2<Long, String>> smallDeliveryLogInput =
        tinyDeliveryLog.keyBy(KeyUtil.tinyDeliveryLogAnonUserIdKey);
    SingleOutputStreamOperator<TinyInsertion> tinyInsertions =
        add(
            smallDeliveryLogInput.flatMap(
                (dl, out) -> {
                  List<TinyInsertion.Builder> builders =
                      TinyFlatUtil.createTinyFlatResponseInsertions(TinyInsertion.newBuilder(), dl);
                  for (TinyInsertion.Builder builder : builders) {
                    out.collect(builder.build());
                  }
                },
                TypeInformation.of(TinyInsertion.class)),
            "to-tiny-insertion");

    // TODO - how is the watermark impacted?
    if (contentApiSegment.shouldJoinInsertionOtherContentIds()) {
      tinyInsertions =
          add(
              contentApiSegment.joinOtherInsertionContentIdsFromContentService(
                  tinyInsertions.keyBy(KeyUtil.tinyInsertionContentIdKey)),
              "add-insertion-other-content-ids");
    }
    return tinyInsertions;
  }

  private SingleOutputStreamOperator<TinyImpression> toTinyImpression(
      DataStream<Impression> impressions) {
    return add(impressions.map(new ToTinyImpression()), "map-tiny-impression");
  }

  private SingleOutputStreamOperator<TinyAction> toTinyAction(DataStream<Action> actions) {
    SingleOutputStreamOperator<TinyAction> tinyActions =
        add(
            actions.flatMap(new ToTinyAction(contentApiSegment.contentIdFieldKeys)),
            "map-tiny-action");
    if (contentApiSegment.shouldJoinActionOtherContentIds()) {
      KeyedStream<TinyAction, Tuple2<Long, String>> keyedContentIdTinyAction =
          tinyActions.keyBy(KeyUtil.tinyActionContentIdKey);
      tinyActions =
          add(
              contentApiSegment.joinOtherActionContentIdsFromContentService(
                  keyedContentIdTinyAction),
              "add-action-other-content-ids");
    }
    return tinyActions;
  }

  private SingleOutputStreamOperator<TinyAttributedAction> toTinyAttributedAction(
      SingleOutputStreamOperator<TinyJoinedImpression> tinyJoinedImpression) {

    outputDroppedEventsParquet(
        "redundant_impression",
        RedundantImpression::getEventApiTimestamp,
        tinyJoinedImpression.getSideOutput(ReduceRedundantTinyImpressions.REDUNDANT_IMPRESSION),
        RedundantImpression.class);

    OutputTag<TinyActionPath> tinyPostNavigateActionPathTag =
        new OutputTag<>("post-navigate-action-path", TypeInformation.of(TinyActionPath.class));
    IsImpressionAction isImpressionAction =
        new IsImpressionAction(
            actionJoinSegment.impressionActionTypes, actionJoinSegment.impressionCustomActionTypes);
    SingleOutputStreamOperator<TinyActionPath> tinyImpressionActionPath =
        add(
            tinyJoinedImpression
                .getSideOutput(ReduceRedundantTinyImpressions.OUTPUT_ACTION_PATH)
                .process(
                    new FilterOperator<>(
                        new IsImpressionActionPath(isImpressionAction),
                        tinyPostNavigateActionPathTag)),
            "split-tiny-action-path");

    SideOutputDataStream<TinyActionPath> tinyPostNavigateActionPath =
        tinyImpressionActionPath.getSideOutput(tinyPostNavigateActionPathTag);

    // "IsImpression" Actions have a different reduction key since they're reduced by
    // insertion_id instead of the content_id.  This allows sessions with multiple Impressions of
    // the same content_id to multiple NAVIGATES attributed to them but post-NAVIGATE actions
    // are reduced across multiple Impressions.
    SingleOutputStreamOperator<TinyActionPath> reducedTinyIsImpressionActionPath =
        add(
            tinyImpressionActionPath
                .keyBy(RedundantEventKeys::forRedundantActionsInImpressionActionPathStream)
                .process(
                    new ReduceRedundantTinyActions<>(
                        actionJoinSegment.impressionActionJoinMin.abs(), getDebugIds())),
            "reduce-redundant-actions-in-impression-action-paths");

    SingleOutputStreamOperator<TinyActionPath> reducedTinyPostNavigateActionPath =
        add(
            tinyPostNavigateActionPath
                .keyBy(RedundantEventKeys::forRedundantActionsInPostNavigateActionStream)
                .process(
                    new ReduceRedundantTinyActions<>(
                        actionJoinSegment.impressionActionJoinMin.abs(), getDebugIds())),
            "reduce-redundant-actions-in-post-navigate-action-paths");

    DataStream<TinyActionPath> reducedTinyActionPath =
        reducedTinyIsImpressionActionPath.union(reducedTinyPostNavigateActionPath);

    outputDroppedEventsParquet(
        "redundant_action",
        RedundantAction::getEventApiTimestamp,
        reducedTinyIsImpressionActionPath
            .getSideOutput(ReduceRedundantTinyActions.REDUNDANT_ACTION)
            .union(
                reducedTinyPostNavigateActionPath.getSideOutput(
                    ReduceRedundantTinyActions.REDUNDANT_ACTION)),
        RedundantAction.class);

    // TODO - remove "tiny_" and add a different identifier to the denormalized versions.
    outputRecords(
        "tiny_joined_impression",
        joinedImpression -> joinedImpression.getImpression().getCommon().getEventApiTimestamp(),
        "impression.common.event_api_timestamp",
        tinyJoinedImpression);
    outputRecords(
        "tiny_action_path",
        actionPath -> actionPath.getAction().getCommon().getEventApiTimestamp(),
        "action.common.event_api_timestamp",
        reducedTinyActionPath);

    SingleOutputStreamOperator<TinyAttributedAction> tinyAttributedAction =
        add(
            reducedTinyActionPath.flatMap(new ToTinyAttributedAction()),
            "to-tiny-attributed-action");

    outputRecords(
        "tiny_attributed_action",
        attributedAction -> attributedAction.getAction().getCommon().getEventApiTimestamp(),
        "action.common.event_api_timestamp",
        tinyAttributedAction);
    return tinyAttributedAction;
  }

  /**
   * TODO - try to switch to a built-in join.
   *
   * <p>Currently, multiple things are broken.
   *
   * <p>Event times are inconsistent and wrong. This impacts the downstream operators.
   *
   * <ul>
   *   <li>For full events, it depends on the last input time.
   *   <li>For incomplete events, it depends on the event time + missingOutputDelay.
   * </ul>
   *
   * <p>Downstream watermarks are not delayed for the missingOutputDelays.
   *
   * <p>It's unclear how late events will be handled. Depending on the ordering of inputs, it can
   * either get fully populated or partially (and dropped). If multiple inputs for a key are
   * delayed, delaying the timers has an issue since the tiny event time onTimer will fire before
   * all of the data is present. The dropped records do not easily indicate why.
   */
  private MergeImpressionDetails createMergeImpressionDetails() {
    // It's better to over allocate the clean-up delay than under allocate it.

    // We need to delay cleaning up all records for any delays in the main join.  We'll add this to
    // impressionCleanupDelay.
    // - inferred ref delays.
    // - combineDeliveryLogWindow
    // - mergeDetailsMissingOutputDelay
    // - mergeDetailsCleanupBuffer
    // - redundant impressions.
    //
    // The maxes are added to the leaf clean-up to support out of order events.
    Duration impressionCleanupDelay =
        add(
            impressionJoinSegment.insertionImpressionJoinMaxOutOfOrder,
            combineDeliveryLogWindow,
            mergeImpressionDetailsMissingOutputDelay,
            mergeDetailsCleanupBuffer);
    Duration deliveryLogCleanupDelay =
        add(impressionCleanupDelay, impressionJoinSegment.insertionImpressionJoinMin);
    LOGGER.info(
        "setting up MergeImpressionDetails.  impressionCleanupDelay={}, deliveryLogCleanupDelay={}",
        impressionCleanupDelay,
        deliveryLogCleanupDelay);
    return new MergeImpressionDetails(
        deliveryLogCleanupDelay,
        impressionCleanupDelay,
        mergeDetailsCleanupPeriod,
        mergeImpressionDetailsMissingOutputDelay,
        batchCleanupAllMergeDetailsTimersBeforeTimestamp,
        getDebugIds());
  }

  // Keeping MergeActionDetails separate from MergeImpressionDetails allows MergeImpressionDetails
  // to have shorter TTLs
  // on it's state.  Since many customers will have a lot more DeliveryLog info than impressions,
  // this allows us to
  // remove a lot of state earlier.
  private MergeActionDetails createMergeActionDetails() {
    // It's better to over allocate the clean-up delay than under allocate it.

    // We need to delay cleaning up all records for any delays in the main join.  We'll add this to
    // actionCleanupDelay.
    // - inferred ref delays.
    // - combineDeliveryLogWindow
    // - mergeDetailsMissingOutputDelay
    // - mergeDetailsCleanupBuffer
    // - redundant impressions.
    //
    // The maxes are added to the leaf clean-up to support out of order events.
    Duration actionCleanupDelay =
        add(
            impressionJoinSegment.insertionImpressionJoinMaxOutOfOrder,
            combineDeliveryLogWindow,
            actionJoinSegment.impressionActionJoinMaxOutOfOrder,
            mergeActionDetailsMissingOutputDelay,
            mergeDetailsCleanupBuffer);
    Duration joinedImpressionCleanupDelay =
        add(actionCleanupDelay, actionJoinSegment.impressionActionJoinMin);
    LOGGER.info(
        "setting up MergeActionDetails.  actionCleanupDelay={}, joinedImpressionCleanupDelay={}",
        actionCleanupDelay,
        joinedImpressionCleanupDelay);
    return new MergeActionDetails(
        joinedImpressionCleanupDelay,
        actionCleanupDelay,
        mergeDetailsCleanupPeriod,
        mergeActionDetailsMissingOutputDelay,
        batchCleanupAllMergeDetailsTimersBeforeTimestamp,
        getDebugIds());
  }

  private DataStream<UnionEvent> toUnionEventForImpressions(
      SingleOutputStreamOperator<CombinedDeliveryLog> deliveryLogs,
      DataStream<Impression> impressions) {

    SingleOutputStreamOperator<UnionEvent> unionDeliveryLogs =
        add(
            deliveryLogs.map(
                deliveryLog ->
                    UnionEvent.newBuilder()
                        // TODO - clean fields.
                        .setCombinedDeliveryLog(deliveryLog)
                        .build()),
            "map-delivery-log-to-union-event");
    SingleOutputStreamOperator<UnionEvent> unionImpressions =
        add(
            impressions.map(
                impression ->
                    UnionEvent.newBuilder()
                        .setImpression(
                            FlatUtil.clearRedundantFlatImpressionFields(impression.toBuilder()))
                        .build()),
            "map-impression-to-union-event");
    return unionDeliveryLogs.union(unionImpressions);
  }

  private DataStream<UnionEvent> toUnionEventForActions(
      SingleOutputStreamOperator<JoinedImpression> joinedImpressions, DataStream<Action> actions) {
    SingleOutputStreamOperator<UnionEvent> unionImpressions =
        add(
            joinedImpressions.map(
                joinedImpression ->
                    UnionEvent.newBuilder().setJoinedImpression(joinedImpression).build()),
            "map-joined-impression-to-union-event");
    SingleOutputStreamOperator<UnionEvent> unionActions =
        add(
            actions.map(
                action ->
                    UnionEvent.newBuilder()
                        .setAction(FlatUtil.clearRedundantFlatActionFields(action.toBuilder()))
                        .build()),
            "map-action-to-union-event");
    return unionImpressions.union(unionActions);
  }

  void joinUserAndOutput(
      SingleOutputStreamOperator<JoinedImpression> joinedImpressions,
      SingleOutputStreamOperator<AttributedAction> attributedActions,
      DataStream<MismatchError> mismatchErrors) {
    DebugIds debugIds = getDebugIds();

    KeyedStream<JoinedImpression, Tuple2<Long, String>> keyedJoinedImpressions =
        joinedImpressions.keyBy(KeyUtil.joinedImpressionAnonUserIdKey);
    KeyedStream<AttributedAction, Tuple2<Long, String>> keyedAttributedActions =
        attributedActions.keyBy(KeyUtil.attributedActionAnonUserIdKey);

    // TODO - group together attribution models.
    SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions =
        cogroupFlatResponseInsertions(keyedJoinedImpressions, keyedAttributedActions);
    // Currently this should only happen during edge cases.
    // This might become more common after we support outer joins with DeliveryLog.
    flatResponseInsertions = filterFlatResponseInsertion(flatResponseInsertions);

    if (writeMismatchError) {
      outputMismatchError(mismatchErrors);
    }

    outputJoinedEvents("etl", joinedImpressions, attributedActions, flatResponseInsertions);
  }

  private SingleOutputStreamOperator<FlatResponseInsertion> cogroupFlatResponseInsertions(
      KeyedStream<JoinedImpression, Tuple2<Long, String>> keyedJoinedImpressions,
      KeyedStream<AttributedAction, Tuple2<Long, String>> keyedAttributedActions) {

    // TODO - evaluate switching to key by our usual Tuple2 and do our own window join.
    // CR: this is going to be very memory intensive.
    // are we sure an offline (non-realtime) enrichment process is not the right way to go
    // instead of using flink streaming?
    SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions =
        add(
            keyedJoinedImpressions
                .coGroup(keyedAttributedActions)
                .where(KeyUtil.joinedImpressionAnonUserInsertionKey)
                .equalTo(KeyUtil.attributedActionAnonUserInsertionKey)
                .window(
                    EventTimeSessionWindows.withGap(toFlinkTime(flatResponseInsertionGapDuration)))
                // TODO: switch to apply with flink 2.0+.
                .with(
                    new CoGroupFunction<
                        JoinedImpression, AttributedAction, FlatResponseInsertion>() {
                      @Override
                      public void coGroup(
                          Iterable<JoinedImpression> impressions,
                          Iterable<AttributedAction> actions,
                          Collector<FlatResponseInsertion> out) {
                        out.collect(
                            FlatUtil.createFlatResponseInsertion(impressions, actions).build());
                      }
                    }),
            "join-flat-response-insertion");

    // Flink core has a bug with cogroups.  It has internal operators and does not support setting
    // direct uids on them.
    // For now, set uids and names using the Transformation interface.
    // https://issues.apache.org/jira/browse/FLINK-25285
    Transformation<?> partition =
        Iterables.getOnlyElement(flatResponseInsertions.getTransformation().getInputs());
    setUidAndName(partition, "partition-join-flat-response-insertion");
    Transformation<?> union = Iterables.getOnlyElement(partition.getInputs());
    setUidAndName(union, "union-join-flat-response-insertion");
    List<Transformation<?>> mapTransformations = union.getInputs();
    Preconditions.checkState(
        mapTransformations.size() == 2, "There should be 2 Cogroup internal Map transformations");
    setUidAndName(mapTransformations.get(0), "map-left-join-flat-response-insertion");
    setUidAndName(mapTransformations.get(1), "map-right-join-flat-response-insertion");

    return flatResponseInsertions;
  }

  // TODO - move these filters to happen before denormalization.  We want these to be filtered
  // out in the main tiny_joined_impression and tiny_attributed_action outputs too.
  private SingleOutputStreamOperator<JoinedImpression> filterJoinedImpression(
      SingleOutputStreamOperator<JoinedImpression> joinedImpressions) {
    SingleOutputStreamOperator<JoinedImpression> filteredJoinedImpression =
        filterOutIgnoreUsage(
            joinedImpressions,
            "joined_impression",
            JoinedImpression.class,
            FlatUtil::hasIgnoreUsage,
            joined -> joined.getTiming().getEventApiTimestamp(),
            "timing.event_api_timestamp");
    return filterBuyer(
        filteredJoinedImpression,
        "joined_impression",
        JoinedImpression.class,
        JoinedImpression::getApiExecutionInsertion,
        joined -> joined.getTiming().getEventApiTimestamp(),
        "timing.event_api_timestamp");
  }

  private SingleOutputStreamOperator<AttributedAction> filterAttributedAction(
      SingleOutputStreamOperator<AttributedAction> attributedAction) {
    SingleOutputStreamOperator<AttributedAction> filteredAttributedAction =
        filterOutIgnoreUsage(
            attributedAction,
            "attributed_action",
            AttributedAction.class,
            FlatUtil::hasIgnoreUsage,
            attributed -> attributed.getAction().getTiming().getEventApiTimestamp(),
            "action.timing.event_api_timestamp");
    return filterBuyer(
        filteredAttributedAction,
        "attributed_action",
        AttributedAction.class,
        attributed -> attributed.getTouchpoint().getJoinedImpression().getApiExecutionInsertion(),
        attributed -> attributed.getAction().getTiming().getEventApiTimestamp(),
        "action.timing.event_api_timestamp");
  }

  private SingleOutputStreamOperator<FlatResponseInsertion> filterFlatResponseInsertion(
      SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions) {
    flatResponseInsertions =
        filterOutIgnoreUsage(
            flatResponseInsertions,
            "flat_response_insertion",
            FlatResponseInsertion.class,
            FlatUtil::hasIgnoreUsage,
            flat -> flat.getTiming().getEventApiTimestamp(),
            "timing.event_api_timestamp");
    return filterBuyer(
        flatResponseInsertions,
        "flat_response_insertion",
        FlatResponseInsertion.class,
        FlatResponseInsertion::getApiExecutionInsertion,
        flat -> flat.getTiming().getEventApiTimestamp(),
        "timing.event_api_timestamp");
  }

  private SingleOutputStreamOperator<DeliveryLog> filterShouldJoin(DataStream<DeliveryLog> events) {
    // Use TinyEvent to keep the records small.
    OutputTag<DeliveryLog> tag =
        new OutputTag("should-not-join", TypeInformation.of(DeliveryLog.class));
    SingleOutputStreamOperator<DeliveryLog> result =
        add(
            events.process(new FilterOperator<>(DeliveryLogUtil::shouldJoin, tag)),
            "filter-delivery-log-should-join");
    SingleOutputStreamOperator<TinyDeliveryLog> tinyDroppedDeliveryLogs =
        toTinyDeliveryLogRequest(result.getSideOutput(tag), "map-should-join-tiny-delivery-log");
    outputDroppedEvents(
        "delivery_log_should_not_join",
        tinyDeliveryLog -> tinyDeliveryLog.getCommon().getEventApiTimestamp(),
        "common.event_api_timestamp",
        tinyDroppedDeliveryLogs);
    return result;
  }

  private SingleOutputStreamOperator<DeliveryLog> filterNonBotDeliveryLogs(
      SingleOutputStreamOperator<DeliveryLog> events) {
    // Use TinyEvent to keep the records small.
    OutputTag<DeliveryLog> tag = new OutputTag("has-bot", TypeInformation.of(DeliveryLog.class));
    SingleOutputStreamOperator<DeliveryLog> result =
        add(
            events.process(new FilterOperator<>(BotUtil::isNotBot, tag)),
            "filter-delivery-log-is-bot");
    SingleOutputStreamOperator<TinyDeliveryLog> tinyDroppedDeliveryLogs =
        toTinyDeliveryLogRequest(result.getSideOutput(tag), "map-is-bot-tiny-delivery-log");
    outputDroppedEvents(
        "delivery_log_is_bot",
        tinyDeliveryLog -> tinyDeliveryLog.getCommon().getEventApiTimestamp(),
        "common.event_api_timestamp",
        tinyDroppedDeliveryLogs);
    return result;
  }

  private <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> filterOutIgnoreUsage(
      SingleOutputStreamOperator<T> events,
      String recordType,
      Class<T> clazz,
      SerializablePredicate<T> isNotIgnoreUsage,
      SerializableToLongFunction<T> getEventApiTimestamp,
      String timestampExp) {
    OutputTag<T> tag = new OutputTag("filtered-out", TypeInformation.of(clazz));
    SingleOutputStreamOperator<T> result =
        add(
            events.process(new FilterOperator<>(SerializablePredicates.not(isNotIgnoreUsage), tag)),
            "filter-out-ignore-usage-" + recordType);
    outputDroppedEvents(
        "ignore_usage_" + recordType,
        getEventApiTimestamp,
        timestampExp,
        result.getSideOutput(tag));
    return result;
  }

  private <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> filterBuyer(
      SingleOutputStreamOperator<T> events,
      String recordType,
      Class<T> clazz,
      SerializableFunction<T, Insertion> getApiExecutionInsertion,
      SerializableToLongFunction<T> getEventApiTimestamp,
      String timestampExp) {
    if (nonBuyerUserSparseHashes.isEmpty()) {
      return events;
    }
    OutputTag<T> tag = new OutputTag("filtered-out", TypeInformation.of(clazz));
    SingleOutputStreamOperator<T> result =
        add(
            events.process(
                new FilterOperator<>(
                    new BuyerPredicate<>(nonBuyerUserSparseHashes, getApiExecutionInsertion), tag)),
            "filter-buyer-" + recordType);
    outputDroppedEvents(
        "non_buyer_" + recordType, getEventApiTimestamp, timestampExp, result.getSideOutput(tag));
    return result;
  }

  void outputJoinedEvents(
      String pathPrefix,
      SingleOutputStreamOperator<JoinedImpression> joinedImpressions,
      SingleOutputStreamOperator<AttributedAction> attributedActions,
      SingleOutputStreamOperator<FlatResponseInsertion> flatResponseInsertions) {
    // TODO: fix latest impressions stuff to only attach to output needed for aws personalize
    // and our own modelling (if needed).  right now all flat action ouputs have them.

    if (writeJoinedEventsToKafka) {
      // Note that we're sending both joined impression and action streams to the same topic.
      // Execution information is cleared to sink less data to Kafka.
      flatOutputKafkaSink.sinkJoinedImpression(
          add(
              joinedImpressions.map(DeliveryExecutionUtil::clearAllDeliveryExecutionDetails),
              "clear-all-delivery-execution-details-joined-impressions"));
      flatOutputKafkaSink.sinkAttributedAction(
          add(
              attributedActions.map(DeliveryExecutionUtil::clearAllDeliveryExecutionDetails),
              "clear-all-delivery-execution-details-attributed-actions"));
    }

    // TBD: we could break out s3 writing to its own job now.
    S3Path outputDir = s3.getOutputDir(pathPrefix).build();

    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          joinedImpressions,
          databaseName,
          "joined_impression",
          // TODO - should this be impression_id?
          genExtractedExtraFields(
              "impression.timing.event_api_timestamp",
              List.of("ids.platform_id", "ids.impression_id")),
          List.of("platform_id", "impression_id"),
          genPartitionFields(
              isPartitionByHour(
                  getLabeledDatabaseName(databaseName, getJobLabel()), "joined_impression")),
          Collections.emptyMap());
      // TODO - does single_cart_content need to be included in the primary key?
      paimonSegment.writeProtoToPaimon(
          attributedActions,
          databaseName,
          "attributed_action",
          genExtractedExtraFields(
              "action.timing.event_api_timestamp",
              List.of(
                  "action.platform_id",
                  "action.action_id",
                  "attribution.model_id",
                  "touchpoint.joined_impression.ids.insertion_id")),
          List.of("platform_id", "action_id", "model_id", "insertion_id"),
          genPartitionFields(
              isPartitionByHour(
                  getLabeledDatabaseName(databaseName, getJobLabel()), "attributed_action")),
          Collections.emptyMap());
      paimonSegment.writeProtoToPaimon(
          flatResponseInsertions,
          databaseName,
          "flat_response_insertion",
          genExtractedExtraFields(
              "request.timing.event_api_timestamp", List.of("ids.platform_id", "ids.insertion_id")),
          List.of("platform_id", "insertion_id"),
          genPartitionFields(
              isPartitionByHour(
                  getLabeledDatabaseName(databaseName, getJobLabel()), "flat_response_insertion")),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              joinedImpressions,
              joinedEvent -> joinedEvent.getImpression().getTiming(),
              outputDir.toBuilder().addSubDir("joined_impression")));
      addSinkTransformations(
          s3FileOutput.sink(
              attributedActions,
              attributedAction -> attributedAction.getAction().getTiming(),
              outputDir.toBuilder().addSubDir("attributed_action")));
      addSinkTransformations(
          s3FileOutput.sink(
              flatResponseInsertions,
              joinedEvent -> joinedEvent.getRequest().getTiming(),
              outputDir.toBuilder().addSubDir("flat_response_insertion")));

      // In Paimon, we'll use the main flat_response_insertion table and remove this side table.
      DataStream<FlatResponseInsertion> flatActionedResponseInsertions =
          add(
              flatResponseInsertions.filter(i -> i.getAttributedActionCount() > 0),
              "filter-flat-actioned-response-insertion");
      addSinkTransformations(
          s3FileOutput.sink(
              flatActionedResponseInsertions,
              joinedEvent -> joinedEvent.getRequest().getTiming(),
              outputDir.toBuilder().addSubDir("flat_actioned_response_insertion")));
    }
  }

  private <T extends GeneratedMessageV3> void outputDroppedMergeDetailsEvents(
      String name,
      SerializableToLongFunction<T> timestampGetter,
      String timestampExp,
      DataStream<T> droppedStream) {
    outputDroppedEvents(name, timestampGetter::applyAsLong, timestampExp, droppedStream);
  }

  private <T extends GeneratedMessageV3> void outputLateEvents(
      String name, SerializableToLongFunction<T> timestampGetter, DataStream<T> lateStream) {
    if (writePaimonTables) {
      outputSideRecordsToPaimon(
          lateStream,
          "late_" + name,
          genExtractedExtraFields("flink_event_timestamp"),
          Collections.emptyList(),
          genPartitionFields(false));
    } else {
      outputSideRecordsToS3(
          ImmutableList.of("late_" + name), timestampGetter::applyAsLong, lateStream);
    }
  }

  private <T extends GeneratedMessageV3> void outputDroppedEvents(
      String name,
      SerializableToLongFunction<T> timestampGetter,
      String timestampExp,
      DataStream<T> droppedStream) {
    if (writePaimonTables) {
      outputSideRecordsToPaimon(
          droppedStream,
          "dropped_" + name,
          genExtractedExtraFields(timestampExp),
          Collections.emptyList(),
          genPartitionFields(false));
    } else {
      outputSideRecordsToS3(ImmutableList.of("dropped_" + name), timestampGetter, droppedStream);
    }
  }

  private <T extends SpecificRecordBase> void outputDroppedEventsParquet(
      String name,
      SerializableToLongFunction<T> timestampGetter,
      DataStream<T> droppedStream,
      Class<T> clazz) {
    if (writePaimonTables) {
      paimonSegment.writeAvroToPaimon(
          droppedStream,
          sideDatabaseName,
          "dropped_" + name,
          genExtractedExtraFields("eventApiTimestamp"),
          Collections.emptyList(),
          genPartitionFields(false),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              droppedStream,
              clazz,
              timestampGetter,
              s3.getOutputDir("etl_side", "dropped_" + name).build()));
    }
  }

  private <T extends GeneratedMessageV3> void outputRecords(
      String name,
      SerializableToLongFunction<T> timestampGetter,
      String timestampExpression,
      DataStream<T> stream) {
    if (writePaimonTables) {
      outputRecordsToPaimon(
          stream,
          name,
          genExtractedExtraFields(timestampExpression),
          Collections.emptyList(),
          genPartitionFields(false));
    } else {
      outputRecordsToS3(ImmutableList.of(name), timestampGetter, stream);
    }
  }

  private <T extends GeneratedMessageV3> void outputDebugRecords(
      String name,
      SerializableToLongFunction<T> timestampGetter,
      String timestampExpression,
      DataStream<T> stream) {
    if (writePaimonTables) {
      outputSideRecordsToPaimon(
          stream,
          "debug_" + name,
          genExtractedExtraFields(timestampExpression),
          Collections.emptyList(),
          genPartitionFields(false));
    } else {
      outputSideRecordsToS3(ImmutableList.of("debug_" + name), timestampGetter, stream);
    }
  }

  private <T extends GeneratedMessageV3> void outputRecordsToS3(
      Iterable<String> subdirs,
      SerializableToLongFunction<T> timestampGetter,
      DataStream<T> stream) {
    addSinkTransformations(
        s3FileOutput.sink(
            timestampGetter,
            stream,
            s3.getOutputDir(ImmutableList.<String>builder().add("etl").addAll(subdirs).build())));
  }

  private <T extends GeneratedMessageV3> void outputSideRecordsToS3(
      Iterable<String> subdirs,
      SerializableToLongFunction<T> timestampGetter,
      DataStream<T> stream) {
    addSinkTransformations(
        s3FileOutput.sink(
            timestampGetter,
            stream,
            s3.getOutputDir(
                ImmutableList.<String>builder().add("etl_side").addAll(subdirs).build())));
  }

  private <T extends GeneratedMessageV3> void outputRecordsToPaimon(
      DataStream<T> stream,
      String tableName,
      List<String> fieldsExtraction,
      List<String> pkExps,
      List<String> partitionExps) {
    paimonSegment.writeProtoToPaimon(
        stream,
        databaseName,
        tableName,
        fieldsExtraction,
        pkExps,
        partitionExps,
        Collections.emptyMap());
  }

  private <T extends GeneratedMessageV3> void outputSideRecordsToPaimon(
      DataStream<T> stream,
      String tableName,
      List<String> fieldsExtraction,
      List<String> pkExps,
      List<String> partitionExps) {
    paimonSegment.writeProtoToPaimon(
        stream,
        sideDatabaseName,
        tableName,
        fieldsExtraction,
        pkExps,
        partitionExps,
        Collections.emptyMap());
  }

  @VisibleForTesting
  void outputAllDeliveryLogVariants(DataStream<DeliveryLog> allDeliveryLogs) {
    DataStream<DeliveryLog> metadataStream =
        add(
            allDeliveryLogs.map(
                deliveryLog -> {
                  DeliveryLog.Builder builder = deliveryLog.toBuilder();
                  builder.getRequestBuilder().clearInsertion();
                  builder.getRequestBuilder().clearInsertionMatrix();
                  builder.getResponseBuilder().clearInsertion();
                  builder.getExecutionBuilder().clearExecutionInsertion();
                  UserInfoUtil.clearUserId(builder);
                  return builder.build();
                }),
            "strip-all-delivery-log");
    outputAllDeliveryLog(metadataStream, "all_delivery_log_metadata");

    DataStream<Request> requestStream =
        add(
            allDeliveryLogs.map(
                (deliveryLog) ->
                    UserInfoUtil.clearUserId(deliveryLog.getRequest().toBuilder()).build()),
            "map-to-request-and-clear-user-id-all-delivery-log-request");
    outputAllDeliveryLogRequest(requestStream, "all_delivery_log_request");
    DataStream<Request> requestMetadataStream =
        add(
            requestStream.map(
                request -> request.toBuilder().clearInsertion().clearInsertionMatrix().build()),
            "clear-insertion-all-delivery-log-request");
    outputAllDeliveryLogRequest(requestMetadataStream, "all_delivery_log_request_metadata");
  }

  // This writes out the combined v1 (synthetic DeliveryLog formed from Request and response
  // Insertion) and v2 DeliveryLog.
  // This function is unit tested.
  @VisibleForTesting
  void outputAllDeliveryLog(DataStream<DeliveryLog> allDeliveryLogs, String name) {
    Preconditions.checkArgument(
        !writePaimonTables,
        "writePaimonTables should not be enabled for the side DelvieryLog output");
    addSinkTransformations(
        s3FileOutput.sink(
            allDeliveryLogs,
            deliveryLog -> deliveryLog.getRequest().getTiming(),
            s3.getOutputDir("etl_side", "debug_" + name)));
  }

  // TODO - refactor these methods after PR/929 is merged.
  // Writes debug Request files out to S3 Avro files.
  @VisibleForTesting
  void outputAllDeliveryLogRequest(DataStream<Request> requests, String name) {
    Preconditions.checkArgument(
        !writePaimonTables,
        "writePaimonTables should not be enabled for the side DelvieryLog output");
    addSinkTransformations(
        s3FileOutput.sink(
            requests,
            request -> request.getTiming(),
            s3.getOutputDir("etl_side", "debug_" + name)));
  }

  @VisibleForTesting
  void outputPartialResponseInsertion(DataStream<TinyInsertion> partialResponseInsertion) {
    if (writePaimonTables) {
      outputSideRecordsToPaimon(
          partialResponseInsertion,
          "debug_partial_response_insertion",
          genExtractedExtraFields("common.event_api_timestamp"),
          Collections.emptyList(),
          genPartitionFields(false));
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              tinyInsertion -> tinyInsertion.getCommon().getEventApiTimestamp(),
              partialResponseInsertion,
              s3.getOutputDir("etl_side", "debug_partial_response_insertion")));
    }
  }

  private void outputMismatchError(DataStream<MismatchError> errors) {
    if (writePaimonTables) {
      paimonSegment.writeAvroToPaimon(
          errors,
          sideDatabaseName,
          "mismatch_error",
          genExtractedExtraFields("eventApiTimestamp"),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              errors,
              MismatchError.class,
              MismatchError::getEventApiTimestamp,
              s3.getOutputDir("etl_side", "mismatch_error").build()));
    }
  }

  @Override
  public void setTableEnv(StreamTableEnvironment tEnv) {
    super.setTableEnv(tEnv);
    paimonSegment.setStreamTableEnvironment(tEnv);
  }
}
