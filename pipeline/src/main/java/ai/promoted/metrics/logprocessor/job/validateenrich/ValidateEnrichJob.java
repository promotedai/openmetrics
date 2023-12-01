package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.functions.FixDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.RestructureDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.SetLogTimestamp;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableBiFunction;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableConsumer;
import ai.promoted.metrics.logprocessor.common.functions.base.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.validate.BaseValidate;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateAction;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateCohortMembership;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateDiagnostics;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateImpression;
import ai.promoted.metrics.logprocessor.common.functions.validate.ValidateView;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkTableJob;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.RegionSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSink;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventSink;
import ai.promoted.metrics.logprocessor.common.job.paimon.PaimonSegment;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.TableUtil;
import ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer.AnonymizerSegment;
import ai.promoted.metrics.logprocessor.job.validateenrich.anonymizer.TimedStringAnonymizer;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.EnrichmentUnion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "validateenrich",
    mixinStandardHelpOptions = true,
    version = "validateenrich 1.0.0",
    description =
        "Creates a Flink job that validates our LogRequests, enriches them and writes them out to Kafka.")
public class ValidateEnrichJob extends BaseFlinkTableJob {
  private static final Logger LOGGER = LogManager.getLogger(ValidateEnrichJob.class);
  private static final String RETAINED_USER_TABLE_NAME = "retained_user";
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final MetricsApiKafkaSource metricsApiKafkaSource =
      new MetricsApiKafkaSource(this, kafkaSegment, kafkaSourceSegment);

  @CommandLine.Mixin public final PaimonSegment paimonSegment = new PaimonSegment(this);

  @CommandLine.Mixin
  public final KafkaSinkSegment kafkaSinkSegment = new KafkaSinkSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final ValidatedEventKafkaSegment validatedEventKafkaSegment =
      new ValidatedEventKafkaSegment(this, kafkaSegment);

  public ValidatedEventSink validatedEventSink =
      new ValidatedEventKafkaSink(kafkaSinkSegment, validatedEventKafkaSegment);

  @CommandLine.Mixin public final RegionSegment regionSegment = new RegionSegment();

  @CommandLine.Mixin
  public final AnonymizerSegment anonymizerSegment = new AnonymizerSegment(regionSegment);

  @Option(
      names = {"--newRetainedUserIdFlinkStateTtl"},
      defaultValue = "PT4H",
      description =
          "The TTL for keeping newly created retainedUserIds in Flink state.  The job keeps newly "
              + "created retainedUserIds in Flink state because the lookup state is not updated "
              + "immediately.  The retained_user_id Paimon tables are written out on checkpoint. "
              + "The Paimon lookups are cached.  The Paimon lookups use processing time."
              + "Default=PT4H. Java8 Duration parse format.  The default is a little arbitrary. "
              + "Large enough to handle backfill checkpoint durations.")
  public Duration newRetainedUserIdFlinkStateTtl = Duration.parse("PT4H");

  @Option(
      names = {"--requireAnonUserId"},
      defaultValue = "true",
      description = "Whether to require anonUserId.  Default=true")
  public boolean requireAnonUserId = true;

  @Option(
      names = {"--customActionTypeMap"},
      description =
          "Supports mapping customActionTypes to ActionTypes.  This can be used to map flexible inputs to build-in ActionTypes.")
  public Map<String, ActionType> customActionTypeMap = new HashMap<>();

  @CommandLine.Mixin
  public final RemoveEventsSegment removeEventsSegment = new RemoveEventsSegment();

  private transient DataStream<ValidationError> validationErrorUnion = null;

  private transient DataStream<EnrichmentUnion> authUnion = null;
  private transient DataStream<EnrichmentUnion> unauthUnion = null;

  public static void main(String[] args) {
    executeMain(new ValidateEnrichJob(), args);
  }

  private static UserInfo migrateV1UserInfo(UserInfo userInfo) {
    // BIG HACK for the release.  For now, just make the IDs the same and lowercase them.
    //
    // The next release step should be:
    // 1. Change the join job and counter job to use logUserId.
    // 2. Allow anonUserId to have mixed casing.
    // 3. Change client code to set anonUserId and not lowercase it.
    //
    // We're not fixing in this release because it'd require a full backfill of some of our
    // blue environments.

    UserInfo.Builder builder = userInfo.toBuilder();
    String logUserId = userInfo.getLogUserId();
    if (logUserId.isEmpty()) {
      logUserId = userInfo.getAnonUserId();
    }
    logUserId = LogUserIdUtil.toLogUserId(logUserId);
    return builder.setLogUserId(logUserId).setAnonUserId(logUserId).build();
  }

  private static final UserInfo scrubUserId(UserInfo userInfo) {
    return userInfo.toBuilder().clearUserId().build();
  }

  private static CohortMembership modifyCohortMembershipUserInfo(
      CohortMembership record, UserInfo userInfo) {
    return record.toBuilder().setUserInfo(userInfo).build();
  }

  private static View modifyViewUserInfo(View record, UserInfo userInfo) {
    return record.toBuilder().setUserInfo(userInfo).build();
  }

  private static UserInfo getDeliveryLogUserInfo(DeliveryLog record) {
    return record.getRequest().getUserInfo();
  }

  private static DeliveryLog modifyDeliveryLogUserInfo(DeliveryLog record, UserInfo userInfo) {
    DeliveryLog.Builder builder = record.toBuilder();
    builder.getRequestBuilder().setUserInfo(userInfo);
    return builder.build();
  }

  private static Impression modifyImpressionUserInfo(Impression record, UserInfo userInfo) {
    return record.toBuilder().setUserInfo(userInfo).build();
  }

  private static Action modifyActionUserInfo(Action record, UserInfo userInfo) {
    return record.toBuilder().setUserInfo(userInfo).build();
  }

  private static Diagnostics modifyDiagnosticsUserInfo(Diagnostics record, UserInfo userInfo) {
    return record.toBuilder().setUserInfo(userInfo).build();
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        kafkaSinkSegment,
        metricsApiKafkaSource,
        validatedEventKafkaSegment,
        validatedEventSink);
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.<Class<? extends GeneratedMessageV3>>builder()
        .addAll(super.getProtoClasses())
        .add(Diagnostics.class)
        .add(UnionEvent.class)
        .build();
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    setTableEnv(tEnv);

    MetricsApiKafkaSource.SplitSources splitLogRequest =
        metricsApiKafkaSource.splitSources(env, toKafkaConsumerGroupId("validateenrich"));

    startValidateAndEnrichJob(splitLogRequest);

    LOGGER.info("{}.executionPlan\n{}", getJobName(), env.getExecutionPlan());
    env.execute(getJobName());
  }

  @Override
  protected String getDefaultBaseJobName() {
    return "validate-enrich";
  }

  private DataStream<RetainedUserIdInput> toRetainedUserInput(
      DataStream<EnrichmentUnion> authUnions) {
    return add(
        authUnions.map(
            union -> {
              UserInfo userInfo = EnrichmentUnionUtil.getUserInfo(union);
              return new RetainedUserIdInput(
                  EnrichmentUnionUtil.getPlatformId(union),
                  userInfo.getUserId(),
                  userInfo.getAnonUserId());
            }),
        "to-retained-user-input");
  }

  /**
   * Validates and enriches input stream.
   *
   * @param splitLogRequest
   */
  @VisibleForTesting
  synchronized void startValidateAndEnrichJob(MetricsApiKafkaSource.SplitSources splitLogRequest)
      throws Exception {
    validationErrorUnion = null;
    authUnion = null;
    unauthUnion = null;

    // TODO - restructure so we can set retainedUserId on invalid records.
    // TODO - don't lowercase anonUserId.

    // For CohortMembership
    processAndGenUnionStream(
        splitLogRequest.getRawCohortMembershipSource(),
        CohortMembership.class,
        new ValidateCohortMembership(requireAnonUserId),
        Constants.COHORT_MEMBERSHIP_TOPIC_DATA_NAME,
        Constants.INVALID_COHORT_MEMBERSHIP_TOPIC_DATA_NAME,
        SetLogTimestamp.forCohortMembership,
        CohortMembership::getUserInfo,
        ValidateEnrichJob::modifyCohortMembershipUserInfo,
        validatedEventSink::sinkInvalidCohortMembership,
        EnrichmentUnionUtil::toCohortMembershipUnion);

    // TODO - implement AutoView.

    // For View
    processAndGenUnionStream(
        splitLogRequest.getRawViewSource(),
        View.class,
        new ValidateView(requireAnonUserId),
        Constants.VIEW_TOPIC_DATA_NAME,
        Constants.INVALID_VIEW_TOPIC_DATA_NAME,
        SetLogTimestamp.forView,
        View::getUserInfo,
        ValidateEnrichJob::modifyViewUserInfo,
        validatedEventSink::sinkInvalidView,
        EnrichmentUnionUtil::toViewUnion);

    // TODO - scrub all extra UserInfo fields (since we have some embedded).
    // Process delivery-log
    processAndGenUnionStream(
        splitLogRequest.getRawDeliveryLogSource(),
        DeliveryLog.class,
        new ValidateDeliveryLog(requireAnonUserId),
        Constants.DELIVERY_LOG_TOPIC_DATA_NAME,
        Constants.INVALID_DELIVERY_LOG_TOPIC_DATA_NAME,
        SetLogTimestamp.forDeliveryLog,
        ValidateEnrichJob::getDeliveryLogUserInfo,
        ValidateEnrichJob::modifyDeliveryLogUserInfo,
        validatedEventSink::sinkInvalidDeliveryLog,
        EnrichmentUnionUtil::toDeliveryLogUnion,
        // DeliveryLog has extra transformations.
        // We'll keep invalid DeliveryLogs unmodified for now.
        validatedDeliveryLog -> {
          SingleOutputStreamOperator<DeliveryLog> restructuredDeliveryLog =
              add(
                  validatedDeliveryLog.map(new RestructureDeliveryLog()),
                  "restructure-delivery-log");
          return fixDeliveryLogStream(restructuredDeliveryLog);
        });

    // Process impression
    processAndGenUnionStream(
        splitLogRequest.getRawImpressionSource(),
        Impression.class,
        new ValidateImpression(requireAnonUserId),
        Constants.IMPRESSION_TOPIC_DATA_NAME,
        Constants.INVALID_IMPRESSION_TOPIC_DATA_NAME,
        SetLogTimestamp.forImpression,
        Impression::getUserInfo,
        ValidateEnrichJob::modifyImpressionUserInfo,
        validatedEventSink::sinkInvalidImpression,
        EnrichmentUnionUtil::toImpressionUnion);

    // Process action
    processAndGenUnionStream(
        splitLogRequest.getRawActionSource(),
        Action.class,
        new ValidateAction(requireAnonUserId),
        Constants.ACTION_TOPIC_DATA_NAME,
        Constants.INVALID_ACTION_TOPIC_DATA_NAME,
        SetLogTimestamp.forAction,
        Action::getUserInfo,
        ValidateEnrichJob::modifyActionUserInfo,
        validatedEventSink::sinkInvalidAction,
        EnrichmentUnionUtil::toActionUnion,
        validatedAction -> {
          if (removeEventsSegment.canRemoveActions()) {
            validatedAction =
                add(
                    validatedAction.filter(removeEventsSegment.getKeepActionsPredicate()),
                    "filter-custom-action-types-by-remove-event-api-timestamp-range");
          }

          if (!customActionTypeMap.isEmpty()) {
            validatedAction =
                add(
                    validatedAction.map(new MapCustomActionType(customActionTypeMap)),
                    "map-custom-action-types");
          }
          return validatedAction;
        });

    // Process diagnostics
    processAndGenUnionStream(
        splitLogRequest.getRawDiagnosticsSource(),
        Diagnostics.class,
        new ValidateDiagnostics(requireAnonUserId),
        Constants.DIAGNOSTICS_TOPIC_DATA_NAME,
        Constants.INVALID_DIAGNOSTICS_TOPIC_DATA_NAME,
        SetLogTimestamp.forDiagnostics,
        Diagnostics::getUserInfo,
        ValidateEnrichJob::modifyDiagnosticsUserInfo,
        validatedEventSink::sinkInvalidDiagnostics,
        EnrichmentUnionUtil::toDiagnosticsUnion);

    // TODO - deduplicate ValidationError.

    // Write out the union validation error stream.
    validatedEventSink.sinkValidationError(validationErrorUnion);

    DataStream<EnrichmentUnion> preScrubAuthUnion = authUnion;
    // During initial rollout, keep it optional.
    if (anonymizerSegment.hasAnonymizer("retained_user_id")) {
      DataStream<RetainedUserIdInput> retainedUserInput = toRetainedUserInput(authUnion);
      DataStream<RetainedUserLookupRow> lookupResult = lookupRetainedUser(retainedUserInput);
      SingleOutputStreamOperator<SelectRetainedUserChange> retainedUserChange =
          loadOrCreateRetainedUserId(lookupResult);
      KeyedStream<SelectRetainedUserChange, AuthUserIdKey> userIdToRetainedUserChange =
          retainedUserChange.keyBy(AuthUserIdKey::toKey);
      authUnion =
          add(
              authUnion
                  .keyBy(EnrichmentUnionUtil::toAuthUserIdKey)
                  .connect(userIdToRetainedUserChange)
                  .process(new JoinRetainedUserId()),
              "join-retained-user-id");
    } else {
      authUnion =
          add(
              authUnion.map(
                  union ->
                      EnrichmentUnionUtil.setUserInfo(
                          union,
                          EnrichmentUnionUtil.getUserInfo(union).toBuilder()
                              .clearUserId()
                              .build())),
              "scrub-user-id");
    }

    // 6. Output.
    SingleOutputStreamOperator<DeliveryLog> splitAuthStream =
        add(authUnion.process(new SplitUnion()), "split-auth-union");
    SingleOutputStreamOperator<DeliveryLog> splitUnauthStream =
        add(unauthUnion.process(new SplitUnion()), "split-unauth-union");

    // Output delivery-log
    validatedEventSink.sinkDeliveryLog(splitAuthStream.union(splitUnauthStream));
    validatedEventSink.sinkCohortMembership(
        splitAuthStream
            .getSideOutput(SplitUnion.COHORT_MEMBERSHIP_TAG)
            .union(splitUnauthStream.getSideOutput(SplitUnion.COHORT_MEMBERSHIP_TAG)));
    validatedEventSink.sinkView(
        splitAuthStream
            .getSideOutput(SplitUnion.VIEW_TAG)
            .union(splitUnauthStream.getSideOutput(SplitUnion.VIEW_TAG)));
    validatedEventSink.sinkImpression(
        splitAuthStream
            .getSideOutput(SplitUnion.IMPRESSION_TAG)
            .union(splitUnauthStream.getSideOutput(SplitUnion.IMPRESSION_TAG)));
    validatedEventSink.sinkAction(
        splitAuthStream
            .getSideOutput(SplitUnion.ACTION_TAG)
            .union(splitUnauthStream.getSideOutput(SplitUnion.ACTION_TAG)));
    validatedEventSink.sinkDiagnostics(
        splitAuthStream
            .getSideOutput(SplitUnion.DIAGNOSTICS_TAG)
            .union(splitUnauthStream.getSideOutput(SplitUnion.DIAGNOSTICS_TAG)));

    // `preScrubAuthUnion` still has `userId` populated.
    SingleOutputStreamOperator<LogUserUser> logUserUser =
        add(preScrubAuthUnion.process(new ToLogUserUser()), "to-log-user-user");
    validatedEventSink.sinkLogUserUser(logUserUser);

    // `authUser` has already has `retainedUserId` populated.
    SingleOutputStreamOperator<AnonUserRetainedUser> anonUserRetainedUser =
        add(authUnion.process(new ToAnonUserRetainedUser()), "to-anon-user-retained-user");
    validatedEventSink.sinkAnonUserRetainedUser(anonUserRetainedUser);

    prepareToExecute();
  }

  @VisibleForTesting
  static String getCreateRetainedUserPaimonTableSql(String fullTableName) {
    // This statement was copied from the text logs from the writeProtoToPaimon call.
    // TODO - restructure the PaimonSegment so we can create tables based on lookups.
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s (\n"
            + "    platform_id BIGINT NOT NULL,\n"
            + "    user_id STRING NOT NULL,\n"
            + "    retained_user_id STRING NOT NULL,\n"
            + "    create_event_api_time_millis BIGINT,\n"
            + "    process_time_millis BIGINT,\n"
            + "    last_forgotten_time_millis BIGINT\n"
            + ") WITH ('primary-key' = 'platform_id,user_id', "
            + "'partition' = 'platform_id', 'file.format'='orc')",
        fullTableName);
  }

  private DataStream<RetainedUserLookupRow> lookupRetainedUser(
      DataStream<RetainedUserIdInput> eventUserIds) throws Exception {
    StreamTableEnvironment tEnv = getStreamTableEnvironment();

    Table eventUserIdTable =
        tEnv.fromDataStream(
            eventUserIds,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("f0", "BIGINT NOT NULL")
                .column("f1", "STRING NOT NULL")
                .column("f2", "STRING")
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3) NOT NULL")
                // This is used in the Paimon lookup.
                .columnByExpression("proc_time", "PROCTIME()")
                .primaryKey("f0", "f1")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());

    LOGGER.info("event_user_id schema={}", eventUserIdTable.getResolvedSchema());

    String eventUserIdFullViewName =
        TableUtil.fullTableName(paimonSegment.paimonCatalogName, databaseName, "event_user_id");

    tEnv.createTemporaryView(eventUserIdFullViewName, eventUserIdTable);
    paimonSegment.createPaimonCatalog();
    // An attempt to fix the Paimon table writing.
    tEnv.executeSql(String.format("USE CATALOG `%s`", paimonSegment.paimonCatalogName)).await();

    try (CloseableIterator<Row> iterator = paimonSegment.createPaimonDatabase(databaseName)) {
      while (iterator.hasNext()) {
        LOGGER.info("createPaimonDatabase row = {}", iterator.next());
      }
    }

    // PR - should we use labeled DBs or non-labeled?
    String fullTableName =
        TableUtil.fullTableName(
            paimonSegment.paimonCatalogName, databaseName, RETAINED_USER_TABLE_NAME);
    String createPaimonTableSql = getCreateRetainedUserPaimonTableSql(fullTableName);
    tEnv.executeSql(createPaimonTableSql).await();

    String lookupPaimonSql =
        String.format(
            "SELECT /*+ LOOKUP('table'='u', 'output-mode'='allow_unordered') */\n"
                // Keep rowtime at the end since we ignore it outside of this function.
                + "e.f0 AS platform_id, e.f1 AS user_id, e.f2 AS anon_user_id, u.retained_user_id, u.last_forgotten_time_millis, rowtime\n"
                + "FROM %s AS e\n"
                + "LEFT JOIN %s /*+ OPTIONS('lookup.async'='true', 'lookup.async-thread-number'='16') */\n"
                + "FOR SYSTEM_TIME AS OF e.proc_time\n"
                + "AS u\n"
                + "ON e.f0 = u.platform_id AND e.f1 = u.user_id;",
            eventUserIdFullViewName, fullTableName);

    return tEnv.toDataStream(tEnv.sqlQuery(lookupPaimonSql), RetainedUserLookupRow.class);
  }

  private void writeRetainedUserPaimonTable(DataStream<RetainedUser> retainedUser)
      throws Exception {
    // PR - should we use labeled DBs or non-labeled?
    String fullTableName =
        TableUtil.fullTableName(
            paimonSegment.paimonCatalogName, databaseName, RETAINED_USER_TABLE_NAME);
    String createPaimonTableSql = getCreateRetainedUserPaimonTableSql(fullTableName);
    tEnv.executeSql(createPaimonTableSql).await();

    paimonSegment.writeProtoToPaimon(
        retainedUser,
        databaseName,
        "retained_user",
        null,
        ImmutableList.of("platform_id", "user_id"),
        null,
        ImmutableMap.of());
  }

  private SingleOutputStreamOperator<SelectRetainedUserChange> loadOrCreateRetainedUserId(
      DataStream<RetainedUserLookupRow> retainedUserLookupResult) throws Exception {
    KeyedStream<RetainedUserLookupRow, AuthUserIdKey> keyedUserIds =
        retainedUserLookupResult.keyBy(RetainedUserLookupRow::toKey);

    // TODO - put in asserts to verify that this min session time is longer than the retained_user
    // Paimon cache time.
    SingleOutputStreamOperator<SelectRetainedUserChange> changes =
        add(
            keyedUserIds.process(
                new CreateRetainedUserId(
                    new RetainedUserIdAnonymizer(
                        anonymizerSegment.getAnonymizer("retained_user_id")),
                    newRetainedUserIdFlinkStateTtl.toMillis())),
            "create-retained-user-id");

    SideOutputDataStream<RetainedUser> upsertRetainedUserTags =
        changes.getSideOutput(CreateRetainedUserId.NEW_RETAINED_USERS_TAG);
    validatedEventSink.sinkRetainedUser(upsertRetainedUserTags);
    writeRetainedUserPaimonTable(upsertRetainedUserTags);
    return changes;
  }

  /**
   * Enrich, validate and convert the input events into a union format and generate a global union
   * stream. General steps:
   *
   * <ol>
   *   <li>Push down fields from LogRequest to each Record (This happens before invoking this method
   *       in MetricsApiKafkaSource.SplitSources).
   *   <li>Fill in Kafka log timestamp on the records.
   *   <li>Temporarily fix v1 UserInfo input where anonUserId is passed in on logUserId.
   *   <li>Validate records.
   *   <li>Enrich with a log user id.
   *   <li>More steps are continued further in the outer method...
   * </ol>
   */
  private <T extends GeneratedMessageV3> void processAndGenUnionStream(
      DataStream<T> inputStream,
      Class<T> clazz,
      BaseValidate<T> validator,
      String streamName,
      String invalidStreamName,
      ProcessFunction<T, T> setLogTimestampFunc,
      SerializableFunction<T, UserInfo> getUserInfoFunc,
      SerializableBiFunction<T, UserInfo, T> setUserInfoFunc,
      SerializableConsumer<DataStream<T>> sinkInvalidFunc,
      MapFunction<T, EnrichmentUnion> toUnionFun,
      @Nullable
          SerializableFunction<SingleOutputStreamOperator<T>, SingleOutputStreamOperator<T>>
              extraTransform) {
    // Step 1. Set log timestamp.
    SingleOutputStreamOperator<T> stream =
        add(inputStream.process(setLogTimestampFunc), "set-" + streamName + "-log-timestamp");

    // Step 2. For certain environments, allow anonymizing the userId into an anonUserId to keep
    // the down stream jobs running.
    if (anonymizerSegment.hasAnonymizer("anon_user_id")) {
      TimedStringAnonymizer anonymizer = anonymizerSegment.getAnonymizer("anon_user_id");
      stream =
          add(
              stream.process(
                  new ProcessFunction<>() {
                    @Override
                    public void processElement(
                        T record, ProcessFunction<T, T>.Context context, Collector<T> collector)
                        throws Exception {
                      UserInfo userInfo = getUserInfoFunc.apply(record);
                      // HACK - just fully override anonUserId for now.  We'll eventually undo this
                      // when we change the join job to join based on retainedUserId.
                      // TODO - remove.
                      if (!userInfo.getUserId().isEmpty()) {
                        String anonUserId =
                            anonymizer.apply(userInfo.getUserId(), context.timestamp());
                        userInfo =
                            userInfo.toBuilder()
                                .setAnonUserId(anonUserId)
                                .setLogUserId(anonUserId)
                                .build();
                        record = setUserInfoFunc.apply(record, userInfo);
                      }
                      collector.collect(record);
                    }
                  }),
              "anonymize-" + streamName + "-user-id");
    }

    // Step 3. Update the v1 UserInfo that has anon_user_id in the log_user_id field.
    // TODO - remove the migrateV1AnonUserId migration code after clients migrate to anonUserId.
    stream =
        add(
            stream.map(
                record ->
                    setUserInfoFunc.apply(
                        record, migrateV1UserInfo(getUserInfoFunc.apply(record)))),
            "migrate-v1-" + streamName + "-log-user-id");

    // Step 4. Runs the validator.
    SingleOutputStreamOperator<T> validatedStream =
        add(stream.process(validator, TypeInformation.of(clazz)), "validate-" + streamName);
    if (null == validationErrorUnion) {
      validationErrorUnion = validatedStream.getSideOutput(BaseValidate.VALIDATION_ERROR_TAG);
    } else {
      validationErrorUnion =
          validationErrorUnion.union(
              validatedStream.getSideOutput(BaseValidate.VALIDATION_ERROR_TAG));
    }

    sinkInvalidFunc.accept(
        scrubUserId(
            validatedStream.getSideOutput(validator.getInvalidRecordTag()),
            invalidStreamName,
            getUserInfoFunc,
            setUserInfoFunc));

    // Apply extra transformations if provided.
    if (null != extraTransform) {
      validatedStream = extraTransform.apply(validatedStream);
    }

    // Step 4b Union the auth and unauth streams
    SplitAuth<T> splitAuth = new SplitAuth<>(getUserInfoFunc, clazz);
    SingleOutputStreamOperator<T> authStream =
        add(validatedStream.process(splitAuth), "split-auth-" + streamName);
    SideOutputDataStream<T> unauthStream = authStream.getSideOutput(splitAuth.getFilteredOutTag());

    // Union so we can have all events to use the same keyby state.
    SingleOutputStreamOperator<EnrichmentUnion> singleAuthUnion =
        add(authStream.map(toUnionFun), "to-auth-" + streamName + "-union");
    if (null == authUnion) {
      authUnion = singleAuthUnion;
    } else {
      authUnion = authUnion.union(singleAuthUnion);
    }
    SingleOutputStreamOperator<EnrichmentUnion> singleUnauthUnion =
        add(unauthStream.map(toUnionFun), "to-unauth-" + streamName + "-union");
    if (null == unauthUnion) {
      unauthUnion = singleUnauthUnion;
    } else {
      unauthUnion = unauthUnion.union(singleUnauthUnion);
    }
  }

  private <T extends GeneratedMessageV3> void processAndGenUnionStream(
      DataStream<T> inputStream,
      Class<T> clazz,
      BaseValidate<T> validator,
      String streamName,
      String invalidStreamName,
      ProcessFunction<T, T> setLogTimestampFunc,
      SerializableFunction<T, UserInfo> getUserInfoFunc,
      SerializableBiFunction<T, UserInfo, T> setUserInfoFunc,
      SerializableConsumer<DataStream<T>> sinkInvalidFunc,
      MapFunction<T, EnrichmentUnion> toUnionFun) {
    processAndGenUnionStream(
        inputStream,
        clazz,
        validator,
        streamName,
        invalidStreamName,
        setLogTimestampFunc,
        getUserInfoFunc,
        setUserInfoFunc,
        sinkInvalidFunc,
        toUnionFun,
        null);
  }

  private <T extends GeneratedMessageV3> SingleOutputStreamOperator<T> scrubUserId(
      DataStream<T> stream,
      String topicDataName,
      SerializableFunction<T, UserInfo> getUserInfo,
      SerializableBiFunction<T, UserInfo, T> setUserInfo) {
    return add(
        stream.map(record -> setUserInfo.apply(record, scrubUserId(getUserInfo.apply(record)))),
        "scrub-" + topicDataName + "-user-id");
  }

  // This code is temporary.  We can remove it later.
  @VisibleForTesting
  SingleOutputStreamOperator<DeliveryLog> fixDeliveryLogStream(
      SingleOutputStreamOperator<DeliveryLog> deliveryLogs) {
    return withDebugIdLogger(
        add(deliveryLogs.map(new FixDeliveryLog()), "prepare-delivery-log"), "rawDeliveryLogInput");
  }

  private SingleOutputStreamOperator<DeliveryLog> withDebugIdLogger(
      SingleOutputStreamOperator<DeliveryLog> deliveryLogs, String label) {
    DebugIds debugIds = getDebugIds();
    if (!debugIds.hasAnyIds()) {
      return deliveryLogs;
    } else {
      return add(
          deliveryLogs.map(
              deliveryLog -> {
                if (debugIds.matches(deliveryLog)) {
                  LOGGER.info("Found debugId in {}, deliveryLog={}", label, deliveryLog);
                }
                return deliveryLog;
              }),
          "match-debug-id-" + label + "-log");
    }
  }

  @Override
  public void setTableEnv(StreamTableEnvironment tEnv) {
    super.setTableEnv(tEnv);
    paimonSegment.setStreamTableEnvironment(tEnv);
  }

  @Override
  public void tableOperationsToDataStream() {
    paimonSegment.getStatementSet().attachAsDataStream();
  }
}
