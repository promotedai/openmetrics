package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.DeliveryLogIds;
import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.common.RequestInsertionIds;
import ai.promoted.metrics.common.ResponseInsertionIds;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkTableJob;
import ai.promoted.metrics.logprocessor.common.job.DirectValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.FlinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSinkSegment;
import ai.promoted.metrics.logprocessor.common.job.KafkaSourceSegment;
import ai.promoted.metrics.logprocessor.common.job.KeepFirstSegment;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.ValidatedEventKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.paimon.PaimonSegment;
import ai.promoted.metrics.logprocessor.common.util.TrackingUtil;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "rawoutput",
    mixinStandardHelpOptions = true,
    version = "rawoutput 1.0.0",
    description =
        "Creates a Flink job that reads LogRequests from Kafka, fills in defaults, logs the "
            + "records to disk.")
public class RawOutputJob extends BaseFlinkTableJob {

  private static final Logger LOGGER = LogManager.getLogger(RawOutputJob.class);
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment();

  @CommandLine.Mixin
  public final KafkaSourceSegment kafkaSourceSegment = new KafkaSourceSegment(this, kafkaSegment);

  @CommandLine.Mixin
  public final MetricsApiKafkaSource metricsApiKafkaSource =
      new MetricsApiKafkaSource(this, kafkaSegment, kafkaSourceSegment);

  @CommandLine.Mixin
  public final KafkaSinkSegment kafkaSinkSegment = new KafkaSinkSegment(this, kafkaSegment);

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
  @CommandLine.Mixin public final PaimonSegment paimonSegment = new PaimonSegment(this);

  @FeatureFlag
  @CommandLine.Option(
      names = {"--writePaimonTables"},
      negatable = true,
      description = "Whether to write data to Paimon tables.  Default=false")
  public boolean writePaimonTables = false;

  @Option(
      names = {"--excludeTables"},
      description = "List of table names to exclude.  Defaults to empty")
  public Set<String> excludeTables = new HashSet<>();

  public static void main(String[] args) {
    executeMain(new RawOutputJob(), args);
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    return ImmutableSet.of(
        kafkaSegment,
        kafkaSourceSegment,
        metricsApiKafkaSource,
        kafkaSinkSegment,
        s3,
        s3FileOutput,
        validatedEventKafkaSegment,
        directValidatedEventKafkaSource,
        validatedEventKafkaSource);
  }

  @Override
  protected void startJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    setTableEnv(tEnv);

    String consumerGroupId = toKafkaConsumerGroupId("log");
    LOGGER.info("Exclude tables {}", excludeTables);

    if (!excludeTables.contains("log_user_user")) {
      SingleOutputStreamOperator<LogUserUser> logUserUser;
      // For the older format, we want to deduplicate per hour.
      if (writePaimonTables) {
        logUserUser = validatedEventKafkaSource.getLogUserUserSource(env, consumerGroupId);
      } else {
        logUserUser =
            validatedEventKafkaSource.getLogUserUserSource(
                env, consumerGroupId, KeyUtil.logUserUserHourKeySelector);
      }
      outputLogUserUser(getDatabaseName(false), "log_user_user", logUserUser);
    }

    if (!excludeTables.contains("anon_user_retained_user")) {
      SingleOutputStreamOperator<AnonUserRetainedUser> anonUserRetainedUser;
      // For the older format, we want to deduplicate per hour.
      if (writePaimonTables) {
        anonUserRetainedUser =
            validatedEventKafkaSource.getAnonUserRetainedUserSource(env, consumerGroupId);
      } else {
        anonUserRetainedUser =
            validatedEventKafkaSource.getAnonUserRetainedUserSource(
                env, consumerGroupId, KeyUtil.anonUserRetainedUserHourKeySelector);
      }
      outputAnonUserRetainedUser(
          getDatabaseName(false), "anon_user_retained_user", anonUserRetainedUser);
    }

    if (!excludeTables.contains("validation_error")) {
      outputValidationError(
          getDatabaseName(false),
          "validation_error",
          directValidatedEventKafkaSource.getValidationErrorSource(env, consumerGroupId));
    }

    if (!excludeTables.contains("cohort_membership")) {
      outputCohortMembership(
          getDatabaseName(false),
          "cohort_membership",
          validatedEventKafkaSource.getCohortMembershipSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("invalid_cohort_membership")) {
      outputCohortMembership(
          getDatabaseName(true),
          "invalid_cohort_membership",
          validatedEventKafkaSource.getInvalidCohortMembershipSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("view")) {
      outputView(
          getDatabaseName(false),
          "view",
          validatedEventKafkaSource.getViewSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("invalid_view")) {
      outputView(
          getDatabaseName(true),
          "invalid_view",
          validatedEventKafkaSource.getInvalidViewSource(env, consumerGroupId));
    }
    // outputAutoView(rawViewSegment.getDeduplicatedAutoView(
    // splitLogRequest.getRawAutoViewSource()));

    if (!excludeTables.contains("delivery_log")) {
      SingleOutputStreamOperator<DeliveryLog> deliveryLog =
          validatedEventKafkaSource.getDeliveryLogSource(env, consumerGroupId);
      outputDeliveryLogTables(deliveryLog);
    }
    if (!excludeTables.contains("invalid_delivery_log")) {
      outputDeliveryLog(
          getDatabaseName(true),
          "invalid_delivery_log",
          validatedEventKafkaSource.getInvalidDeliveryLogSource(env, consumerGroupId));
    }

    if (!excludeTables.contains("impression")) {
      outputImpression(
          getDatabaseName(false),
          "impression",
          validatedEventKafkaSource.getImpressionSource(env, consumerGroupId));
    }

    if (!excludeTables.contains("invalid_impression")) {
      outputImpression(
          getDatabaseName(true),
          "invalid_impression",
          validatedEventKafkaSource.getInvalidImpressionSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("action")) {
      outputAction(
          getDatabaseName(false),
          "action",
          validatedEventKafkaSource.getActionSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("invalid_action")) {
      outputAction(
          getDatabaseName(true),
          "invalid_action",
          validatedEventKafkaSource.getInvalidActionSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("diagnostics")) {
      outputDiagnostics(
          getDatabaseName(false),
          "diagnostics",
          directValidatedEventKafkaSource.getDiagnosticsSource(env, consumerGroupId));
    }
    if (!excludeTables.contains("invalid_diagnostics")) {
      outputDiagnostics(
          getDatabaseName(true),
          "invalid_diagnostics",
          directValidatedEventKafkaSource.getInvalidDiagnosticsSource(env, consumerGroupId));
    }

    prepareToExecute();
    LOGGER.info("RawLogLogRequest.executionPlan\n{}", env.getExecutionPlan());
    env.execute(getJobName());
  }

  @Override
  public void validateArgs() {
    super.validateArgs();
    paimonSegment.validateArgs();
  }

  // TODO - rename the job.
  @Override
  protected String getDefaultBaseJobName() {
    return "log-log-request";
  }

  @VisibleForTesting
  void outputDeliveryLogTables(SingleOutputStreamOperator<DeliveryLog> deliveryLog) {
    outputDeliveryLog(getDatabaseName(false), "delivery_log", deliveryLog);
    // Output DeliveryLog side tables when Paimon is disabled
    if (!writePaimonTables) {
      outputDeliveryLogIds(deliveryLog);
      outputRequestInsertionIds(deliveryLog);
      outputResponseInsertionIds(deliveryLog);
    }
  }

  @Override
  public void setTableEnv(StreamTableEnvironment tEnv) {
    super.setTableEnv(tEnv);
    paimonSegment.setStreamTableEnvironment(tEnv);
  }

  private String getDatabaseName(boolean isInvalidTable) {
    if (isInvalidTable) {
      return sideDatabaseName;
    } else {
      return databaseName;
    }
  }

  @VisibleForTesting
  void outputCohortMembership(
      String database,
      String tableName,
      SingleOutputStreamOperator<CohortMembership> cohortMemberships) {
    SingleOutputStreamOperator<CohortMembership> trackedCohortMemberships =
        cohortMemberships.map(
            new MapFunction<CohortMembership, CohortMembership>() {
              @Override
              public CohortMembership map(CohortMembership cohortMembership) throws Exception {
                return TrackingUtil.COHORT_MEMBERSHIP_PROCESSING_TIME_SETTER.apply(
                    cohortMembership);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedCohortMemberships,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          // TODO membership_id could be an empty string which is not allowed
          // Collections.singletonList("membership_id"),
          null,
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              trackedCohortMemberships,
              CohortMembership::getTiming,
              s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputView(String database, String tableName, SingleOutputStreamOperator<View> views) {
    SingleOutputStreamOperator<View> trackedViews =
        views
            .map(
                new MapFunction<View, View>() {
                  @Override
                  public View map(View view) throws Exception {
                    return TrackingUtil.VIEW_PROCESSING_TIME_SETTER.apply(view);
                  }
                })
            .returns(View.class);
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedViews,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          List.of("platform_id", "view_id"),
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(trackedViews, View::getTiming, s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputAutoView(String database, String tableName, DataStream<AutoView> autoViews) {
    autoViews = add(autoViews.map(UserInfoUtil::clearUserId), "clear-auto-view-user-id");
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          autoViews,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          List.of("platform_id", "auto_view_id"),
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(autoViews, AutoView::getTiming, s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputDeliveryLog(
      String database, String tableName, SingleOutputStreamOperator<DeliveryLog> deliveryLogs) {
    SingleOutputStreamOperator<DeliveryLog> trackedDeliveryLogs =
        deliveryLogs.map(
            new MapFunction<DeliveryLog, DeliveryLog>() {
              @Override
              public DeliveryLog map(DeliveryLog deliveryLog) throws Exception {
                return TrackingUtil.DELIVERY_LOG_PROCESSING_TIME_SETTER.apply(deliveryLog);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedDeliveryLogs,
          database,
          tableName,
          genExtractedExtraFields(
              "request.timing.event_api_timestamp", List.of("request.request_id")),
          List.of("platform_id", "request_id"),
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              trackedDeliveryLogs,
              deliveryLog -> deliveryLog.getRequest().getTiming(),
              s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputLogUserUser(String database, String tableName, DataStream<LogUserUser> logUserUser) {
    if (writePaimonTables) {
      paimonSegment.writeAvroToPaimon(
          logUserUser,
          database,
          tableName,
          null,
          List.of("platformId", "logUserId"),
          null,
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              logUserUser,
              LogUserUser.class,
              LogUserUser::getEventTimeMillis,
              s3.getOutputDir("raw", tableName).build()));
    }
  }

  @VisibleForTesting
  void outputAnonUserRetainedUser(
      String database,
      String tableName,
      SingleOutputStreamOperator<AnonUserRetainedUser> anonUserRetainedUsers) {
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          anonUserRetainedUsers,
          database,
          tableName,
          null,
          List.of("platform_id", "anon_user_id", "retained_user_id"),
          null,
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              AnonUserRetainedUser::getEventTimeMillis,
              anonUserRetainedUsers,
              AnonUserRetainedUser.class,
              s3.getOutputDir("raw", tableName)));
    }
  }

  private void outputValidationError(
      String database, String tableName, SingleOutputStreamOperator<ValidationError> errorStream) {
    SingleOutputStreamOperator<ValidationError> trackedErrorStream =
        errorStream.map(
            new MapFunction<ValidationError, ValidationError>() {
              @Override
              public ValidationError map(ValidationError validationError) throws Exception {
                return TrackingUtil.VALIDATION_ERROR_PROCESSING_TIME_SETTER.apply(validationError);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeAvroToPaimon(
          trackedErrorStream,
          database,
          tableName,
          genExtractedExtraFields("timing.eventApiTimestamp"),
          null,
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              trackedErrorStream,
              ValidationError.class,
              error -> error.getTiming().getEventApiTimestamp(),
              s3.getOutputDir("raw", tableName).build()));
    }
  }

  private void outputDeliveryLogIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<DeliveryLogIds> deliveryLogIds =
        add(deliveryLog.map(new ToDeliveryLogIds()), "to-delivery-log-ids");
    assert (!writePaimonTables);
    addSinkTransformation(
        s3FileOutput.outputSpecificAvroRecordParquet(
            deliveryLogIds,
            DeliveryLogIds.class,
            DeliveryLogIds::getEventApiTimestamp,
            s3.getOutputDir("raw-side", "delivery-log-ids").build()));
  }

  private void outputRequestInsertionIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<RequestInsertionIds> requestInsertionIds =
        add(deliveryLog.flatMap(new ToRequestInsertionIds()), "to-request-insertion-ids");
    assert (!writePaimonTables);
    addSinkTransformation(
        s3FileOutput.outputSpecificAvroRecordParquet(
            requestInsertionIds,
            RequestInsertionIds.class,
            RequestInsertionIds::getEventApiTimestamp,
            s3.getOutputDir("raw-side", "request-insertion-ids").build()));
  }

  private void outputResponseInsertionIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<ResponseInsertionIds> responseInsertionIds =
        add(deliveryLog.flatMap(new ToResponseInsertionIds()), "to-response-insertion-ids");
    assert (!writePaimonTables);
    addSinkTransformation(
        s3FileOutput.outputSpecificAvroRecordParquet(
            responseInsertionIds,
            ResponseInsertionIds.class,
            ResponseInsertionIds::getEventApiTimestamp,
            s3.getOutputDir("raw-side", "response-insertion-ids").build()));
  }

  @VisibleForTesting
  void outputImpression(
      String database, String tableName, SingleOutputStreamOperator<Impression> impressions) {
    SingleOutputStreamOperator<Impression> trackedImpressions =
        impressions.map(
            new MapFunction<Impression, Impression>() {
              @Override
              public Impression map(Impression impression) throws Exception {
                return TrackingUtil.IMPRESSION_PROCESSING_TIME_SETTER.apply(impression);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedImpressions,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          List.of("platform_id", "impression_id"),
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              trackedImpressions, Impression::getTiming, s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputAction(String database, String tableName, SingleOutputStreamOperator<Action> actions) {
    SingleOutputStreamOperator<Action> trackedAction =
        actions.map(
            new MapFunction<Action, Action>() {
              @Override
              public Action map(Action action) throws Exception {
                return TrackingUtil.ACTION_PROCESSING_TIME_SETTER.apply(action);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedAction,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          List.of("platform_id", "action_id"),
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(trackedAction, Action::getTiming, s3.getOutputDir("raw", tableName)));
    }
  }

  @VisibleForTesting
  void outputDiagnostics(
      String database, String tableName, SingleOutputStreamOperator<Diagnostics> diagnostics) {
    SingleOutputStreamOperator<Diagnostics> trackedDiagnostics =
        diagnostics.map(
            new MapFunction<Diagnostics, Diagnostics>() {
              @Override
              public Diagnostics map(Diagnostics diagnostics) throws Exception {
                return TrackingUtil.DIAGNOSTICS_PROCESSING_TIME_SETTER.apply(diagnostics);
              }
            });
    if (writePaimonTables) {
      paimonSegment.writeProtoToPaimon(
          trackedDiagnostics,
          database,
          tableName,
          genExtractedExtraFields("timing.event_api_timestamp"),
          null,
          genPartitionFields(isPartitionByHour(database, tableName)),
          Collections.emptyMap());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              trackedDiagnostics, Diagnostics::getTiming, s3.getOutputDir("raw", tableName)));
    }
  }

  @Override
  public void tableOperationsToDataStream() {
    paimonSegment.getStatementSet().attachAsDataStream();
  }
}
