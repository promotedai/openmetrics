package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.DeliveryLogIds;
import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.common.RequestInsertionIds;
import ai.promoted.metrics.common.ResponseInsertionIds;
import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.RestructureDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.UserInfoUtil;
import ai.promoted.metrics.logprocessor.common.job.BaseFlinkJob;
import ai.promoted.metrics.logprocessor.common.job.FeatureFlag;
import ai.promoted.metrics.logprocessor.common.job.KafkaSegment;
import ai.promoted.metrics.logprocessor.common.job.MetricsApiKafkaSource;
import ai.promoted.metrics.logprocessor.common.job.RawActionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawImpressionSegment;
import ai.promoted.metrics.logprocessor.common.job.RawOutputKafka;
import ai.promoted.metrics.logprocessor.common.job.RawViewSegment;
import ai.promoted.metrics.logprocessor.common.job.S3FileOutput;
import ai.promoted.metrics.logprocessor.common.job.S3Segment;
import ai.promoted.metrics.logprocessor.common.job.hudi.HudiOutput;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

@CommandLine.Command(
    name = "rawoutput",
    mixinStandardHelpOptions = true,
    version = "rawoutput 1.0.0",
    description =
        "Creates a Flink job that reads LogRequests from Kafka, fills in defaults, logs the "
            + "records to disk.")
public class RawOutputJob extends BaseFlinkJob {
  public static final Map<String, String> HUDI_EXTRA_OPTIONS =
      Map.of("hadoop.fs.s3a.connection.maximum", "500", "hadoop.fs.s3a.threads.max", "300");
  private static final Logger LOGGER = LogManager.getLogger(RawOutputJob.class);
  @CommandLine.Mixin public final KafkaSegment kafkaSegment = new KafkaSegment(this);

  @CommandLine.Mixin
  public final MetricsApiKafkaSource metricsApiKafkaSource =
      new MetricsApiKafkaSource(this, kafkaSegment);

  @CommandLine.Mixin public final RawOutputKafka rawOutputKafka = new RawOutputKafka(kafkaSegment);
  @CommandLine.Mixin public final S3Segment s3 = new S3Segment(this);
  @CommandLine.Mixin public final S3FileOutput s3FileOutput = new S3FileOutput(this, s3);
  @CommandLine.Mixin public final HudiOutput hudiOutput = new HudiOutput(this);
  @CommandLine.Mixin public final RawViewSegment rawViewSegment = new RawViewSegment(this);

  @CommandLine.Mixin
  public final RawImpressionSegment rawImpressionSegment = new RawImpressionSegment(this);

  @CommandLine.Mixin public final RawActionSegment rawActionSegment = new RawActionSegment(this);

  @CommandLine.Option(
      names = {"--keepFirstLogUserUserDuration"},
      defaultValue = "PT2H",
      description =
          "The "
              + "duration to keep track of [dt, hour, LogUserUser] outputs.  This can be used to further reduce outputs "
              + "in our "
              + "Parquet tables.  Default=PT2H.  Java8 Duration parse format.")
  public Duration keepFirstLogUserUserDuration = Duration.parse("PT2H");

  @CommandLine.Option(
      names = {"--keepFirstCohortMembershipDuration"},
      defaultValue = "P1D",
      description =
          "The "
              + "duration to keep track of recent CohortMemberships.  This is used to de-duplicate raw inputs.  "
              + "Default=P1D to match flat_response_insertion join window.  Java8 Duration parse format.")
  public Duration keepFirstCohortMembershipDuration = Duration.parse("P1D");

  @CommandLine.Option(
      names = {"--keepFirstDeliveryLogDuration"},
      defaultValue = "P1D",
      description =
          "The "
              + "duration to keep track of recent DeliveryLogs.  This is used to de-duplicate raw inputs.  "
              + "Default=P1D to match flat_response_insertion join window.  Java8 Duration parse format.")
  public Duration keepFirstDeliveryLogDuration = Duration.parse("P1D");

  @FeatureFlag
  @CommandLine.Option(
      names = {"--writeLogUserUserEventsToKafka"},
      negatable = true,
      description = "Whether to write LogUserUser events to Kafka.  Default=false")
  public boolean writeLogUserUserEventsToKafka = false;

  @FeatureFlag
  @CommandLine.Option(
      names = {"--writeHudiTables"},
      negatable = true,
      description = "Whether to write data to Hudi tables.  Default=false")
  public boolean writeHudiTables = false;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new RawOutputJob()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    validateArgs();
    startRawLogJob();
    return 0;
  }

  @Override
  public void validateArgs() {
    kafkaSegment.validateArgs();
    metricsApiKafkaSource.validateArgs();
    s3.validateArgs();
    s3FileOutput.validateArgs();
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.<Class<? extends GeneratedMessageV3>>builder()
        .addAll(kafkaSegment.getProtoClasses())
        .addAll(metricsApiKafkaSource.getProtoClasses())
        .addAll(s3.getProtoClasses())
        .addAll(s3FileOutput.getProtoClasses())
        .build();
  }

  @Override
  public String getJobName() {
    return prefixJobLabel("log-log-request");
  }

  private void startRawLogJob() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureExecutionEnvironment(env, parallelism, maxParallelism);

    String kafkaConsumerGroupId = toKafkaConsumerGroupId("loglogrequest");
    MetricsApiKafkaSource.SplitSources splitLogRequest =
        metricsApiKafkaSource.splitSources(env, kafkaConsumerGroupId, null);
    outputLogRequest(splitLogRequest);
    LOGGER.info("RawLogLogRequest.executionPlan\n{}", env.getExecutionPlan());
    env.execute(getJobName());
  }

  @VisibleForTesting
  void outputLogRequest(MetricsApiKafkaSource.SplitSources splitLogRequest) {
    if (writeHudiTables) {
      try {
        hudiOutput.open();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (writeHudiTables) {
      LOGGER.warn("Writing log-request table to Hudi is not supported. Fall back to S3.");
      //    hudiOutput.outputProtoToHudi(
      //        splitLogRequest.getLogRequestStream(),
      //        "log_request",
      //        null,
      //        List.of(
      //            "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
      //            "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
      //        null);
      // TODO Fix the code-gen issue PRO-4395
      addSinkTransformations(
          s3FileOutput.sink(
              splitLogRequest.getLogRequestStream(),
              LogRequest::getTiming,
              s3.getDir("raw", "log-request")));
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              splitLogRequest.getLogRequestStream(),
              LogRequest::getTiming,
              s3.getDir("raw", "log-request")));
    }

    outputUser(splitLogRequest.getRawUserSource());
    // Output User-LogUser join table.
    outputLogUserUser(splitLogRequest.getLogRequestStream());

    outputCohortMembership(
        keepFirstCohortMembership(splitLogRequest.getRawCohortMembershipSource()));
    outputView(rawViewSegment.getDeduplicatedView(splitLogRequest.getRawViewSource()));
    outputAutoView(rawViewSegment.getDeduplicatedAutoView(splitLogRequest.getRawAutoViewSource()));

    DataStream<DeliveryLog> deliveryLog =
        restructureDeliveryLog(keepFirstDeliveryLog(splitLogRequest.getRawDeliveryLogSource()));
    outputDeliveryLog(deliveryLog);
    // Output DeliveryLog side tables.
    outputDeliveryLogIds(deliveryLog);
    outputRequestInsertionIds(deliveryLog);
    outputResponseInsertionIds(deliveryLog);

    outputImpression(
        rawImpressionSegment.getDeduplicatedImpression(splitLogRequest.getRawImpressionSource()));
    outputAction(rawActionSegment.getDeduplicatedAction(splitLogRequest.getRawActionSource()));
    outputDiagnostics(splitLogRequest.getRawDiagnosticsSource());
    if (writeHudiTables) {
      hudiOutput.close();
    }
  }

  private void outputUser(DataStream<User> users) {
    // Do not strip userId for User streams.
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          users,
          "users",
          List.of("platform_id", "user_info.log_user_id"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(s3FileOutput.sink(users, User::getTiming, s3.getDir("raw", "user")));
    }
  }

  private DataStream<CohortMembership> keepFirstCohortMembership(
      DataStream<CohortMembership> cohortMemberships) {
    return add(
        cohortMemberships
            .keyBy(RawKeys.cohortMembershipKeySelector)
            .process(
                new KeepFirstRow<>("cohort-membership", keepFirstCohortMembershipDuration),
                TypeInformation.of(CohortMembership.class)),
        "keep-first-cohort-membership");
  }

  private void outputCohortMembership(DataStream<CohortMembership> cohortMemberships) {
    cohortMemberships =
        add(cohortMemberships.map(UserInfoUtil::clearUserId), "clear-cohort-membership-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          cohortMemberships,
          "cohort_membership",
          // TODO membership_id could be an empty string which is not allowed in Hudi
          // Collections.singletonList("membership_id"),
          null,
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              cohortMemberships,
              CohortMembership::getTiming,
              s3.getDir("raw", "cohort-membership")));
    }
  }

  private void outputView(DataStream<View> views) {
    views = add(views.map(UserInfoUtil::clearUserId), "clear-view-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          views,
          "view",
          List.of("platform_id", "view_id"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(s3FileOutput.sink(views, View::getTiming, s3.getDir("raw", "view")));
    }
  }

  private void outputAutoView(DataStream<AutoView> autoViews) {
    autoViews = add(autoViews.map(UserInfoUtil::clearUserId), "clear-auto-view-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          autoViews,
          "auto_view",
          List.of("platform_id", "auto_view_id"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(
          s3FileOutput.sink(autoViews, AutoView::getTiming, s3.getDir("raw", "auto-view")));
    }
  }

  private DataStream<DeliveryLog> keepFirstDeliveryLog(DataStream<DeliveryLog> deliveryLogs) {
    return add(
        deliveryLogs
            .keyBy(RawKeys.deliveryLogKeySelector)
            .process(
                new KeepFirstRow<>("delivery-log", keepFirstDeliveryLogDuration),
                TypeInformation.of(DeliveryLog.class)),
        "keep-first-delivery-log");
  }

  private DataStream<DeliveryLog> restructureDeliveryLog(DataStream<DeliveryLog> deliveryLogs) {
    return add(deliveryLogs.map(new RestructureDeliveryLog()), "restructure-delivery-log");
  }

  private void outputDeliveryLog(DataStream<DeliveryLog> deliveryLogs) {
    deliveryLogs = add(deliveryLogs.map(UserInfoUtil::clearUserId), "clear-delivery-log-user-id");
    if (writeHudiTables) {
      addSinkTransformation(
          hudiOutput
              .outputProtoToHudi(
                  deliveryLogs,
                  "delivery_log",
                  List.of("platform_id", "request.request_id"), // PK fields
                  List.of(
                      "DATE_FORMAT(TO_TIMESTAMP_LTZ(request.timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
                      "DATE_FORMAT(TO_TIMESTAMP_LTZ(request.timing.event_api_timestamp, 3), 'HH') hr"),
                  HUDI_EXTRA_OPTIONS)
              .getTransformation());
    } else {
      addSinkTransformations(
          s3FileOutput.sink(
              deliveryLogs,
              deliveryLog -> deliveryLog.getRequest().getTiming(),
              s3.getDir("raw", "delivery-log")));
    }
  }

  private void outputLogUserUser(DataStream<LogRequest> logRequests) {
    SingleOutputStreamOperator<LogUserUser> logUserUser =
        add(logRequests.flatMap(new ToLogUserUser()), "to-log-user-user");

    // Output LogUserUser to kafka before the deduplication to keep the stream continuous.
    if (writeLogUserUserEventsToKafka) {
      rawOutputKafka.addLogUserUserSink(
          logUserUser, rawOutputKafka.getLogUserUserTopic(getJobLabel()));
    }

    // Try to de-duplicate UserLogUsers inside each [dt, hour].
    // The dedupe window is 2h by default (--keepFirstLogUserUserDuration).
    // A user that expands across hours will have multiple rows.  That's fine.  Consumers need to
    // de-duplicate.
    logUserUser =
        add(
            logUserUser
                .keyBy(RawKeys.logUserUserKeySelector)
                .process(
                    new KeepFirstRow<>("log-user-user", keepFirstLogUserUserDuration),
                    TypeInformation.of(LogUserUser.class)),
            "keep-first-log-user-user");

    if (writeHudiTables) {
      hudiOutput.outputAvroToHudi(
          logUserUser,
          "log_user_user",
          List.of("platformId", "logUserId"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'HH') hr"),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              logUserUser,
              LogUserUser.class,
              LogUserUser::getEventApiTimestamp,
              s3.getDir("raw", "log-user-user").build()));
    }
  }

  private void outputDeliveryLogIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<DeliveryLogIds> deliveryLogIds =
        add(deliveryLog.map(new ToDeliveryLogIds()), "to-delivery-log-ids");
    if (writeHudiTables) {
      hudiOutput.outputAvroToHudi(
          deliveryLogIds,
          "delivery_log_ids",
          null,
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'HH') hr"),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              deliveryLogIds,
              DeliveryLogIds.class,
              DeliveryLogIds::getEventApiTimestamp,
              s3.getDir("raw-side", "delivery-log-ids").build()));
    }
  }

  private void outputRequestInsertionIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<RequestInsertionIds> requestInsertionIds =
        add(deliveryLog.flatMap(new ToRequestInsertionIds()), "to-request-insertion-ids");
    if (writeHudiTables) {
      hudiOutput.outputAvroToHudi(
          requestInsertionIds,
          "request_insertion_ids",
          List.of("platformId", "requestId"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'HH') hr"),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              requestInsertionIds,
              RequestInsertionIds.class,
              RequestInsertionIds::getEventApiTimestamp,
              s3.getDir("raw-side", "request-insertion-ids").build()));
    }
  }

  private void outputResponseInsertionIds(DataStream<DeliveryLog> deliveryLog) {
    SingleOutputStreamOperator<ResponseInsertionIds> responseInsertionIds =
        add(deliveryLog.flatMap(new ToResponseInsertionIds()), "to-response-insertion-ids");
    if (writeHudiTables) {
      hudiOutput.outputAvroToHudi(
          responseInsertionIds,
          "response_insertion_ids",
          List.of("platformId", "requestId"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(eventApiTimestamp, 3), 'HH') hr"),
          Collections.emptyMap());
    } else {
      addSinkTransformation(
          s3FileOutput.outputSpecificAvroRecordParquet(
              responseInsertionIds,
              ResponseInsertionIds.class,
              ResponseInsertionIds::getEventApiTimestamp,
              s3.getDir("raw-side", "response-insertion-ids").build()));
    }
  }

  private void outputImpression(DataStream<Impression> impressions) {
    impressions = add(impressions.map(UserInfoUtil::clearUserId), "clear-impression-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          impressions,
          "impression",
          List.of("platform_id", "impression_id"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(
          s3FileOutput.sink(impressions, Impression::getTiming, s3.getDir("raw", "impression")));
    }
  }

  private void outputAction(DataStream<Action> actions) {
    actions = add(actions.map(UserInfoUtil::clearUserId), "clear-action-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          actions,
          "action",
          List.of("platform_id", "action_id"),
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(
          s3FileOutput.sink(actions, Action::getTiming, s3.getDir("raw", "action")));
    }
  }

  private void outputDiagnostics(DataStream<Diagnostics> diagnostics) {
    diagnostics = add(diagnostics.map(UserInfoUtil::clearUserId), "clear-diagnostics-user-id");
    if (writeHudiTables) {
      hudiOutput.outputProtoToHudi(
          diagnostics,
          "diagnostics",
          null,
          List.of(
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'yyyy-MM-dd') dt",
              "DATE_FORMAT(TO_TIMESTAMP_LTZ(timing.event_api_timestamp, 3), 'HH') hr"),
          HUDI_EXTRA_OPTIONS);
    } else {
      addSinkTransformations(
          s3FileOutput.sink(diagnostics, Diagnostics::getTiming, s3.getDir("raw", "diagnostics")));
    }
  }
}
