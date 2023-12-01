package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.functions.filter.LogRequestFilter;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.InsertionBundle;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Option;

// If we need to make this class serializable, we need to figure out how to handle
// filteredLogRequestStream.
public class MetricsApiKafkaSource implements FlinkSegment {
  private static final Logger LOGGER = LogManager.getLogger(MetricsApiKafkaSource.class);

  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;
  private final KafkaSourceSegment kafkaSourceSegment;

  @Option(
      names = {"--logRequestTopicOverride"},
      defaultValue = "",
      description = "Overrides the input LogRequest " + "Kafka topic.")
  public String logRequestTopicOverride = "";

  @Option(
      names = {"--maxLogRequestOutOfOrderness"},
      defaultValue = "PT1S",
      description = "The maxOutOfOrderness " + "for the LogRequest kafka. Default=PT1S")
  public Duration maxLogRequestOutOfOrderness = Duration.parse("PT1S");

  // Is a separate flag from `--kafkaDataset` because the input kafka topic uses a deprecated
  // dataset.
  @Option(
      names = {"--metricsApiKafkaDataset"},
      defaultValue = Constants.DEPRECATED_DEFAULT_KAFKA_DATASET,
      description = "The middle part of the Kafka topic name. Default=event")
  public String metricsApiKafkaDataset = Constants.DEPRECATED_DEFAULT_KAFKA_DATASET;

  public MetricsApiKafkaSource(
      BaseFlinkJob job, KafkaSegment kafkaSegment, KafkaSourceSegment kafkaSourceSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
    this.kafkaSourceSegment = kafkaSourceSegment;
  }

  /**
   * A utility to flexibility get a valid Timing from a LogRequest. Falls back in case there's a bug
   * and LogRequest's top-level Timing field is not set.
   */
  private static Timing getTiming(LogRequest logRequest) {
    if (logRequest.hasTiming()) {
      return logRequest.getTiming();
    } else if (logRequest.getDeliveryLogCount() > 0) {
      return logRequest.getDeliveryLog(0).getRequest().getTiming();
    } else if (logRequest.getImpressionCount() > 0) {
      return logRequest.getImpression(0).getTiming();
    } else if (logRequest.getActionCount() > 0) {
      return logRequest.getAction(0).getTiming();
    } else if (logRequest.getCohortMembershipCount() > 0) {
      return logRequest.getCohortMembership(0).getTiming();
    } else if (logRequest.getViewCount() > 0) {
      return logRequest.getView(0).getTiming();
    } else if (logRequest.getAutoViewCount() > 0) {
      return logRequest.getAutoView(0).getTiming();
    } else if (logRequest.getDiagnosticsCount() > 0) {
      return logRequest.getDiagnostics(0).getTiming();
    } else {
      LOGGER.warn("Encountered missing Timing on LogRequest");
      return Timing.getDefaultInstance();
    }
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  // This function is not unit tested.
  public SingleOutputStreamOperator<LogRequest> getLogRequestSource(
      StreamExecutionEnvironment env, String kafkaGroupId) {
    return kafkaSourceSegment.addProtobufSource(
        env,
        getInputLogRequestTopic(),
        kafkaGroupId,
        LogRequest.class,
        LogRequest::parseFrom,
        maxLogRequestOutOfOrderness,
        logRequest -> getTiming(logRequest).getEventApiTimestamp());
  }

  public SplitSources splitSources(StreamExecutionEnvironment env, String kafkaGroupId) {
    return splitSources(getLogRequestSource(env, kafkaGroupId));
  }

  public SplitSources splitSources(SingleOutputStreamOperator<LogRequest> input) {
    return new SplitSources(input);
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of(
        LogRequest.class,
        User.class,
        Session.class,
        SessionProfile.class,
        CohortMembership.class,
        AutoView.class,
        View.class,
        DeliveryLog.class,
        CombinedDeliveryLog.class,
        Request.class,
        Insertion.class,
        InsertionBundle.class,
        Impression.class,
        Action.class,
        Diagnostics.class);
  }

  @VisibleForTesting
  String getInputLogRequestTopic() {
    return firstNotEmpty(
        logRequestTopicOverride,
        kafkaSegment.getKafkaTopic(
            Constants.LOG_REQUEST_TOPIC_MESSAGE_GROUP,
            // No label because the input LogRequest topic does not have one.
            metricsApiKafkaDataset,
            Constants.LOG_REQUEST_TOPIC_DATA_NAME));
  }

  /**
   * Streams is a class for accessing substreams / split streams of Metrics API LogRequest stream.
   * MetricsApiKafkaSource is scoped to a job and control flags. This class is scoped to a stream of
   * LogRequests.
   */
  public final class SplitSources {
    private final SingleOutputStreamOperator<LogRequest> logRequestStream;

    private SplitSources(SingleOutputStreamOperator<LogRequest> logRequestStream) {
      this.logRequestStream =
          job.add(logRequestStream.process(new LogRequestFilter()), "split-log-request");
    }

    public SingleOutputStreamOperator<LogRequest> getLogRequestStream() {
      return logRequestStream;
    }

    public DataStream<User> getRawUserSource() {
      return LogRequestFilter.getUserStream(logRequestStream);
    }

    public DataStream<CohortMembership> getRawCohortMembershipSource() {
      return LogRequestFilter.getCohortMembershipStream(logRequestStream);
    }

    public DataStream<AutoView> getRawAutoViewSource() {
      return LogRequestFilter.getAutoViewStream(logRequestStream);
    }

    public DataStream<View> getRawViewSource() {
      return LogRequestFilter.getViewStream(logRequestStream);
    }

    public DataStream<DeliveryLog> getRawDeliveryLogSource() {
      return LogRequestFilter.getDeliveryLogStream(logRequestStream);
    }

    public DataStream<Impression> getRawImpressionSource() {
      return LogRequestFilter.getImpressionStream(logRequestStream);
    }

    public DataStream<Action> getRawActionSource() {
      return LogRequestFilter.getActionStream(logRequestStream);
    }

    public DataStream<Diagnostics> getRawDiagnosticsSource() {
      return LogRequestFilter.getDiagnosticsStream(logRequestStream);
    }
  }
}
