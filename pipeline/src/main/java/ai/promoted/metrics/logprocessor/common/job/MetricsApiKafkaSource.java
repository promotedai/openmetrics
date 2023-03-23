package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.functions.filter.LogRequestFilter;
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
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.List;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine.Option;

// If we need to make this class serializable, we need to figure out how to handle
// filteredLogRequestStream.
public class MetricsApiKafkaSource implements FlinkSegment {

  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;

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

  public MetricsApiKafkaSource(BaseFlinkJob job, KafkaSegment kafkaSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  // This function is not unit tested.
  public SingleOutputStreamOperator<LogRequest> getLogRequestSource(
      StreamExecutionEnvironment env, String kafkaGroupId, OffsetsInitializer boundedOffsets) {
    return kafkaSegment.addProtobufSource(
        env,
        getInputLogRequestTopic(),
        kafkaGroupId,
        LogRequest.class,
        LogRequest::parseFrom,
        maxLogRequestOutOfOrderness,
        null,
        boundedOffsets);
  }

  public SplitSources splitSources(
      StreamExecutionEnvironment env, String kafkaGroupId, OffsetsInitializer boundedOffsets) {
    return splitSources(getLogRequestSource(env, kafkaGroupId, boundedOffsets));
  }

  public SplitSources splitSources(SingleOutputStreamOperator<LogRequest> input) {
    return new SplitSources(input);
  }

  @Override
  public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableList.of(
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
