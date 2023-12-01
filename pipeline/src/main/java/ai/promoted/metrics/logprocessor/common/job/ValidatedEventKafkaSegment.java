package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import picocli.CommandLine.Option;

public class ValidatedEventKafkaSegment implements FlinkSegment {
  public static final Set<Class<? extends GeneratedMessageV3>> PROTO_CLASSES =
      ImmutableSet.of(
          CohortMembership.class,
          View.class,
          DeliveryLog.class,
          Impression.class,
          Action.class,
          Diagnostics.class);
  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;

  @Option(
      names = {"--validatedTopicOverride"},
      description = "Overrides the Validated Kafka topics")
  public Map<String, String> validatedTopicOverride = new HashMap<>();

  public ValidatedEventKafkaSegment(BaseFlinkJob job, KafkaSegment kafkaSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return PROTO_CLASSES;
  }

  public SourceSinkTopics getRetainedUserTopics() {
    return getSourceSinkTopics(Constants.RETAINED_USER_EVENT_TOPIC_DATA_NAME);
  }

  // TODO - delete.
  public SourceSinkTopics getLogUserUserTopics() {
    return getSourceSinkTopics(Constants.LOG_USER_USER_EVENT_TOPIC_DATA_NAME);
  }

  public SourceSinkTopics getAnonUserRetainedUserTopics() {
    return getSourceSinkTopics(Constants.ANON_USER_RETAINED_USER_EVENT_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getCohortMembershipTopics() {
    return getValidatedSourceSinkTopics(Constants.COHORT_MEMBERSHIP_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getViewTopics() {
    return getValidatedSourceSinkTopics(Constants.VIEW_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getDeliveryLogTopics() {
    return getValidatedSourceSinkTopics(Constants.DELIVERY_LOG_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getImpressionTopics() {
    return getValidatedSourceSinkTopics(Constants.IMPRESSION_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getActionTopics() {
    return getValidatedSourceSinkTopics(Constants.ACTION_TOPIC_DATA_NAME);
  }

  public ValidatedSourceSinkTopics getDiagnosticsTopics() {
    return getValidatedSourceSinkTopics(Constants.DIAGNOSTICS_TOPIC_DATA_NAME);
  }

  public SourceSinkTopics getValidationErrorTopics() {
    return getSourceSinkTopics(Constants.VALIDATION_ERROR_TOPIC_DATA_NAME);
  }

  private ValidatedSourceSinkTopics getValidatedSourceSinkTopics(String topicDataName) {
    return new ValidatedSourceSinkTopics() {
      @Override
      public SourceSinkTopics getValid() {
        return getSourceSinkTopics(topicDataName);
      }

      @Override
      public SourceSinkTopics getInvalid() {
        return getSourceSinkTopics(Constants.INVALID_PREFIX + topicDataName);
      }
    };
  }

  private SourceSinkTopics getSourceSinkTopics(String topicDataName) {
    return new SourceSinkTopics() {
      @Override
      public String getSourceTopic() {
        return getValidatedTopic(job.getInputLabel(topicDataName), topicDataName);
      }

      @Override
      public String getSinkTopic() {
        return getValidatedTopic(job.getJobLabel(), topicDataName);
      }
    };
  }

  private String getValidatedTopic(String label, String topicDataName) {
    return firstNotEmpty(
        validatedTopicOverride.getOrDefault(topicDataName, ""),
        kafkaSegment.getKafkaTopic(
            // TODO - figure out a better message group for validated events.
            Constants.METRICS_TOPIC_MESSAGE_GROUP, kafkaSegment.getDataset(label), topicDataName));
  }

  /** Used to access similar topic names without having the full combination declared in code. */
  public interface ValidatedSourceSinkTopics {

    SourceSinkTopics getValid();

    SourceSinkTopics getInvalid();
  }

  /** Used to access similar topic names without having the full combination declared in code. */
  public interface SourceSinkTopics {
    String getSourceTopic();

    String getSinkTopic();
  }
}
