package ai.promoted.metrics.logprocessor.common.job;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.firstNotEmpty;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import picocli.CommandLine.Option;

/** Segment for Kafka topics for the {@code FlatOutputJob}. */
public class FlatOutputKafkaSegment implements FlinkSegment {
  private final BaseFlinkJob job;
  private final KafkaSegment kafkaSegment;

  @Option(
      names = {"--joinedImpressionTopic"},
      defaultValue = "",
      description = "Overrides the JoinedImpression Kafka topic.")
  public String joinedImpressionTopicOverride = "";

  @Option(
      names = {"--attributedActionTopic"},
      defaultValue = "",
      description = "Overrides the AttributedAction Kafka topic.")
  public String attributedActionTopicOverride = "";

  @Option(
      names = {"--flatResponseInsertionTopic"},
      defaultValue = "",
      description = "Overrides the FlatResponseInsertion Kafka topic.")
  public String flatResponseInsertionTopicOverride = "";

  public FlatOutputKafkaSegment(BaseFlinkJob job, KafkaSegment kafkaSegment) {
    this.job = job;
    this.kafkaSegment = kafkaSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    return ImmutableSet.of(
        AttributedAction.class, JoinedImpression.class, FlatResponseInsertion.class);
  }

  public String getJoinedImpressionSourceTopic() {
    return getJoinedImpressionTopic(job.getInputLabel("joined-impression"));
  }

  public String getJoinedImpressionSinkTopic() {
    return getJoinedImpressionTopic(job.getJobLabel());
  }

  private String getJoinedImpressionTopic(String label) {
    return firstNotEmpty(
        joinedImpressionTopicOverride,
        kafkaSegment.getKafkaTopic(
            Constants.METRICS_TOPIC_MESSAGE_GROUP,
            kafkaSegment.getDataset(label),
            Constants.JOINED_IMPRESSION_TOPIC_DATA_NAME));
  }

  public String getAttributedActionSourceTopic() {
    return getAttributedActionTopic(job.getInputLabel("attributed-action"));
  }

  public String getAttributedActionSinkTopic() {
    return getAttributedActionTopic(job.getJobLabel());
  }

  private String getAttributedActionTopic(String label) {
    return firstNotEmpty(
        attributedActionTopicOverride,
        kafkaSegment.getKafkaTopic(
            Constants.METRICS_TOPIC_MESSAGE_GROUP,
            kafkaSegment.getDataset(label),
            Constants.ATTRIBUTED_ACTION_TOPIC_DATA_NAME));
  }
}
