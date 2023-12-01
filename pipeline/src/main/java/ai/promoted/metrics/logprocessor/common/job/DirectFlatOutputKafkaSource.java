package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine.Option;

/**
 * Provides direct (raw) access to FlatOutputKafkaSource. Most consumers should use the
 * deduplicating version, FlatOutputKafkaSource. We avoid the word "raw" or "duplicated" because
 * it's confusing.
 */
public class DirectFlatOutputKafkaSource implements FlinkSegment, FlatOutputKafkaSourceProvider {
  public static final String DIRECT_FLAT_RO_DATABASE_PREFIX = "direct_flat_ro";
  private final KafkaSourceSegment kafkaSourceSegment;
  private final FlatOutputKafkaSegment flatOutputKafkaSegment;

  @Option(
      names = {"--maxJoinedImpressionOutOfOrderness"},
      defaultValue = "PT1S",
      description = "The maxOutOfOrderness for the JoinedImpression kafka. Default=PT1S")
  public Duration maxJoinedImpressionOutOfOrderness = Duration.parse("PT1S");

  @Option(
      names = {"--maxAttributedActionOutOfOrderness"},
      defaultValue = "PT1S",
      description = "The maxOutOfOrderness for the AttributedAction kafka. Default=PT1S")
  public Duration maxAttributedActionOutOfOrderness = Duration.parse("PT1S");

  public DirectFlatOutputKafkaSource(
      KafkaSourceSegment kafkaSourceSegment, FlatOutputKafkaSegment flatOutputKafkaSegment) {
    this.kafkaSourceSegment = kafkaSourceSegment;
    this.flatOutputKafkaSegment = flatOutputKafkaSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Do not register Segments passed into the constructor.
    return ImmutableSet.of();
  }

  /** Returns the source for JoinedImpression topics. */
  public SingleOutputStreamOperator<JoinedImpression> getJoinedImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    String topic = flatOutputKafkaSegment.getJoinedImpressionSourceTopic();
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        JoinedImpression.class,
        JoinedImpression::parseFrom,
        maxJoinedImpressionOutOfOrderness,
        joinedImpression -> joinedImpression.getTiming().getEventApiTimestamp());
  }

  /** Returns the source for AttributedAction topics. */
  public SingleOutputStreamOperator<AttributedAction> getAttributedActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    // NOTE: the workaround to use addSource doesn't allow max.poll.records overrides for now which
    // is ok.
    String topic = flatOutputKafkaSegment.getAttributedActionSourceTopic();
    return kafkaSourceSegment.addProtobufSource(
        env,
        topic,
        consumerGroupId,
        AttributedAction.class,
        AttributedAction::parseFrom,
        maxAttributedActionOutOfOrderness,
        attributedAction -> attributedAction.getAction().getTiming().getEventApiTimestamp());
  }
}
