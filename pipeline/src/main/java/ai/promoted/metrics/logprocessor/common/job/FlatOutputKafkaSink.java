package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Sinks for {@code FlatOutputJob}. */
public class FlatOutputKafkaSink implements CompositeFlinkSegment {
  private final BaseFlinkJob job;
  private final FlatOutputKafkaSegment flatOutputKafkaSegment;
  private final KafkaSinkSegment kafkaSinkSegment;

  public FlatOutputKafkaSink(
      BaseFlinkJob job,
      FlatOutputKafkaSegment flatOutputKafkaSegment,
      KafkaSinkSegment kafkaSinkSegment) {
    this.job = job;
    this.flatOutputKafkaSegment = flatOutputKafkaSegment;
    this.kafkaSinkSegment = kafkaSinkSegment;
  }

  @Override
  public Set<FlinkSegment> getInnerFlinkSegments() {
    // Do not register Segments passed into the constructor.
    return ImmutableSet.of();
  }

  public StreamSinkWithLateOutput<JoinedImpression> sinkJoinedImpression(
      DataStream<JoinedImpression> stream) {
    String topic = flatOutputKafkaSegment.getJoinedImpressionSinkTopic();
    // For the key:
    // - prefer anonUserId.
    // - TODO - implement retainedUserId.
    // - then impressionId.
    return kafkaSinkSegment.sinkTo(
        stream,
        topic,
        f -> f.getIds().getPlatformId(),
        f -> {
          if (!f.getIds().getAnonUserId().isEmpty()) {
            return f.getIds().getAnonUserId();
          } else {
            return f.getIds().getImpressionId();
          }
        });
  }

  public StreamSinkWithLateOutput<AttributedAction> sinkAttributedAction(
      DataStream<AttributedAction> stream) {
    String topic = flatOutputKafkaSegment.getAttributedActionSinkTopic();
    // For the key:
    // - prefer anonUserId.
    // - TODO - implement retainedUserId.
    // - then actionId.
    return kafkaSinkSegment.sinkTo(
        stream,
        topic,
        f -> f.getAction().getPlatformId(),
        f -> {
          if (!f.getAction().getUserInfo().getAnonUserId().isEmpty()) {
            return f.getAction().getUserInfo().getAnonUserId();
          } else if (!f.getTouchpoint().getJoinedImpression().getIds().getAnonUserId().isEmpty()) {
            return f.getTouchpoint().getJoinedImpression().getIds().getAnonUserId();
          } else {
            return f.getAction().getActionId();
          }
        });
  }
}
