package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import com.google.protobuf.GeneratedMessageV3;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlatOutputKafkaSource implements FlinkSegment, FlatOutputKafkaSourceProvider {
  public static final String FLAT_RO_DB_PREFIX = "flat_ro";
  private final DirectFlatOutputKafkaSource directFlatOutputKafkaSource;
  private final FlatOutputKafkaSegment flatOutputKafkaSegment;
  private final KeepFirstSegment keepFirstSegment;

  public FlatOutputKafkaSource(
      DirectFlatOutputKafkaSource directFlatOutputKafkaSource,
      FlatOutputKafkaSegment flatOutputKafkaSegment,
      KeepFirstSegment keepFirstSegment) {
    this.directFlatOutputKafkaSource = directFlatOutputKafkaSource;
    this.flatOutputKafkaSegment = flatOutputKafkaSegment;
    this.keepFirstSegment = keepFirstSegment;
  }

  @Override
  public void validateArgs() {
    // Do nothing.
  }

  @Override
  public Set<Class<? extends GeneratedMessageV3>> getProtoClasses() {
    // Do not register Segments passed into the constructor.
    return directFlatOutputKafkaSource.getProtoClasses();
  }

  @Override
  public SingleOutputStreamOperator<JoinedImpression> getJoinedImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directFlatOutputKafkaSource.getJoinedImpressionSource(env, consumerGroupId),
        TypeInformation.of(JoinedImpression.class),
        KeyUtil.joinedImpressionKey,
        flatOutputKafkaSegment.getJoinedImpressionSourceTopic());
  }

  @Override
  public SingleOutputStreamOperator<AttributedAction> getAttributedActionSource(
      StreamExecutionEnvironment env, String consumerGroupId) {
    return keepFirstSegment.keepFirst(
        directFlatOutputKafkaSource.getAttributedActionSource(env, consumerGroupId),
        TypeInformation.of(AttributedAction.class),
        KeyUtil.attributedActionKey,
        flatOutputKafkaSegment.getAttributedActionSourceTopic());
  }
}
