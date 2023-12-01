package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.JoinedImpression;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface FlatOutputKafkaSourceProvider {
  SingleOutputStreamOperator<JoinedImpression> getJoinedImpressionSource(
      StreamExecutionEnvironment env, String consumerGroupId);

  SingleOutputStreamOperator<AttributedAction> getAttributedActionSource(
      StreamExecutionEnvironment env, String consumerGroupId);
}
