package ai.promoted.metrics.logprocessor.job.join.impression;

import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyJoinedImpression;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/** Outputs from the ImpressionJoin stage. */
@AutoValue
public abstract class ImpressionJoinOutputs {
  public abstract SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions();

  public abstract DataStream<TinyImpression> droppedImpressions();

  public static ImpressionJoinOutputs create(
      SingleOutputStreamOperator<TinyJoinedImpression> joinedImpressions,
      DataStream<TinyImpression> droppedImpressions) {
    return new AutoValue_ImpressionJoinOutputs(joinedImpressions, droppedImpressions);
  }
}
