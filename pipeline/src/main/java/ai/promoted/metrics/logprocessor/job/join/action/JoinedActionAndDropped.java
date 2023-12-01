package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyJoinedAction;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

@AutoValue
public abstract class JoinedActionAndDropped {
  public abstract SingleOutputStreamOperator<TinyJoinedAction> actions();

  public abstract DataStream<TinyAction> droppedActions();

  public static JoinedActionAndDropped create(
      SingleOutputStreamOperator<TinyJoinedAction> actions, DataStream<TinyAction> droppedActions) {
    return new AutoValue_JoinedActionAndDropped(actions, droppedActions);
  }
}
