package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyJoinedAction;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

@AutoValue
public abstract class ImpressionActionSplit {
  public abstract SingleOutputStreamOperator<TinyJoinedAction> impressionActions();

  public abstract SideOutputDataStream<TinyJoinedAction> postNavigateActions();

  public static ImpressionActionSplit create(
      SingleOutputStreamOperator<TinyJoinedAction> impressionActions,
      SideOutputDataStream<TinyJoinedAction> postNavigateActions) {
    return new AutoValue_ImpressionActionSplit(impressionActions, postNavigateActions);
  }
}
