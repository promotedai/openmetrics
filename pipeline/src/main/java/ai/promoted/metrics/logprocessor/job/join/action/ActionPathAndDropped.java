package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import com.google.auto.value.AutoValue;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Outputs from the ActionJoin stage. */
@AutoValue
public abstract class ActionPathAndDropped {
  public abstract DataStream<TinyActionPath> actionPaths();

  public abstract DataStream<TinyAction> droppedActions();

  public static ActionPathAndDropped create(
      DataStream<TinyActionPath> actionPaths, DataStream<TinyAction> droppedActions) {
    return new AutoValue_ActionPathAndDropped(actionPaths, droppedActions);
  }
}
