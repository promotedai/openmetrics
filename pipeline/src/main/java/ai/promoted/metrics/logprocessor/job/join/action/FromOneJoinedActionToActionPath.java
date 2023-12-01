package ai.promoted.metrics.logprocessor.job.join.action;

import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyTouchpoint;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Converts a single joined action to an action path. This is used for converting ImpressionActions
 * to ActionPath.
 *
 * <p>TODO - ImpressionActions can have a simplified schema.
 */
class FromOneJoinedActionToActionPath implements MapFunction<TinyJoinedAction, TinyActionPath> {

  @Override
  public TinyActionPath map(TinyJoinedAction joinedAction) throws Exception {
    return TinyActionPath.newBuilder()
        .setAction(joinedAction.getAction())
        .addTouchpoints(
            TinyTouchpoint.newBuilder().setJoinedImpression(joinedAction.getJoinedImpression()))
        .build();
  }
}
