package ai.promoted.metrics.logprocessor.job.validateenrich;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;

class MapCustomActionType implements MapFunction<Action, Action> {

  private final Map<String, ActionType> customActionTypeMap;

  MapCustomActionType(Map<String, ActionType> customActionTypeMap) {
    this.customActionTypeMap = customActionTypeMap;
  }

  @Override
  public Action map(Action action) throws Exception {
    ActionType actionType = customActionTypeMap.get(action.getCustomActionType());
    if (actionType != null) {
      return action.toBuilder().setActionType(actionType).build();
    } else {
      return action;
    }
  }
}
