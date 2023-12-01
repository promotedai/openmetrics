package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import java.util.EnumSet;
import java.util.Set;

/**
 * Predicate that determines if a {@code TinyAction} is an action that (true) happens on an
 * Impression or (false) other locations (e.g. product page, checkout, purchase).
 *
 * <p>TODO - might make sense to classify more. E.g. post-click.
 */
public class IsImpressionAction implements SerializablePredicate<TinyAction> {
  private final EnumSet<ActionType> actionTypes;
  private final Set<String> customActionTypes;

  public IsImpressionAction(EnumSet<ActionType> actionTypes, Set<String> customActionTypes) {
    this.actionTypes = actionTypes;
    this.customActionTypes = customActionTypes;
  }

  @Override
  public boolean test(TinyAction action) {
    if (action.getActionType() == ActionType.NAVIGATE) {
      // Assumes that NAVIGATES happen on Impressions.
      return true;
    }
    if (action.getActionType() == ActionType.CUSTOM_ACTION_TYPE) {
      return customActionTypes.contains(action.getCustomActionType());
    }
    return actionTypes.contains(action.getActionType());
  }
}
