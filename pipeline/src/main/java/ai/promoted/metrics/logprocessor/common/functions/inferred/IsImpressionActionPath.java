package ai.promoted.metrics.logprocessor.common.functions.inferred;

import ai.promoted.metrics.logprocessor.common.functions.base.SerializablePredicate;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;

/**
 * A {@code FilterFunction} that determines if a {@code TinyActionPath} is an action that (true)
 * happens on an Impression or (false) other locations (e.g. product page, checkout, purchase).
 */
public class IsImpressionActionPath implements SerializablePredicate<TinyActionPath> {
  private final SerializablePredicate<TinyAction> isImpressionAction;

  public IsImpressionActionPath(SerializablePredicate<TinyAction> isImpressionAction) {
    this.isImpressionAction = isImpressionAction;
  }

  @Override
  public boolean test(TinyActionPath actionPath) {
    return isImpressionAction.test(actionPath.getAction());
  }
}
