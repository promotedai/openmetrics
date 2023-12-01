package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import org.junit.jupiter.api.Test;

public class IsImpressionActionPathTest {

  @Test
  public void test() {
    IsImpressionActionPath predicate =
        new IsImpressionActionPath(
            (actionPath) -> actionPath.getActionType() == ActionType.NAVIGATE);
    assertThat(predicate.test(TinyActionPath.getDefaultInstance())).isFalse();
    assertThat(predicate.test(newActionPath(newAction(ActionType.NAVIGATE)))).isTrue();
    assertThat(predicate.test(newActionPath(newAction(ActionType.CHECKOUT)))).isFalse();
  }

  private static TinyActionPath newActionPath(TinyAction action) {
    return TinyActionPath.newBuilder().setAction(action).build();
  }

  private static TinyAction newAction(ActionType actionType) {
    return TinyAction.newBuilder().setActionType(actionType).build();
  }
}
