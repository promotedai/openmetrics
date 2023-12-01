package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.CartContent;
import org.junit.jupiter.api.Test;

public class CounterUtilTest {
  @Test
  public void getCount() {
    assertEquals(1, CounterUtil.getCount(attributedAction(createAction(ActionType.PURCHASE))));
    // We should filter these out earlier.
    assertEquals(
        0,
        CounterUtil.getCount(
            attributedAction(
                createAction(ActionType.PURCHASE).setSingleCartContent(CartContent.newBuilder()))));
    assertEquals(
        1,
        CounterUtil.getCount(
            attributedAction(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(1)))));
    assertEquals(
        5,
        CounterUtil.getCount(
            attributedAction(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(5)))));
    assertEquals(
        -3,
        CounterUtil.getCount(
            attributedAction(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(-3)))));
  }

  private static AttributedAction attributedAction(Action.Builder action) {
    return AttributedAction.newBuilder().setAction(action).build();
  }

  private static Action.Builder createAction(ActionType actionType) {
    return Action.newBuilder().setActionType(actionType);
  }
}
