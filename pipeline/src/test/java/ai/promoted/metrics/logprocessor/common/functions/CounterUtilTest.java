package ai.promoted.metrics.logprocessor.common.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.CartContent;
import ai.promoted.proto.event.JoinedEvent;
import org.junit.jupiter.api.Test;

public class CounterUtilTest {
  @Test
  public void getCount() {
    assertEquals(1, CounterUtil.getCount(joinedEvent(createAction(ActionType.PURCHASE))));
    // We should filter these out earlier.
    assertEquals(
        0,
        CounterUtil.getCount(
            joinedEvent(
                createAction(ActionType.PURCHASE).setSingleCartContent(CartContent.newBuilder()))));
    assertEquals(
        1,
        CounterUtil.getCount(
            joinedEvent(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(1)))));
    assertEquals(
        5,
        CounterUtil.getCount(
            joinedEvent(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(5)))));
    assertEquals(
        -3,
        CounterUtil.getCount(
            joinedEvent(
                createAction(ActionType.PURCHASE)
                    .setSingleCartContent(CartContent.newBuilder().setQuantity(-3)))));
  }

  private static JoinedEvent joinedEvent(Action.Builder action) {
    return FlatUtil.setFlatAction(JoinedEvent.newBuilder(), action.build(), (tag, error) -> {})
        .build();
  }

  private static Action.Builder createAction(ActionType actionType) {
    return Action.newBuilder().setActionType(actionType);
  }
}
