package ai.promoted.metrics.logprocessor.job.validateenrich;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.ActionType;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MapCustomActionTypeTest {
  protected MapCustomActionType operator;

  @BeforeEach
  public void setUp() {
    operator = new MapCustomActionType(ImmutableMap.of("test purchase", ActionType.PURCHASE));
  }

  @Test
  public void map() throws Exception {
    Action action =
        Action.newBuilder()
            .setActionType(ActionType.CUSTOM_ACTION_TYPE)
            .setCustomActionType("test purchase")
            .build();
    assertThat(operator.map(action))
        .isEqualTo(
            Action.newBuilder()
                .setActionType(ActionType.PURCHASE)
                .setCustomActionType("test purchase")
                .build());
  }

  @Test
  public void doNotMap() throws Exception {
    Action action1 = Action.newBuilder().setActionType(ActionType.NAVIGATE).build();
    assertThat(operator.map(action1)).isEqualTo(action1);
    Action action2 =
        Action.newBuilder()
            .setActionType(ActionType.CUSTOM_ACTION_TYPE)
            .setCustomActionType("ignore")
            .build();
    assertThat(operator.map(action2)).isEqualTo(action2);
  }
}
