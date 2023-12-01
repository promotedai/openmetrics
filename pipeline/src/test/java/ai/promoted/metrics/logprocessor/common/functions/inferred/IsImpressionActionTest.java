package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import com.google.common.collect.ImmutableSet;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

public class IsImpressionActionTest {

  @Test
  public void test_defaultFlags() {
    IsImpressionAction predicate =
        new IsImpressionAction(EnumSet.noneOf(ActionType.class), ImmutableSet.of());
    assertThat(predicate.test(TinyAction.getDefaultInstance())).isFalse();
    assertThat(predicate.test(newAction(ActionType.NAVIGATE))).isTrue();
    assertThat(predicate.test(newAction(ActionType.LIKE))).isFalse();
    assertThat(predicate.test(newCustomAction("hover"))).isFalse();
    assertThat(predicate.test(newAction(ActionType.CUSTOM_ACTION_TYPE))).isFalse();
    assertThat(predicate.test(newAction(ActionType.ADD_TO_CART))).isFalse();
    assertThat(predicate.test(newAction(ActionType.PURCHASE))).isFalse();
  }

  @Test
  public void test_withFlagValues() {
    IsImpressionAction predicate =
        new IsImpressionAction(EnumSet.of(ActionType.LIKE), ImmutableSet.of("hover"));
    assertThat(predicate.test(TinyAction.getDefaultInstance())).isFalse();
    assertThat(predicate.test(newAction(ActionType.NAVIGATE))).isTrue();
    assertThat(predicate.test(newAction(ActionType.LIKE))).isTrue();
    assertThat(predicate.test(newCustomAction("hover"))).isTrue();
    assertThat(predicate.test(newAction(ActionType.CUSTOM_ACTION_TYPE))).isFalse();
    assertThat(predicate.test(newAction(ActionType.ADD_TO_CART))).isFalse();
    assertThat(predicate.test(newAction(ActionType.PURCHASE))).isFalse();
  }

  @Test
  public void pleaseReviewNewActionTypes() {
    assertWithMessage(
            "For new ActionTypes, please review if the ActionType should default to being a post-NAVIGATE ActionType.")
        .that(ActionType.values().length)
        .isEqualTo(23);
  }

  private static TinyAction newAction(ActionType actionType) {
    return TinyAction.newBuilder().setActionType(actionType).build();
  }

  private static TinyAction newCustomAction(String customActionType) {
    return TinyAction.newBuilder()
        .setActionType(ActionType.CUSTOM_ACTION_TYPE)
        .setCustomActionType(customActionType)
        .build();
  }
}
