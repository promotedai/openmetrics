package ai.promoted.metrics.logprocessor.job.join.action;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedAction;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FromOneJoinedActionToActionPathUnitTest {
  private FromOneJoinedActionToActionPath fn;

  @BeforeEach
  public void setUp() throws Exception {
    fn = new FromOneJoinedActionToActionPath();
  }

  @Test
  public void oneRecord() throws Exception {
    assertThat(
            fn.map(
                newJoinedAction(
                    newJoinedImpression(newInsertion("ins1"), newImpression("imp1")),
                    newAction("act1"))))
        .isEqualTo(
            TinyActionPath.newBuilder()
                .setAction(newAction("act1"))
                .addTouchpoints(
                    TinyTouchpoint.newBuilder()
                        .setJoinedImpression(
                            newJoinedImpression(newInsertion("ins1"), newImpression("imp1"))))
                .build());
  }

  private TinyJoinedAction newJoinedAction(
      TinyJoinedImpression joinedImpression, TinyAction action) {
    return TinyJoinedAction.newBuilder()
        .setJoinedImpression(joinedImpression)
        .setAction(action)
        .build();
  }

  private static TinyAction newAction(String actionId) {
    return TinyAction.newBuilder().setActionId(actionId).build();
  }

  // otherContentId = used to create lower priority TinyInsertions.
  private static TinyInsertion newInsertion(String insertionId) {
    return TinyInsertion.newBuilder()
        .setCore(TinyInsertionCore.newBuilder().setInsertionId(insertionId))
        .build();
  }

  private static TinyImpression newImpression(String impressionId) {
    return TinyImpression.newBuilder().setImpressionId(impressionId).build();
  }

  private static TinyJoinedImpression newJoinedImpression(
      TinyInsertion insertion, TinyImpression impression) {
    return TinyJoinedImpression.newBuilder()
        .setInsertion(insertion)
        .setImpression(impression)
        .build();
  }
}
