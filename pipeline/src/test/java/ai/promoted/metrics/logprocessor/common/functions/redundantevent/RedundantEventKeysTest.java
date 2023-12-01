package ai.promoted.metrics.logprocessor.common.functions.redundantevent;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.event.ActionType;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyInsertionCore;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.TinyTouchpoint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.jupiter.api.Test;

public class RedundantEventKeysTest {

  @Test
  public void forRedundantImpressionInImpressionStream() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantImpressionInImpressionStream(
                newTinyJoinedImpression("paging1")))
        .isEqualTo(Tuple3.of(1L, "anonUserId", "c1"));
  }

  @Test
  public void forRedundantImpressionInActionPathStream_noTouchpoints() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantImpressionInActionPathStream(
                TinyActionPath.newBuilder().setAction(newAction()).build()))
        .isEqualTo(Tuple3.of(1L, "anonUserId", "c2"));
  }

  @Test
  public void forRedundantImpressionInActionPathStream_hasTouchpoints() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantImpressionInActionPathStream(
                TinyActionPath.newBuilder()
                    .setAction(newAction())
                    .addTouchpoints(
                        TinyTouchpoint.newBuilder()
                            .setJoinedImpression(newTinyJoinedImpression("pagingId")))
                    .build()))
        // Prefers insertion content.
        .isEqualTo(Tuple3.of(1L, "anonUserId", "c1"));
  }

  @Test
  public void forRedundantActionsInImpressionActionPathStream_hasTouchpoints() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantActionsInImpressionActionPathStream(
                TinyActionPath.newBuilder()
                    .setAction(newAction())
                    .addTouchpoints(
                        TinyTouchpoint.newBuilder()
                            .setJoinedImpression(newTinyJoinedImpression("pagingId")))
                    .build()))
        .isEqualTo(
            Tuple5.of(
                1L, "anonUserId", "ins1", ActionType.CUSTOM_ACTION_TYPE.getNumber(), "negotiate"));
  }

  @Test
  public void forRedundantActionsInPostNavigateActionStream_noTouchpoints() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantActionsInPostNavigateActionStream(
                TinyActionPath.newBuilder().setAction(newAction()).build()))
        .isEqualTo(
            Tuple5.of(
                1L, "anonUserId", "c2", ActionType.CUSTOM_ACTION_TYPE.getNumber(), "negotiate"));
  }

  @Test
  public void forRedundantActionsInPostNavigateActionStream_hasTouchpoints() throws Exception {
    assertThat(
            RedundantEventKeys.forRedundantActionsInPostNavigateActionStream(
                TinyActionPath.newBuilder()
                    .setAction(newAction())
                    .addTouchpoints(
                        TinyTouchpoint.newBuilder()
                            .setJoinedImpression(newTinyJoinedImpression("pagingId")))
                    .build()))
        // Prefers insertion content.
        .isEqualTo(
            Tuple5.of(
                1L, "anonUserId", "c1", ActionType.CUSTOM_ACTION_TYPE.getNumber(), "negotiate"));
  }

  private TinyAction newAction() {
    return TinyAction.newBuilder()
        .setCommon(newCommonInfo())
        .setActionId("act1")
        .setContentId("c2")
        .setActionType(ActionType.CUSTOM_ACTION_TYPE)
        .setCustomActionType("negotiate")
        .build();
  }

  private TinyJoinedImpression newTinyJoinedImpression(String pagingId) {
    return TinyJoinedImpression.newBuilder()
        .setImpression(
            TinyImpression.newBuilder().setCommon(newCommonInfo()).setImpressionId("imp1"))
        .setInsertion(newTinyInsertion(pagingId))
        .build();
  }

  private TinyInsertion newTinyInsertion(String pagingId) {
    return TinyInsertion.newBuilder()
        .setCommon(newCommonInfo())
        .setViewId("view1")
        .setPagingId(pagingId)
        .setCore(
            TinyInsertionCore.newBuilder()
                .setInsertionId("ins1")
                .setContentId("c1")
                .setPosition(2L))
        .build();
  }

  private TinyCommonInfo newCommonInfo() {
    return TinyCommonInfo.newBuilder().setPlatformId(1L).setAnonUserId("anonUserId").build();
  }
}
